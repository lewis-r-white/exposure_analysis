library(tidyverse)
library(lubridate)
library(here)
library(zoo)
library(openxlsx)
library(stringr)

# Base directory 
base_dir    <- "/Users/lewiswhite/CHAP_columbia/GRAPHS/exposure_analysis/weekly_data_checks/upas_data2025"
archive_dir <- file.path(base_dir, "processed_archive")

# New folder for corrected outputs
out_dir     <- file.path(base_dir, "processed_corrected")
dir.create(out_dir, showWarnings = FALSE)


safe_stat <- function(x, fun) {
  x <- x[is.finite(x)]
  if (!length(x)) return(NA_real_)
  fun(x, na.rm = TRUE)
}


# List all list.upas_*.rds files in the archive
rds_files <- list.files(
  archive_dir,
  pattern = "^list\\.upas_.*\\.rds$",
  full.names = TRUE
)


for (rds_path in rds_files) {
  # Extract the community+date stem, e.g. "ampoma20250925"
  stem <- sub("^list\\.upas_(.*)\\.rds$", "\\1", basename(rds_path))
  
  message("Processing: ", stem)
  
  # Corresponding CSV + Excel in the archive
  csv_in  <- file.path(archive_dir, paste0("pm_summary_upas_", stem, ".csv"))
  xlsx_in <- file.path(archive_dir, paste0("formatted_",        stem, ".xlsx"))  # only needed if you want to check it exists
  
  # Output paths in processed_corrected
  csv_out  <- file.path(out_dir, paste0("pm_summary_upas_", stem, ".csv"))
  xlsx_out <- file.path(out_dir, paste0("formatted_",        stem, ".xlsx"))
  
  # Skip if there's no summary CSV
  if (!file.exists(csv_in)) {
    message("  -> Skipping ", stem, " (no CSV found)")
    next
  }
  
  # Load data
  upas_list <- readRDS(rds_path)                         # list of tibbles (one per run)
  pm_summ   <- readr::read_csv(csv_in, show_col_types = FALSE)  # summary CSV
  
  # Sanity check: rows vs. list length
  if (length(upas_list) != nrow(pm_summ)) {
    warning("  !! For ", stem, ": length(upas_list) = ", length(upas_list),
            " but nrow(pm_summ) = ", nrow(pm_summ),
            ". Proceeding by position, but double-check this file.")
  }
  
  n <- min(length(upas_list), nrow(pm_summ))
  
  for (i in seq_len(n)) {
    df <- upas_list[[i]]
    
    # Optional sanity check: subject_id alignment
    sid_rds <- as.character(df$subject_id[1])
    sid_csv <- as.character(pm_summ$subject_id[i])
    if (!is.na(sid_csv) && !is.na(sid_rds) && sid_csv != sid_rds) {
      message("    [", stem, "] subject mismatch at row ", i,
              ": RDS=", sid_rds, " CSV=", sid_csv)
      # still continue; order is the main linkage
    }
    
    # Ensure types + ordering
    df <- df %>%
      mutate(
        DateTimeLocal_parsed = suppressWarnings(anytime(DateTimeLocal, tz = "UTC")),
        PM2_5MC_clean = as.numeric(PM2_5MC_clean)
      ) %>%
      filter(!is.na(DateTimeLocal_parsed)) %>%   # drop unparseable rows
      mutate(DateTimeLocal = DateTimeLocal_parsed) %>%
      select(-DateTimeLocal_parsed) %>%
      arrange(DateTimeLocal)
    
    # Use run_time_hour from the summary to define in-run window (same as your Rmd)
    run_hrs <- pm_summ$run_time_hour[i]
    if (is.finite(run_hrs) && run_hrs > 0) {
      run_start <- min(df$DateTimeLocal, na.rm = TRUE)
      run_end   <- run_start + run_hrs * 3600
      df_inrun  <- dplyr::filter(df, DateTimeLocal <= run_end)
    } else {
      # if missing/zero, just use the whole record
      df_inrun  <- df
    }
    
    if (!nrow(df_inrun)) {
      # no data for this run — leave pm_* as-is
      next
    }
    
    # --- Forward-fill PM to account for 15-minute nighttime logging ---
    df_inrun <- df_inrun %>%
      mutate(
        PM2_5MC_clean  = as.numeric(PM2_5MC_clean),
        PM2_5MC_filled = zoo::na.locf(PM2_5MC_clean, na.rm = FALSE)
      )
    
    # --- Update PM summaries for THIS subject/run (row i) ---
    pm_summ$pm_min[i]    <- round(safe_stat(df_inrun$PM2_5MC_clean, min),              2)
    pm_summ$pm_max[i]    <- round(safe_stat(df_inrun$PM2_5MC_clean, max),              2)
    pm_summ$pm_mean[i]   <- round(safe_stat(df_inrun$PM2_5MC_filled, mean),            2)
    pm_summ$pm_median[i] <- round(safe_stat(df_inrun$PM2_5MC_filled, stats::median),   2)
  }
  
  # ----- Write corrected CSV -----
  readr::write_csv(pm_summ, csv_out)
  message("  -> Wrote corrected CSV: ", basename(csv_out))
  
  
  # ----- Rebuild the formatted Excel from the corrected pm_summ -----
  wb <- createWorkbook()
  sheet_name <- stem
  addWorksheet(wb, sheet_name)
  writeData(wb, sheet_name, pm_summ)
  
  rows <- 2:(nrow(pm_summ) + 1)   # header is row 1
  style_red <- createStyle(bgFill = "red")
  get_col <- function(var) which(names(pm_summ) == var)
  
  apply_rules <- function(var, rule, type = "expression") {
    if (var %in% names(pm_summ)) {
      conditionalFormatting(
        wb, sheet_name,
        cols = get_col(var),
        rows = rows,
        rule = rule,
        style = style_red,
        type  = type
      )
    }
  }
  
  # Same conditional formatting rules you use in the Rmd
  purrr::walk(c("run_time_hour"), ~apply_rules(.x, "<=44"))
  purrr::walk(c("pm_max", "pm_median", "pm_mean", "temp_min", "rh_min"), ~apply_rules(.x, "<0"))
  purrr::walk(c("temp_max", "temp_mean"), ~apply_rules(.x, ">45"))
  apply_rules("rh_max", ">99")
  purrr::walk(c("flow_max", "flow_mean"), ~apply_rules(.x, ">2.08"))
  apply_rules("flow_mean", "<0.96")
  purrr::walk(c("compliance_hours_day_session1", "compliance_hours_day_session2"), ~apply_rules(.x, "<=10"))
  purrr::walk(c("compliance_hours_night_session1", "compliance_hours_night_session2"), ~apply_rules(.x, "<1"))
  apply_rules("temp_minutes_over_50", ">0")
  apply_rules("flow_minutes_under_0_9", ">10")
  purrr::walk(c("temp_max_flag_over_53", "flow_flag_low"), ~apply_rules(.x, "==1"))
  
  if ("duplicate_subject" %in% names(pm_summ)) {
    conditionalFormatting(
      wb, sheet_name,
      cols = get_col("duplicate_subject"),
      rows = rows,
      type = "contains",
      rule = "Yes",
      style = style_red
    )
  }
  if ("shutdownmode" %in% names(pm_summ)) {
    conditionalFormatting(
      wb, sheet_name,
      cols = get_col("shutdownmode"),
      rows = rows,
      type = "contains",
      rule = "Contact ATS for Support",
      style = style_red
    )
  }
  
  saveWorkbook(wb, xlsx_out, overwrite = TRUE)
  message("  -> Wrote corrected Excel: ", basename(xlsx_out))
}


