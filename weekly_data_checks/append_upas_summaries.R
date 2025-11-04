library(tidyverse)
library(lubridate)
library(here)
library(stringr)
library(anytime) 

summary_path <- here::here("weekly_data_checks", "upas_data2025", "processed_archive")
summary_files <- list.files(summary_path, pattern = "^pm_summary_upas_.*\\.csv$", full.names = TRUE)

# robust parser that accepts ISO strings, epoch seconds/ms, or Excel dates
parse_mixed_time <- function(x) {
  x_chr <- as.character(x)
  
  # Try ISO-like datetime strings first
  out <- suppressWarnings(
    lubridate::parse_date_time(
      x_chr,
      orders = c("Ymd HMS", "Ymd HM", "Ymd", 
                 "ymd HMS", "ymd HM", "ymd",
                 "YmdTz", "YmdT", "ymdTz", "ymdT"),
      tz = "UTC"
    )
  )
  
  # Handle numeric epoch
  idx_num <- which(grepl("^[0-9]+(\\.[0-9]+)?$", x_chr))
  if (length(idx_num)) {
    n <- suppressWarnings(as.numeric(x_chr[idx_num]))
    if (length(n)) {
      # milliseconds since epoch
      ms   <- !is.na(n) & n > 1e12
      # seconds since epoch
      secs <- !is.na(n) & n > 3e7 & n <= 1e12
      
      out[idx_num[ms]]   <- as.POSIXct(n[ms] / 1000, origin = "1970-01-01", tz = "UTC")
      out[idx_num[secs]] <- as.POSIXct(n[secs],      origin = "1970-01-01", tz = "UTC")
    }
  }
  
  out
}

master_upas_summary <- purrr::map_dfr(summary_files, function(file) {
  readr::read_csv(
    file,
    show_col_types = FALSE,
    col_types = cols(
      .default   = col_guess(),
      start_time = col_character(),
      end_time   = col_character()
    )
  ) |>
    # normalize time columns
    mutate(
      start_time = parse_mixed_time(start_time),
      end_time   = parse_mixed_time(end_time)
    ) |>
    # coerce any battery columns that might exist (names vary across versions)
    mutate(
      across(any_of(c("battery_start","battery_end","battvolt_start","battvolt_end")),
             ~ suppressWarnings(as.numeric(.)))
    ) |>
    # drop accidental index column if present
    select(-any_of(c("...1")))
})



# Save the combined dataset
write_csv(master_upas_summary, here::here("weekly_data_checks", "upas_data2025", paste0("upas_all_data_",  Sys.Date(), ".csv")))



## UPAS ANALYSIS 

rev_analysis <- master_upas_summary %>%
  mutate(new_rev = ifelse(start_time >= as.Date("2025-08-01"), 1, 0),
         
         low_runtime = ifelse(run_time_hour < 40, 1, 0))

rev_analysis %>% group_by(new_rev) %>% summarise(mean_runtime = mean(run_time_hour))
           

t.test(run_time_hour ~ new_rev, data = rev_analysis)

# wilcoxon rank-sum test (non-parametric)
wilcox.test(run_time_hour ~ new_rev, data = rev_analysis)




tab <- table(rev_analysis$new_rev, rev_analysis$low_runtime)

# chi-square test (if all expected counts > 5)
chisq.test(tab)

# Fisher's exact test (safer with small counts)
fisher.test(tab)
