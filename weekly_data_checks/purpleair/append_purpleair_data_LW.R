library(tidyverse)

path_new <- "/Users/lewiswhite/CHAP_columbia/GRAPHS/exposure_analysis/weekly_data_checks/purpleair/processed/purpleair_processed_2026-03-10.rds"
path_full <- "/Users/lewiswhite/CHAP_columbia/GRAPHS/exposure_analysis/weekly_data_checks/purpleair/appended_full_data/purpleair_appended_full.rds"

new_dat <- readRDS(path_new)

if (file.exists(path_full)) {
  old_dat <- readRDS(path_full)
  purpleair_full <- bind_rows(old_dat, new_dat)
} else {
  purpleair_full <- new_dat
}

purpleair_full <- purpleair_full %>%
  mutate(
    community_id = vname,
    timestamp = as.POSIXct(timestamp, tz = "UTC"),
    community_id = case_when(
      community_id == "kadeslo" ~ "kadelso",
      community_id == "suronause" ~ "suronuase",
      TRUE ~ community_id
    )
  ) %>%
  filter(!is.na(community_id), !is.na(timestamp)) %>%
  distinct(community_id, timestamp, .keep_all = TRUE) %>%
  arrange(community_id, timestamp)

dir.create(dirname(path_full), recursive = TRUE, showWarnings = FALSE)
saveRDS(purpleair_full, path_full)

saveRDS(
  purpleair_full,
  file.path(dirname(path_full), paste0("purpleair_appended_full_", Sys.Date(), ".rds"))
)
