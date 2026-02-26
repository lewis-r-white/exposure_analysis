###########################################################
######  Data Cleaning Script for PurpleAir Monitors  ######
###########################################################


# Load Dependencies

library(data.table)
library(dplyr)
library(xts)
library(dygraphs)
library(readr) #for read_csv function
library(ggpubr)
library(readr)
library(magrittr)
library(purrr)
library(stringr)


##Read in PurpleAir Files for All Communities
purpleair <- list.dirs(path = "/Users/lewiswhite/CHAP_columbia/GRAPHS/exposure_analysis/weekly_data_checks/purpleair/raw_data", full.names = FALSE, recursive = FALSE) #list to director containing PurpleAir Folders


list.purpleair <- list()
problem_files <- list()

##################################################
### Read in all csv files for each community    ##
##################################################

# Load Dependencies
library(data.table)
library(dplyr)
library(readr)
library(stringr)

# ----------------------------
# Helpers
# ----------------------------

# Clean + parse numeric-ish character vectors:
# - strip control characters (e.g., the "" bytes)
# - treat common missing tokens as NA
clean_num <- function(x) {
  x <- as.character(x)
  x <- stringr::str_replace_all(x, "[[:cntrl:]]", "")
  readr::parse_number(x, na = c("", "NA", "NaN", "nan", "NULL", "null"))
}

# Read a PurpleAir CSV robustly (UTF-8 first, then Latin1)
read_pa_csv <- function(path, colname_list, encoding = "UTF-8") {
  df <- readr::read_csv(
    path,
    col_names = colname_list,
    show_col_types = FALSE,
    skip_empty_rows = TRUE,
    locale = readr::locale(encoding = encoding),
    guess_max = 10000
  )
  
  # Drop first row ONLY if it looks like a repeated header row
  if (nrow(df) > 0 && !is.na(df$UTCDateTime[1]) && df$UTCDateTime[1] == "UTCDateTime") {
    df <- df[-1, ]
  }
  
  df
}

# ----------------------------
# Paths + setup
# ----------------------------
base_path <- "/Users/lewiswhite/CHAP_columbia/GRAPHS/exposure_analysis/weekly_data_checks/purpleair/raw_data"

# list of PurpleAir community folders
purpleair <- list.dirs(path = base_path, full.names = FALSE, recursive = FALSE)

list.purpleair <- list()
problem_files <- list()

# Expected column names (as in your original script)
colname_list <- c(
  "UTCDateTime","mac_address","firmware_ver","hardware","current_temp_f","current_humidity",
  "current_dewpoint_f","pressure","adc","mem","rssi","uptime","pm1_0_cf_1","pm2_5_cf_1","pm10_0_cf_1",
  "pm1_0_atm","pm2_5_atm","pm10_0_atm","pm2.5_aqi_cf_1","pm2.5_aqi_atm","p_0_3_um","p_0_5_um","p_1_0_um",
  "p_2_5_um","p_5_0_um","p_10_0_um","pm1_0_cf_1_b","pm2_5_cf_1_b","pm10_0_cf_1_b","pm1_0_atm_b",
  "pm2_5_atm_b","pm10_0_atm_b","pm2.5_aqi_cf_1_b","pm2.5_aqi_atm_b","p_0_3_um_b","p_0_5_um_b","p_1_0_um_b",
  "p_2_5_um_b","p_5_0_um_b","p_10_0_um_b","gas"
)

# Columns we do NOT want to coerce to numeric
non_num_cols <- c("UTCDateTime", "timestamp", "mac_address", "hardware")

# ----------------------------
# Main loop
# ----------------------------
for (i in seq_along(purpleair)) {
  
  comm_folder <- purpleair[i]
  comm_path <- file.path(base_path, comm_folder)
  
  filenames_purpleair <- list.files(
    path = comm_path,
    recursive = TRUE,
    pattern = "\\.csv$",
    full.names = TRUE
  )
  
  # reset per-community holders
  list.files <- list()
  files <- NULL
  
  for (j in seq_along(filenames_purpleair)) {
    
    f <- filenames_purpleair[j]
    
    # Try UTF-8, fall back to Latin1
    rawfile <- tryCatch(
      read_pa_csv(f, colname_list, encoding = "UTF-8"),
      error = function(e1) {
        warning("UTF-8 read failed; trying Latin1: ", f)
        read_pa_csv(f, colname_list, encoding = "Latin1")
      }
    )
    
    # Skip if empty after any cleaning
    if (nrow(rawfile) == 0) {
      warning("Skipping empty file (no data rows): ", f)
      problem_files[[length(problem_files) + 1]] <- list(file = f, reason = "empty_after_read")
      next
    }
    
    # Track readr parsing problems from read_csv step (optional but kept)
    prob <- readr::problems(rawfile)
    rawfile$badrows <- nrow(prob)
    
    # Store
    list.files[[j]] <- rawfile
    
    # Convert numeric columns (clean control chars + handle nan tokens)
    num_cols <- setdiff(names(list.files[[j]]), non_num_cols)
    list.files[[j]][, num_cols] <- lapply(list.files[[j]][, num_cols], clean_num)
    
    # Add identifiers
    list.files[[j]]$community <- tolower(stringr::str_extract(comm_folder, "[^_]+"))
    list.files[[j]]$filename  <- f
    
    # Bind incrementally
    files <- data.table::rbindlist(list.files, fill = TRUE)
    
    # Community backup + vname
    files$community_2 <- tolower(stringr::str_extract(comm_folder, "[^_]+"))
    files$vname <- ifelse(is.na(files$community), files$community_2, files$community)
    files <- files %>% dplyr::select(-(c(community, community_2)))
  }
  
  if (is.null(files)) {
    warning("No usable files for community: ", comm_folder)
    list.purpleair[[i]] <- data.table::data.table()
  } else {
    list.purpleair[[i]] <- files
  }
}

# Optional quick sanity checks:
# length(list.purpleair)
# sapply(list.purpleair, nrow)
# head(list.purpleair[[1]])



## Combine into one dataframe

purpleair_latest24 <- rbindlist(list.purpleair)

#################################
####   Data Processing       ####
#################################

# fix issue dates
ok_dt <- grepl("^\\d{4}/\\d{2}/\\d{2}T\\d{2}:\\d{2}:\\d{2}[zZ]$", purpleair_latest24$UTCDateTime) |
  grepl("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$",  purpleair_latest24$UTCDateTime)

purpleair_rejects <- purpleair_latest24[!ok_dt]
purpleair_latest24 <- purpleair_latest24[ok_dt]

# Parse timestamps for both common formats
purpleair_latest24$timestamp <- as.POSIXct(NA)

idx_slash <- grepl("^\\d{4}/\\d{2}/\\d{2}T\\d{2}:\\d{2}:\\d{2}[zZ]$", purpleair_latest24$UTCDateTime)
purpleair_latest24$timestamp[idx_slash] <- as.POSIXct(
  purpleair_latest24$UTCDateTime[idx_slash],
  format = "%Y/%m/%dT%H:%M:%Sz",
  tz = "UTC"
)

idx_dash <- grepl("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$", purpleair_latest24$UTCDateTime)
purpleair_latest24$timestamp[idx_dash] <- as.POSIXct(
  purpleair_latest24$UTCDateTime[idx_dash],
  format = "%Y-%m-%dT%H:%M:%SZ",
  tz = "UTC"
)

# correct date if it doesn't match filename date (only when filename is YYYYMMDD.csv)
purpleair_latest24$date_filename <- as.Date(sub(".*/(\\d{8})\\.csv$", "\\1", purpleair_latest24$filename), format = "%Y%m%d")
purpleair_latest24$timestamp_date <- as.Date(purpleair_latest24$timestamp)
purpleair_latest24$timestamp_time <- format(purpleair_latest24$timestamp, "%H:%M:%S")

needs_fix <- !is.na(purpleair_latest24$date_filename) &
  !is.na(purpleair_latest24$timestamp) &
  purpleair_latest24$timestamp_date != purpleair_latest24$date_filename

purpleair_latest24$timestamp[needs_fix] <- as.POSIXct(
  paste(purpleair_latest24$date_filename[needs_fix], purpleair_latest24$timestamp_time[needs_fix]),
  format = "%Y-%m-%d %H:%M:%S",
  tz = "UTC"
)

# cleanup helper cols if you want
purpleair_latest24$date_filename <- NULL
purpleair_latest24$timestamp_date <- NULL
purpleair_latest24$timestamp_time <- NULL



#Convert temperature to Celsius
purpleair_latest24$tempc <- (purpleair_latest24$current_temp_f - 32) * (5/9) 




##Maximum Standard Range : >= 1,000 ug/m3 - Set These to Missing?(https://www2.purpleair.com/products/purpleair-pa-ii)
##Effective Range Set to 0 - 500ug/m3 (Limit to this?)
purpleair_latest24 $pm2_5_atm_a <- ifelse(purpleair_latest24$pm2_5_atm > 1000 , NA, purpleair_latest24$pm2_5_atm)

purpleair_latest24$pm2_5_atm_b_ <- ifelse(purpleair_latest24$pm2_5_atm_b > 1000 , NA, purpleair_latest24$pm2_5_atm_b)



## Compute difference and percent difference
purpleair_latest24$perdiff <- abs(purpleair_latest24$pm2_5_atm_a - purpleair_latest24$pm2_5_atm_b_)/purpleair_latest24$pm2_5_atm_a 

purpleair_latest24$diff <- abs(purpleair_latest24$pm2_5_atm_a - purpleair_latest24$pm2_5_atm_b_)


#Some sensors have a value of 0 causing the percent difference to be 0 so this is set to NA
purpleair_latest24$perdiff <- ifelse(is.infinite(purpleair_latest24$perdiff), NA, purpleair_latest24$perdiff)


## Version 2 - Final PM2.5 based on average between the two sensors if below 20% if over 100ug/m3
purpleair_latest24$pm2_5_atm_final <- ifelse(purpleair_latest24$pm2_5_atm_a <= 100 & purpleair_latest24$pm2_5_atm_b_ <= 100 & purpleair_latest24$diff < 11, (purpleair_latest24$pm2_5_atm_a + purpleair_latest24$pm2_5_atm_b_)/2,
                                             ifelse(purpleair_latest24$pm2_5_atm_a > 100 & purpleair_latest24$pm2_5_atm_b_ > 100 & purpleair_latest24$perdiff < 0.21, (purpleair_latest24$pm2_5_atm_a + purpleair_latest24$pm2_5_atm_b_)/2, NA))



#################################################################
## Check Summary Stats of Temperature and Relative Humidity    ##
################################################################

## If temperature is above 120F or Equal to 0, set to NA
summary(purpleair_latest24$current_temp_f)

purpleair_latest24$tempf_updated <- ifelse(purpleair_latest24$current_temp_f > 120 | purpleair_latest24$current_temp_f == 0 , NA, purpleair_latest24$current_temp_f )


## If relative humidity is above 98 or equal to 0 , set to NA

summary(purpleair_latest24$current_humidity)

purpleair_latest24$current_humidity <- ifelse(purpleair_latest24$current_humidity >= 98 | purpleair_latest24$current_humidity == 0 , NA,purpleair_latest24$current_humidity )





## SAVE OUTPUT -----

out_dir <- "/Users/lewiswhite/CHAP_columbia/GRAPHS/exposure_analysis/weekly_data_checks/purpleair/processed_data"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

saveRDS(purpleair_latest24, file.path(out_dir, paste0("purpleair_latest24_", Sys.Date(), ".rds")))
# write_csv(purpleair_latest24, file.path(out_dir, paste0("purpleair_latest24_", Sys.Date(), ".csv")))

# save removed rows
saveRDS(purpleair_rejects, file.path(out_dir, paste0("purpleair_rejects_",Sys.Date(), ".rds")))




##############################################
#####  Potential Troubleshooting      ########
##############################################


#############################################
###    Modify DateTime for PurpleAir      ###
###    Some have mismatched timestamps    ###
#############################################

# 
# 
# purpleair_latest24_subset <- purpleair_latest24[grepl(pattern ="^[0-9]{4}/[0-9]{2}/[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}z$",purpleair_latest24$UTCDateTime)|grepl(pattern ="^[0-9]{4}/[0-9]{2}/[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",purpleair_latest24$UTCDateTime)|grepl(pattern ="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",purpleair_latest24$UTCDateTime),]
# 
# 
# 
# ## Convert UTCDateTime to POSIXct datetime
# 
# purpleair_latest24_subset$timestamp <- as.POSIXct(purpleair_latest24_subset$UTCDateTime, format= "%Y/%m/%dT%H:%M:%Sz", tz="UTC")
# 
# ##Correct for datetimes (correct dates if they don't match the csv file)
# purpleair_latest24_subset$date_filename <-  as.Date(sub(".*/(.*)\\.csv", "\\1", purpleair_latest24_subset$filename), format = "%Y%m%d")
# purpleair_latest24_subset$timestamp_date <- as.Date(purpleair_latest24_subset$timestamp)
# purpleair_latest24_subset$timestamp_time <- format(purpleair_latest24_subset$timestamp, format = "%H:%M:%S")
# 
# 
# ##Paste dates together if dates don't match
# purpleair_latest24_subset$corrected_timestamp <- as.POSIXct(ifelse(purpleair_latest24_subset$timestamp_date == purpleair_latest24_subset$date_filename, purpleair_latest24_subset$timestamp, as.POSIXct(paste0(purpleair_latest24_subset$date_filename," ", purpleair_latest24_subset$timestamp_time), format = "%Y-%m-%d %H:%M:%S", tz= "GMT")), tz = "GMT")
# 
# ##Subset Time that didn't match pattern
# 
# purpleair_latest24_mismatchtime<- purpleair_latest24[!grepl(pattern =c("^[0-9]{4}/[0-9]{2}/[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}z$","^[0-9]{4}/[0-9]{2}/[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$","^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"), purpleair_latest24$UTCDateTime),]
# 
# # unique(purpleair_oct24_mismatchtime$vname)
# 
# #Subset mismatch times (Times originally in the mismatch time dataframe but have an acceptable datetime foramt)
# purpleair_latest24_mismatchtime_correct <- subset(purpleair_latest24_mismatchtime, subset = grepl(pattern = "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$", purpleair_oct24_mismatchtime$UTCDateTime))
# 
# purpleair_latest24_mismatchtime_correct$timestamp <- as.POSIXct(purpleair_latest24_mismatchtime_correct$UTCDateTime, format= "%Y-%m-%dT%H:%M:%SZ", tz="UTC")
# 
# 
# ##Correct for datetimes (loop through and correct dates if they don't match the csv file)
# purpleair_latest24_mismatchtime_correct$date_filename <-  as.Date(sub(".*/(.*)\\.csv", "\\1", purpleair_latest24_mismatchtime_correct$filename), format = "%Y%m%d")
# purpleair_latest24_mismatchtime_correct$timestamp_date <- as.Date(purpleair_latest24_mismatchtime_correct$timestamp)
# purpleair_latest24_mismatchtime_correct$timestamp_time <- format(purpleair_latest24_mismatchtime_correct$timestamp, format = "%H:%M:%S")
# 
# ##Paste dates together if dates don't match
# purpleair_latest24_mismatchtime_correct$corrected_timestamp <- as.POSIXct(ifelse(purpleair_latest24_mismatchtime_correct$timestamp_date == purpleair_latest24_mismatchtime_correct$date_filename, purpleair_latest24_mismatchtime_correct$timestamp, as.POSIXct(paste0(purpleair_latest24_mismatchtime_correct$date_filename," ",purpleair_latest24_mismatchtime_correct$timestamp_time), format = "%Y-%m-%d %H:%M:%S", tz= "GMT")), tz = "GMT")
# 
# 
# ## Bind rows of corrected timestamps
# purpleair_latest24_v2 <- bind_rows(purpleair_latest24_subset,purpleair_latest24_mismatchtime_correct)
# 
# 
# 
# 
# #################################################################
# ### Cross-check mismatch times                                 ##
# ##  Reference raw csv files                                    ##
# #################################################################
# 
# #Times that didn't match any of the patterns above
# purpleair_latest24_v2_mismatchtime2 <- subset(purpleair_latest24_mismatchtime , subset = !grepl(pattern = "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$", purpleair_latest24_mismatchtime$UTCDateTime))
# 
# ##Re-read in these files if some seem to be offset by 1 column
# 



