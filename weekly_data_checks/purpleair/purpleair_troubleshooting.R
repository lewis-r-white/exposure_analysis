##########################################################
############## Purple Air Monitors #######################
#########  Code for Troubleshooting Various Scenarios ####
##########################################################


# This script contains code to troubleshoot PurpleAir files based on previous issues which have occurred 
# thus this is not an extensive list of all possible scenarios, rather, this script contains code to rectify issues which have previously occured.



###############################
#### Mismatched Timestamps  ###
###############################

#Subset file based on UTC DateTime if it matches the pattern below
file_subset <- file[grepl(pattern ="^[0-9]{4}/[0-9]{2}/[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}z$",file$UTCDateTime)|grepl(pattern ="^[0-9]{4}/[0-9]{2}/[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",file$UTCDateTime)|grepl(pattern ="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",file$UTCDateTime),]



## Convert UTCDateTime to POSIXct datetime

file_subset$timestamp <- as.POSIXct(file_subset$UTCDateTime, format= "%Y/%m/%dT%H:%M:%Sz", tz="UTC")

##Correct for datetimes 
file_subset$date_filename <-  as.Date(sub(".*/(.*)\\.csv", "\\1", file_subset$filename), format = "%Y%m%d")
file_subset$timestamp_date <- as.Date(file_subset$timestamp)
file_subset$timestamp_time <- format(file_subset$timestamp, format = "%H:%M:%S")


##Paste dates together if dates don't match
file_subset$corrected_timestamp <- as.POSIXct(ifelse(file_subset$timestamp_date == file_subset$date_filename, file_subset$timestamp, as.POSIXct(paste0(file_subset$date_filename," ", file_subset$timestamp_time), format = "%Y-%m-%d %H:%M:%S", tz= "GMT")), tz = "GMT")

##Subset Time that didn't match pattern

file_mismatchtime <- file[!grepl(pattern =c("^[0-9]{4}/[0-9]{2}/[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}z$","^[0-9]{4}/[0-9]{2}/[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$","^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"), file$UTCDateTime),]

# unique(file_mismatchtime$vname)

#Subset mismatch times (Times originally in the mismatch time dataframe but have an acceptable datetime foramt)
file_mismatchtime_correct <- subset(file_mismatchtime , subset = grepl(pattern = "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$", file_mismatchtime$UTCDateTime))

file_mismatchtime_correct$timestamp <- as.POSIXct(file_mismatchtime_correct$UTCDateTime, format= "%Y-%m-%dT%H:%M:%SZ", tz="UTC")


##Correct for datetimes (loop through and correct dates if they don't match the csv file)
file_mismatchtime_correct$date_filename <-  as.Date(sub(".*/(.*)\\.csv", "\\1", file_mismatchtime_correct$filename), format = "%Y%m%d")
file_mismatchtime_correct$timestamp_date <- as.Date(file_mismatchtime_correct$timestamp)
file_mismatchtime_correct$timestamp_time <- format(file_mismatchtime_correct$timestamp, format = "%H:%M:%S")

##Paste dates together if dates don't match
file_mismatchtime_correct$corrected_timestamp <- as.POSIXct(ifelse(file_mismatchtime_correct$timestamp_date == file_mismatchtime_correct$date_filename, file_mismatchtime_correct$timestamp, as.POSIXct(paste0(file_mismatchtime_correct$date_filename," ",file_mismatchtime_correct$timestamp_time), format = "%Y-%m-%d %H:%M:%S", tz= "GMT")), tz = "GMT")


## Bind rows of corrected timestamps
file_v2 <- bind_rows(file_subset,file_mismatchtime_correct)



###############################
#### Missing  Timestamps   ####
###############################

##These missing timestamps may be due to an error when reading in the file

#remove files missing timestamp (X files)
file_nonmissingtimestamps <- file[!is.na(file$timestamp),]
##Read in these two files again
redo <- list()

for(file in unique(file$filename[is.na(file$timestamp)])){
  
  encoding <- guess_encoding(file)
  
  rawfile <- read_csv(file, col_names = colname_list ,show_col_types = FALSE, skip_empty_rows = T,locale = readr::locale(encoding = encoding$encoding), guess_max = 10000)[-1,]
  
  problems <- problems(rawfile)
  bad_rows <- problems$row
  
  # Count the number of  problematic rows
  rawfile$badrows <- nrow(problems)
  
  
  # Make sure that all columns, aside from the ones mentioned below, are converted to numeric
  
  rawfile[, names(rawfile) != c("UTCDateTime", "timestamp","mac_address", "hardware")] <- lapply(rawfile[, names(rawfile) != c("UTCDateTime", "timestamp","mac_address", "hardware")], as.numeric)
  
  rawfile$community <- tolower(str_extract( basename(dirname(unique(file))),"[^_]+"))
  rawfile$filename <- file
  
  ## Create a second column for community to catch if for some reason, the first one didn't work and returns 'NA'
  rawfile$community_2 <- tolower(str_extract(basename(dirname(unique(file))),"[^_]+")) #based on the full filepath in the 'filename' columns
  rawfile$vname <- ifelse(is.na(rawfile$community), rawfile$community_2, rawfile$community)
  rawfile <- rawfile %>% dplyr::select(-(c(community,community_2)))
  
  
  rawfile$timestamp <- as.POSIXct(rawfile$UTCDateTime, format= "%Y/%m/%dT%H:%M:%Sz", tz="UTC")
  
  rawfile$tempc <- (rawfile$current_temp_f - 32) * (5/9) 
  
  
  
  ##Maximum Standard Range : >= 1,000 ug/m3 - Set These to Missing?(https://www2.purpleair.com/products/purpleair-pa-ii)
  ##Effective Range Set to 0 - 500ug/m3 (Limit to this?)
  rawfile$pm2_5_atm_a <- ifelse(rawfile$pm2_5_atm > 1000 , NA, rawfile$pm2_5_atm)
  
  rawfile$pm2_5_atm_b_ <- ifelse(rawfile$pm2_5_atm_b > 1000 , NA, rawfile$pm2_5_atm_b)
  
  
  
  
  
  ## Compute difference and percent difference
  rawfile$perdiff <- abs(rawfile$pm2_5_atm_a - rawfile$pm2_5_atm_b_)/rawfile$pm2_5_atm_a 
  
  rawfile$diff <- abs(rawfile$pm2_5_atm_a - rawfile$pm2_5_atm_b_)
  
  
  #Some sensors have a value of 0 causing the percent difference to be 0 so this is set to NA
  rawfile$perdiff <- ifelse(is.infinite(rawfile$perdiff), NA, rawfile$perdiff)
  
  
  ## Version 2 - Final PM2.5 based on average between the two sensors if below 20% if over 100ug/m3
  rawfile$pm2_5_atm_final <- ifelse(rawfile$pm2_5_atm_a <= 100 & rawfile$pm2_5_atm_b_ <= 100 & rawfile$diff < 11, (rawfile$pm2_5_atm_a + rawfile$pm2_5_atm_b)/2,
                                    ifelse(rawfile$pm2_5_atm_a > 100 & rawfile$pm2_5_atm_b_ > 100 & rawfile$perdiff < 0.21, (rawfile$pm2_5_atm_a + rawfile$pm2_5_atm_b_)/2, NA))
  
  
  
  redo[[file]] <- rawfile
  
}

redo_files <- bind_rows(redo)




##Bind these files to the non-missing timestamps dataset



###########################################
#### Encoded PM and Temp/RH Variables  ####
###########################################


## If variables have encoded values, report back to KHRC team to coordinate a reupload
## Function to extract all numeric values

# Function to extract all numbers including decimals
extract_all_numbers <- function(x) {
  # Extract all numeric parts including decimals
  numbers <- str_extract_all(x, "\\d+\\.\\d+|\\d+")
  
  # Flatten the list and convert to numeric
  numeric_values <- as.numeric(unlist(numbers))
  
  # Handle case where no numbers are found
  if (length(numeric_values) == 0) {
    return(NA)
  }
  
  return(numeric_values)
  
  
  
  
  
}