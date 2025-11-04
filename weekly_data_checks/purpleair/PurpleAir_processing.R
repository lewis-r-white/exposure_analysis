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
purpleair <- list.dirs(path = "", full.names = FALSE, recursive = FALSE) #list to director containing PurpleAir Folders




list.purpleair <- list()
problem_files <- list()

##################################################
### Read in all csv files for each community    ##
##################################################

for(i in 1:length(purpleair)){
  
  filenames_purpleair <- list.files(path = paste0( "",purpleair[i]),recursive = T, pattern = "*.csv", full.names = T) #list file path to directory containing PurpleAir files
  
  list.files <- list()
  
  colname_list <-c("UTCDateTime","mac_address","firmware_ver","hardware" ,"current_temp_f","current_humidity",   "current_dewpoint_f","pressure" ,"adc" ,"mem", "rssi","uptime","pm1_0_cf_1","pm2_5_cf_1","pm10_0_cf_1","pm1_0_atm","pm2_5_atm","pm10_0_atm" ,"pm2.5_aqi_cf_1", "pm2.5_aqi_atm" , "p_0_3_um","p_0_5_um","p_1_0_um","p_2_5_um","p_5_0_um","p_10_0_um" ,"pm1_0_cf_1_b","pm2_5_cf_1_b","pm10_0_cf_1_b", "pm1_0_atm_b" ,"pm2_5_atm_b" ,"pm10_0_atm_b","pm2.5_aqi_cf_1_b","pm2.5_aqi_atm_b" , "p_0_3_um_b","p_0_5_um_b","p_1_0_um_b","p_2_5_um_b"  ,"p_5_0_um_b" ,"p_10_0_um_b" ,"gas")
  
  for(j in 1:length(filenames_purpleair)){
    
    tryCatch({
      
      encoding <- guess_encoding(filenames_purpleair[j])
      
      rawfile <- read_csv(filenames_purpleair[j], col_names = colname_list ,show_col_types = FALSE, skip_empty_rows = T,locale = readr::locale(encoding = encoding$encoding), guess_max = 10000)[-1,]
      
      problems <- problems(rawfile)
      bad_rows <- problems$row
      
      # Count the number of  problematic rows
      
      rawfile$badrows <- nrow(problems)
      
      
      # Make sure that all columns, aside from the ones mentioned below, are converted to numeric
      list.files[[j]] <- rawfile
      list.files[[j]][, names(list.files[[j]]) != c("UTCDateTime", "timestamp","mac_address", "hardware")] <- lapply(list.files[[j]][, names(list.files[[j]]) != c("UTCDateTime", "timestamp","mac_address", "hardware")], as.numeric)
      
      list.files[[j]]$community <- tolower(str_extract(purpleair[i],"[^_]+"))
      list.files[[j]]$filename <- filenames_purpleair[j]
      files <- rbindlist(list.files, fill = TRUE)
      
      ## Create a second column for community to catch if for some reason, the first one didn't work and returns 'NA'
      files$community_2 <- tolower(str_extract(purpleair[i],"[^_]+"))
      files$vname <- ifelse(is.na(files$community), files$community_2, files$community)
      files <- files %>% dplyr::select(-(c(community,community_2)))
      
    },
    error =function(e){
      
      warning(paste0("Trying to read in ",filenames_purpleair[i]," using an alternative method."))
      
      filepath <- filenames_purpleair[j]
      
      encoding <- guess_encoding(filepath)
      
      rawfile <- read_csv(filepath, col_names = colname_list ,show_col_types = FALSE)[-1,]
      
      problems <- problems(rawfile)
      bad_rows <- problems$row
      
      # Remove the problematic rows
      
      rawfile$badrows <- nrow(problems)
      
      list.files[[j]] <-  as.data.frame(lapply(rawfile, iconv, from = "ASCII" , to = "UTF8"))
      
      list.files[[j]][, names(list.files[[j]]) != c("UTCDateTime", "timestamp","mac_address", "hardware")] <- lapply(list.files[[j]][, names(list.files[[j]]) != c("UTCDateTime", "timestamp","mac_address", "hardware")], as.numeric)
      list.files[[j]]$community <- tolower(str_extract(purpleair[i],"[^_]+"))
      list.files[[j]]$filename <- filenames_purpleair[j]
      files <- rbindlist(list.files, fill = TRUE)
      
      ## Create a second column for community to catch if for some reason, the first one didn't work and returns 'NA'
      files$community_2 <- tolower(str_extract(purpleair[i],"[^_]+"))
      files$vname <- ifelse(is.na(files$community), files$community_2, files$community)
      files <- files %>% dplyr::select(-(c(community,community_2)))
    }, finally={
      next})
  }         
  
  list.purpleair[[i]] <- files
  
  
  
}


## Combine into one dataframe

purpleair_latest24 <- rbindlist(list.purpleair)




#################################
####   Data Processing       ####
#################################


#Convert time to POSIXct object
purpleair_latest24$timestamp <- as.POSIXct(purpleair_latest24$UTCDateTime, format= "%Y/%m/%dT%H:%M:%Sz", tz="UTC")

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
purpleair_latest24$pm2_5_atm_final <- ifelse(purpleair_latest24$pm2_5_atm_a <= 100 & purpleair_latest24$pm2_5_atm_b_ <= 100 & purpleair_latest24$diff < 11, (purpleair_latest24$pm2_5_atm_a + purpleair_latest24$pm2_5_atm_b)/2,
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



