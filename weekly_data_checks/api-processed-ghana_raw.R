


# -------------------------------- GHANA ------------------------------------------------------------
# 
# ghana_devices <- c("MOD-PM-01054", "MOD-PM-00900", "MOD-PM-00876", "MOD-PM-00882", "MOD-PM-00896",
#                    "MOD-PM-00897", "MOD-PM-00892", "MOD-PM-00877", "MOD-PM-01060", "MOD-PM-01055",
#                    "MOD-PM-00884", "MOD-PM-01056", "MOD-PM-01051", "MOD-PM-01059", "MOD-PM-00881",
#                    "MOD-PM-00891", "MOD-PM-00898", "MOD-PM-01052", "MOD-00400", "MOD-PM-00894",
#                    "MOD-PM-01053", "MOD-PM-00887", "MOD-PM-00886", "MOD-PM-00879", "MOD-PM-00890",
#                    "MOD-PM-00889", "MOD-PM-00899", "MOD-PM-00883", "MOD-PM-00895", "MOD-PM-01057",
#                    "MOD-PM-01058", "MOD-PM-00893", "MOD-PM-00878", "MOD-PM-00888", "MOD-PM-00885",
#                    "MOD-00398", "MOD-00401", "MOD-00399", "MOD-00397", "MOD-PM-00880")

# ghana_devices <- c("MOD-00398", "MOD-00401", "MOD-00399", "MOD-00397","MOD-00400", "MOD-00077")


#Updated to include additional monitors added
ghana_devices <-  c("MOD-00397","MOD-00398","MOD-00399","MOD-00400","MOD-00401","MOD-PM-00876","MOD-PM-00877", "MOD-PM-00878" ,"MOD-PM-00879", "MOD-PM-00880", "MOD-PM-00881", "MOD-PM-00882", "MOD-PM-00883", "MOD-PM-00884",
"MOD-PM-00885", "MOD-PM-00886", "MOD-PM-00887", "MOD-PM-00888", "MOD-PM-00889", "MOD-PM-00890", "MOD-PM-00891", "MOD-PM-00892", "MOD-PM-00893", "MOD-PM-00894" ,"MOD-PM-00895" ,"MOD-PM-00896" ,"MOD-PM-00897" ,"MOD-PM-00898",
"MOD-PM-00899", "MOD-PM-00900", "MOD-PM-01051", "MOD-PM-01052","MOD-PM-01053","MOD-PM-01054","MOD-PM-01055","MOD-PM-01056","MOD-PM-01057","MOD-PM-01058","MOD-PM-01059","MOD-PM-01060","MOD-00077", "MOD-PM-00871",
"MOD-PM-00872","MOD-PM-01071","MOD-PM-01072","MOD-PM-01073","MOD-PM-01074","MOD-PM-01075","MOD-PM-01076","MOD-PM-01077","MOD-PM-01078","MOD-PM-01079","MOD-PM-01080","MOD-PM-01081","MOD-PM-01082","MOD-PM-01083",
"MOD-PM-01084","MOD-PM-01085","MOD-PM-01086","MOD-PM-01087","MOD-PM-01088","MOD-PM-01089","MOD-PM-01090","MOD-PM-01091","MOD-PM-01092","MOD-PM-01093")


Sys.setenv(TZ = 'Africa/Accra') #GMT for Ghana

country = "Ghana"

# List of device serial numbers
device_list <- ghana_devices

# Initialize an empty list to store results for each device
result_combined <- list()

# Loop through each device in the device list
for (device in device_list) {
  # Function to get data by date, handling errors
  get_data_safe <- possibly(get_data_by_date,otherwise = NULL)
  
  # Use map to get data for each date, handling errors
  result_list <- map(seq(start_date, end_date, by = "days"), function(date) {
    formatted_date <- format(date, "%Y-%m-%d")
    get_data_safe(sn = device, date = formatted_date, raw = TRUE)
  })
  
  # Filter out NULL elements (empty lists)
  result_list <- purrr::discard(result_list, ~ is.null(.x) || length(.x) == 0)
  
  if (!is_empty(result_list)) {
    # Combine the list of data frames into a single data frame
    result_df <- do.call(bind_rows, lapply(result_list, as.data.frame)) %>%
      mutate(monitor = device) %>%
      dplyr::select(monitor, everything()) %>%
      mutate(timestamp = as.POSIXct(timestamp)) %>% 
      mutate(timestamp = format(timestamp, "%Y-%m-%d %H:%M")) %>% 
      mutate(timestamp = lubridate::ymd_hm(timestamp)) %>%
      mutate(local_timestamp = timestamp) #local timestamp is the same as the timestamp variable for Ghana (GMT) 
    
    minutely_df <- data.frame(timestamp = seq.POSIXt(
      as.POSIXct(start_date, tz = "UTC"),
      as.POSIXct(end_date + 1, tz = "UTC"),
      by = "min"
    )) %>%
      mutate(local_timestamp = timestamp)
    
    result_df_full <- full_join(result_df, minutely_df) %>% 
      arrange(timestamp) %>%
      mutate(date = as.Date(local_timestamp)) %>%  
      mutate(hour = lubridate::hour(local_timestamp)) %>% #lubridate::ymd_hms()
      mutate(monitor = device) %>%
      dplyr::select(monitor, timestamp, local_timestamp, date, hour, everything()) 
    
    # Store the result for the current device in the combined list
    result_combined[[device]] <- result_df_full
  } else {
    # If there's no data for this device, create an empty dataframe
    minutely_df_empty <- data.frame(timestamp = seq.POSIXt(
      as.POSIXct(start_date, tz = "UTC"),
      as.POSIXct(end_date + 1, tz = "UTC"),
      by = "min"
    )) %>%
      mutate(local_timestamp = timestamp)
    
    minutely_df_empty <- minutely_df_empty %>% mutate(date = as.Date(local_timestamp)) %>%  
      mutate(hour = lubridate::hour(local_timestamp)) %>% #lubridate::ymd_hms()
      mutate(monitor = device) %>%
      dplyr::select(monitor, timestamp, local_timestamp, date, hour) 
    
    result_combined[[device]] <- minutely_df_empty
  }
}

# Combine data for all devices into a single data frame
final_result_df <- bind_rows(result_combined)

# Store data into specific dataset for ghana
ghana_df <- final_result_df



