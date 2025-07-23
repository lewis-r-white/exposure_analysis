#############################################
######  QuantAQ Modulair API Download  ######
######  Weekly Tracker to Record Flags  #####
#############################################


#Purpose: Check for any flags for all monitors in fleet to assess if monitors require additional attention


#Outputs:
# ghana_grp_per - Dataframe containing failure percentages by monitor (notify team of monitors with OPC/Nephelometer, sD card, relative humidity/temperature and/or gas sensor failures)
# summary_mod_ - Dataframe containing sensor flags - notify team if sD card failure flag is present


##Load Dependancies
library(QuantAQAPIClient)
library(here) 
library(lubridate) 
library(tictoc)
library(DT)
library(purrr)
library(tidyverse)
library(jsonlite)

#Check that 'raw=TRUE' to ensure raw data is extracted

### Connect to QuantAQ API 
setup_client() #log in to QuantAQ account and click developer in the left menu to grab API key

### Define start_date and end_date for a particular month
start_date <- Sys.Date() -  days(6) 
end_date <- Sys.Date() 

#Source code from Sanne Glastra to import Modulair data from PRISMA KHRC fleet (use raw to see flags)
source(here::here("weekly_Data_checks", "api-processed-ghana_raw.R")) 


# Define the flag values (https://docs.quant-aq.com/modulair)
FLAG_STARTUP <- 1  #also for mod-pm
FLAG_OPC <- 2 #also for mod-pm
FLAG_NEPH <- 4 #also for mod-pm
FLAG_RHTP <- 8 #also for mod-pm
FLAG_CO <- 16
FLAG_NO <- 32
FLAG_NO2 <- 64
FLAG_O3 <- 128
FLAG_CO2 <- 256
FLAG_SO2 <- 512
FLAG_H2S <- 1024
FLAG_BAT <- 2048
FLAG_OVERHEAT <- 4096 #also for mod-pm
FLAG_SD <- 8192 #also for mod-pm


decode_flags <- function(flag_value) {
  flags <- c()
  if(!is.na(flag_value)){
    if (bitwAnd(flag_value, FLAG_STARTUP) != 0) flags <- c(flags, "FLAG_STARTUP")
    if (bitwAnd(flag_value, FLAG_OPC) != 0) flags <- c(flags, "FLAG_OPC")
    if (bitwAnd(flag_value, FLAG_NEPH) != 0) flags <- c(flags, "FLAG_NEPH")
    if (bitwAnd(flag_value, FLAG_RHTP) != 0) flags <- c(flags, "FLAG_RHTP")
    if (bitwAnd(flag_value, FLAG_CO) != 0) flags <- c(flags, "FLAG_CO")
    if (bitwAnd(flag_value, FLAG_NO) != 0) flags <- c(flags, "FLAG_NO")
    if (bitwAnd(flag_value, FLAG_NO2) != 0) flags <- c(flags, "FLAG_NO2")
    if (bitwAnd(flag_value, FLAG_O3) != 0) flags <- c(flags, "FLAG_O3")
    if (bitwAnd(flag_value, FLAG_CO2) != 0) flags <- c(flags, "FLAG_CO2")
    if (bitwAnd(flag_value, FLAG_SO2) != 0) flags <- c(flags, "FLAG_SO2")
    if (bitwAnd(flag_value, FLAG_H2S) != 0) flags <- c(flags, "FLAG_H2S")
    if (bitwAnd(flag_value, FLAG_BAT) != 0) flags <- c(flags, "FLAG_BAT")
    if (bitwAnd(flag_value, FLAG_OVERHEAT) != 0) flags <- c(flags, "FLAG_OVERHEAT")
    if (bitwAnd(flag_value, FLAG_SD) != 0) flags <- c(flags, "FLAG_SD")
  }
  return(flags)
}


flags <- c("startup","opc","neph","rhtp","co","no","no2","o3","co2","so2","h2s","bat","overheat","sd")
flag_value <- c(1,2,4,8,16,32,64,128,256,512,1024,2048,4096,8192)

for(i in 1:length(flags)){
  
  ghana_df[,paste0(flags[i], "_decoded")] <- ifelse(bitwAnd(ghana_df$flag, flag_value[i]) != 0, 1 , 0)
  
  
}

ghana_df_grp <- ghana_df %>% group_by(monitor) %>% dplyr::summarise(count_startup = sum(startup_decoded == 1,na.rm = TRUE), count_opc = sum(opc_decoded == 1, na.rm = TRUE),
                                                               count_neph = sum(neph_decoded == 1, na.rm = TRUE), count_rhtp = sum(rhtp_decoded == 1, na.rm = TRUE),
                                                               count_co = sum(co_decoded == 1 , na.rm = TRUE), count_no = sum(no_decoded == 1, na.rm = TRUE),
                                                               count_no2 = sum(no2_decoded == 1, na.rm = TRUE), count_o3 = sum(o3_decoded == 1 , na.rm =TRUE),
                                                               count_co2 = sum(co2_decoded == 1, na.rm = TRUE), count_so2 = sum(so2_decoded == 1, na.rm = TRUE),
                                                               count_h2s = sum(h2s_decoded == 1 , na.rm = TRUE), count_bat = sum(bat_decoded == 1, na.rm = TRUE),
                                                               count_bat = sum(bat_decoded == 1, na.rm = TRUE), count_overheat = sum(overheat_decoded == 1, na.rm = TRUE),
                                                               count_sd = sum(sd_decoded == 1 , na.rm = TRUE), total = n(),total_pm25readings = sum(!is.na(neph_pm25)), total_pm25readings2 = sum(!is.na(neph.pm25)), 
                                                               total_pmreadings = total_pm25readings + total_pm25readings2, total_pmreading_percent = round((total_pmreadings/total)*100,1)
                                                               ,total_pm25readings_opc = sum(!is.na(opc_pm25)), total_pm25readings_opc2 = sum(!is.na(opc.pm25)), 
                                                               total_pmreadings_opc = total_pm25readings_opc + total_pm25readings_opc2)


for(col in colnames(ghana_df_grp)[2:15]){
  
  if (col == "count_neph"){
  
  #Compute percentage based on available PM2.5 data from nephelometer    
  ghana_df_grp[,paste0(col,"_per")] <- round((ghana_df_grp[,col]/ghana_df_grp$total_pmreadings) * 100,1)
  } else if (col == "count_opc"){
    
  #Compute percentage based on available PM2.5 data from OPC sensor    
  ghana_df_grp[,paste0(col,"_per")] <- round((ghana_df_grp[,col]/ghana_df_grp$total_pmreadings_opc) * 100,1)
  
  }else{
    
  ghana_df_grp[,paste0(col,"_per")] <- round((ghana_df_grp[,col]/ghana_df_grp$total) * 100,1)
    
  }
  
}


ghana_grp_per <- ghana_df_grp[,c(1,24:37,20)]

write_csv(ghana_grp_per, here::here("weekly_data_checks", "modulair_check_outputs", paste0("modulair_flag_percent_", Sys.Date(), ".csv")))

################################################################################################################################

# Decode the flags for each row
ghana_df$decoded_flags <- lapply(ghana_df$flag, decode_flags)


data_list <- split(ghana_df, f = ghana_df$monitor)

summary_mod <- data.frame( Monitor = rep(NA, length(data_list)),
                           Latest_Flag = rep(NA, length(data_list)),
                           Any_Flag_24hr = rep(NA, length(data_list)),
                           Any_Flag_7days = rep(NA, length(data_list)),
                           # Any_Flag_30days = rep(NA, length(data_list)),
                           # Any_Flag_60days = rep(NA, length(data_list)),
                           Rows_of_Data = rep(NA, length(data_list)))


for(i in 1:length(data_list)){
  data_list[[i]] <- data_list[[i]][complete.cases(data_list[[i]][,c(1:6)]),] #limits to recorded time
  data_list[[i]] <- data_list[[i]] %>% arrange(desc(timestamp_local))
  summary_mod$Monitor[i] <- names(data_list)[i]
  summary_mod$Latest_Flag[i] = ifelse(length(Filter(Negate(is.null),data_list[[i]]$decoded_flags[1])) > 0, Filter(Negate(is.null),data_list[[i]]$decoded_flags[1]), "None")
  summary_mod$Any_Flag_24hr[i] =  paste(Filter(Negate(is.null),unique(data_list[[i]]$decoded_flags[1:1440])), collapse = ", ") #last 24 hours (filter out null)
  summary_mod$Any_Flag_7days[i] =  paste(Filter(Negate(is.null),unique(data_list[[i]]$decoded_flags[1:10080])), collapse = ", ") #last 7 days (in minutes)
  # summary_mod$Any_Flag_30days[i]=  paste(Filter(Negate(is.null),unique(data_list[[i]]$decoded_flags[1:43200])),collapse = ", ")
  # summary_mod$Any_Flag_60days[i]=  paste(Filter(Negate(is.null),unique(data_list[[i]]$decoded_flags[1:86400])),collapse = ", ")
  summary_mod$Rows_of_Data[i] = nrow(data_list[[i]])
}

summary_mod$Any_Flag_24hr_json <- sapply(summary_mod$Any_Flag_24hr, toJSON)
summary_mod$Latest_Flag_json <- sapply(summary_mod$Latest_Flag, toJSON)
summary_mod$Any_Flag_7days_json <- sapply(summary_mod$Any_Flag_7days, toJSON)
summary_mod_ <- summary_mod[,c("Monitor","Latest_Flag_json","Any_Flag_24hr_json","Any_Flag_7days_json")]

write_csv(summary_mod_, here::here("weekly_data_checks", "modulair_check_outputs", paste0("modulair_latestflag_", Sys.Date(), ".csv")))


#




