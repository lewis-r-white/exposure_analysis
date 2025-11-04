### Ghana round 3 follow up microPEM and UPAS data check 2021_12_15_Alhassan_Akura
### Author: Qiang Yang
### last accessed: 2021-12-16
# note: new upas wearing compliance and plotting codes

# progress: line

### loading packages
library(ggplot2)
library(plotly)
library(scales)
library(lubridate)
library(data.table)
library(gridExtra)
library(cowplot)
library(plyr)
library(dplyr)
library(grid)
library(zoo)
library(openxlsx)
library(beanplot)
library(readr)
library(htmlwidgets)
library(changepoint)
library(leaflet)

# set universal plotting theme
theme.timeseries <- theme_bw() +
  theme(aspect.ratio=2/6,
        axis.text.y   = element_text(size=18),
        axis.text.x   = element_text(size=15,angle = 30, hjust = 1),
        axis.title.y  = element_text(size=18),
        #axis.title.x  = element_text(size=18),
        panel.background = element_blank(),
        panel.grid.major = element_line(size=1, color="gray90", linetype="solid"),
        panel.grid.minor = element_blank(),
        axis.line = element_line(color = "black"),
        panel.border = element_rect(color = "black", fill=NA, size=1),
        plot.title = element_text(size=18,hjust = 0.5),
        legend.key = element_rect(fill = "white"),
        legend.title = element_blank(),
        legend.text = element_text(size=15))

theme.scatter <- theme_bw() +
  theme(aspect.ratio=4/6,
        axis.text.y   = element_text(size=18),
        axis.text.x   = element_text(size=18),
        axis.title.y  = element_text(size=18),
        axis.title.x  = element_text(size=18),
        panel.background = element_blank(),
        panel.grid.major = element_line(size=1, color="gray97", linetype="solid"),
        panel.grid.minor = element_blank(),
        axis.line = element_line(color = "black"),
        panel.border = element_rect(color = "black", fill=NA, size=1),
        plot.title = element_text(size=18,hjust = 0.5),
        legend.key = element_rect(fill = "white"),
        legend.title = element_blank(),
        legend.text = element_text(size=15))

# SET WORKING DIRECTORY
getwd()
#setwd("G:/My Drive/GHANA/microPEM/round2/")

## setwd("G:/.shortcut-targets-by-id/1c9Mri43wQM3JQbOh4Rt86UcSQipG6B_D/Child_Lung_Function_Data_Ghana/Data_QC_processing/followup_data/weekly")

################
### GPS ###
##############
file_gps <- list.files("../../../FOLLOWUP DATA/2022_07_14_UPAS WEILA NEW LONGORO", 
                            pattern = "GPS.*", full.names = F)
print(file_gps)

list.gps <- list()

for(i in 1:length(file_gps)) {
  filepath_gps <- file.path("../../../FOLLOWUP DATA/2022_07_14_UPAS WEILA NEW LONGORO", file_gps[i])
  list.gps[[i]] <- read.csv(filepath_gps, header = T)%>%
    mutate(datetime = as.character(DATE*1000000+TIME),
           timestamp = as.POSIXct(datetime, format="%y%m%d%H%M%S", tz = "UTC"),
           unit_id = substr(file_gps[i], 1, 7),
           filter_id   = substr(file_gps[i], 28, 34),
           subject_id  = substr(file_gps[i], 20, 26),
           lon = 0 - as.numeric(substr(LONGITUDE.E.W, 1, 8)),
           lat = as.numeric(substr(LATITUDE.N.S, 1, 8))
           )
}

test_gps_list1 <- list.gps[[1]]


function_onemin_gps <- function(df){
  df$minute  <- floor_date(df$timestamp, unit = "minute")
  
  df_out <- dplyr::summarise(group_by(df, minute), 
                             unit_id = df$unit_id[1],
                             subject_id = df$subject_id[1],
                             filter_id = df$filter_id[1],
                             lon=mean(lon, na.rm = TRUE),
                             lat=mean(lat, na.rm = TRUE),
                             HEIGHT=mean(HEIGHT,na.rm = TRUE),
                             SPEED = mean(SPEED, na.rm = T),
                             HEADING = mean(HEADING, na.rm=T)
  )
  
  df_out$timestamp <- df_out$minute
  df_out <- subset(df_out, abs(df_out$lat) > 0)    # remove lines without reading
  df_out <- subset(df_out, abs(as.numeric(df_out$timestamp)) >= 0)  # remove NA timestamp
  return(df_out)
}

list.gps.1min <- list()
for (i in 1:length(list.gps)) {
  list.gps.1min[[i]] <- function_onemin_gps(list.gps[[i]])
}

gps_all <- do.call(rbind.fill, list.gps.1min)
#rm(gps_all)
test_gps_list_1min <- list.gps.1min[[1]]


# summarize each session for GPS
summary_gps <- data.frame(
  gps_id = rep(NA, length(list.gps.1min)),
  subject_id = rep(NA, length(list.gps.1min)),
  filter_id = rep(NA, length(list.gps.1min)),
  start_time = rep(NA, length(list.gps.1min)),
  end_time = rep(NA, length(list.gps.1min)),
  session_hour = rep(NA, length(list.gps.1min)),
  run_time_hour = rep(NA, length(list.gps.1min)),
  no_signal_hour = rep(NA, length(list.gps.1min)),
  
  lon_min = rep(NA, length(list.gps.1min)),
  lon_max = rep(NA, length(list.gps.1min)),
  lon_median = rep(NA, length(list.gps.1min)),
  lat_min = rep(NA, length(list.gps.1min)),
  lat_max = rep(NA, length(list.gps.1min)),
  lat_median = rep(NA, length(list.gps.1min))
)

#colnames(gps_all)
for (i in 1:length(list.gps.1min)) {
  summary_gps$gps_id[i] <- list.gps.1min[[i]]$unit_id[1]
  summary_gps$subject_id[i] <- list.gps.1min[[i]]$subject_id[1]
  summary_gps$filter_id[i] <- list.gps.1min[[i]]$filter_id[1]
  summary_gps$start_time[i] <- as.character(list.gps.1min[[i]]$timestamp[1])
  summary_gps$end_time[i] <- as.character(list.gps.1min[[i]]$timestamp[nrow(list.gps.1min[[i]])])
  summary_gps$session_hour[i] <- difftime(summary_gps$end_time[i], summary_gps$start_time[i], units = "hours")
  summary_gps$run_time_hour[i] <- nrow(list.gps.1min[[i]])/60
  summary_gps$no_signal_hour[i] <- summary_gps$session_hour[i] - summary_gps$run_time_hour[i]
  
  summary_gps$lon_min[i] <- min(list.gps.1min[[i]]$lon, na.rm = T)
  summary_gps$lon_max[i] <- max(list.gps.1min[[i]]$lon, na.rm = T)
  summary_gps$lon_median[i] <- median(list.gps.1min[[i]]$lon, na.rm = T)
  summary_gps$lat_min[i] <- min(list.gps.1min[[i]]$lat, na.rm = T)
  summary_gps$lat_max[i] <- max(list.gps.1min[[i]]$lat, na.rm = T)
  summary_gps$lat_median[i] <- median(list.gps.1min[[i]]$lat, na.rm = T)
}


write.csv(summary_gps, "2022-07-14/summary_gps_20220714.csv")


test_gps_list_1min <- list.gps.1min[[1]]

#plot_ly()%>%add_trace(data = list.gps[[1]], x=~lon, y=~lat, type="scatter", mode="markers+lines", name="")


function_plotly_gps <- function(df) {
  
  title = paste0(df$unit_id[1], " ", df$subject_id[1], " from ", df$timestamp[1], "\nStart from green to yellow and end at red")

  df$index_reverse = seq(nrow(df), 1, -1)
  
  pal <- colorNumeric(
    palette = "RdYlGn",
    domain = df$index_reverse)
  
  leaflet(data = df) %>%
    setView(lng = mean(df$lon), lat = mean(df$lat), zoom = 14) %>%
    # addTiles() %>%  # Add default OpenStreetMap map tiles
    addProviderTiles(providers$Esri.WorldTopoMap) %>%
    addCircleMarkers(~lon, ~lat, 
                     label = ~ as.character(timestamp),
                     radius = 3,
                     color = ~pal(index_reverse), 
                     stroke = FALSE, 
                     fillOpacity = 0.5,
                     opacity = 0.5) %>%
    addControl(title, position = "topright") %>%
    addMeasure()
}

function_plotly_gps(list.gps.1min[[1]])


for (i in 1:length(list.gps.1min)) {
  html_filename <- gsub(".CSV", ".html", file_gps[i])
  htmlwidgets::saveWidget(widget = function_plotly_gps(list.gps.1min[[i]]), 
                          file = paste0("2022-07-14/", html_filename), 
                          selfcontained = T, libdir = "lib_gps/")
}



##################
### UPAS data ###
##################
# read test UPAS data
#test_parameter_all <- read_csv(file = "C:/Users/johne/My Drive/GHANA/Child_Lung_Function_Data_Ghana/FOLLOWUP DATA/2021_06_20_Kawampe/PSP00001_LOG_2021-06-17T08_35_26UTC_BM1242M_________KHU0020___.txt.txt", 
#                               col_names = T, n_max = 60)

#test_sample_log <- read_csv(file = "C:/Users/johne/My Drive/GHANA/Child_Lung_Function_Data_Ghana/FOLLOWUP DATA/2021_06_20_Kawampe/PSP00001_LOG_2021-06-17T08_35_26UTC_BM1242M_________KHU0020___.txt.txt", 
#                            col_names = F, skip_empty_rows = T, skip = 86,  n_max = 3)

#test_title <- read_csv(file = "C:/Users/johne/My Drive/GHANA/Child_Lung_Function_Data_Ghana/FOLLOWUP DATA/2021_06_20_Kawampe/PSP00001_LOG_2021-06-17T08_35_26UTC_BM1242M_________KHU0020___.txt.txt", 
#                       col_names = T, skip_empty_rows = T, skip = 88,  n_max = 0)

#test_data <- read_csv(file = "C:/Users/johne/My Drive/GHANA/Child_Lung_Function_Data_Ghana/FOLLOWUP DATA/2021_06_20_Kawampe/PSP00001_LOG_2021-06-17T08_35_26UTC_BM1242M_________KHU0020___.txt.txt", 
#                      skip = 89, skip_empty_rows = T, col_names = T)

# function to read a data file
function_read_UPAS_data <- function(filepath) {
  data_parameter <- read_csv(file = filepath, col_names = T, n_max = 60)
  data_column_name      <- read_csv(file = filepath, col_names = T, skip_empty_rows = T, skip = 88,  n_max = 0)
  data_raw       <- read_csv(file = filepath, skip = 89, skip_empty_rows = T, col_names = T)
  colnames(data_raw) <- colnames(data_column_name)
  
  data <- data_raw %>%
    mutate(hour = as.numeric(substr(as.character(DateTimeLocal), 12, 13)),
           UPASserial = subset(data_parameter, PARAMETER == "UPASserial")$VALUE[1],
           SampleName = subset(data_parameter, PARAMETER == "SampleName")$VALUE[1],
           DutyCycle  = subset(data_parameter, PARAMETER == "FlowDutyCycle")$VALUE[1],
           compliance = ifelse((AccelXVar > 100) | (AccelYVar > 100) | (AccelZVar > 100), 1, 0))
  
  data$compliance_rollmean <- ifelse(as.numeric(rollapply(data$compliance, width=20,  FUN = mean, align = "center", na.rm = TRUE, partial=F, fill = NA)) > 0, 1, 0)
  
  return(data)
}

file_upas <- list.files("../../../FOLLOWUP DATA/2022_07_20_UPAS", 
                        pattern = "*.txt", full.names = F)
print(file_upas)

list.upas <- list()

for(i in 1:length(file_upas)){
  filepath_upas <- file.path("../../../FOLLOWUP DATA/2022_07_20_UPAS", file_upas[i])
  list.upas[[i]] <- function_read_UPAS_data(filepath_upas) %>%
    #filter(Date!="Errored Line" & Date!="A0/14/01") %>%
    #mutate(timestamp = as.POSIXct(paste(transform_date(Date),Time),format="%Y-%m-%d %H:%M:%S", tz="GMT"),
    mutate(#timestamp = as.POSIXct(paste(Date, Time),format="%d/%m/%Y %H:%M:%S", tz="GMT"),
      #upas_id = substr(as.character(gsub(".*/", "", file_upas[i])), 1, 10),
      filter_id   = substr(as.character(gsub(".*/", "", file_upas[i])), 53, 59),
      subject_id  = substr(as.character(gsub(".*/", "", file_upas[i])), 37, 43))
}



test_upas_list1 <- list.upas[[1]]

upas_all <- do.call(rbind.fill, list.upas)
plot_ly()%>%
  add_trace(data = upas_all, x=~DateTimeLocal, y=~PM2_5MC, type = "scatter", mode="markers+lines", 
            color=~paste0("UPAS-", UPASserial))
#rm(upas_all)

# summarize each session for UPAS
summary_upas <- data.frame(
  upas_id = rep(NA, length(list.upas)),
  subject_id = rep(NA, length(list.upas)),
  filter_id = rep(NA, length(list.upas)),
  start_time = rep(NA, length(list.upas)),
  end_time = rep(NA, length(list.upas)),
  run_time_hour = rep(NA, length(list.upas)),
  
  pm_min = rep(NA, length(list.upas)),
  pm_max = rep(NA, length(list.upas)),
  pm_median = rep(NA, length(list.upas)),
  pm_mean = rep(NA, length(list.upas)),
  temp_min = rep(NA, length(list.upas)),
  temp_max = rep(NA, length(list.upas)),
  rh_min = rep(NA, length(list.upas)),
  rh_max = rep(NA, length(list.upas)),
  battery_start = rep(NA, length(list.upas)),
  battery_end = rep(NA, length(list.upas)),
  filterdp_min = rep(NA, length(list.upas)),
  filterdp_max = rep(NA, length(list.upas)),
  flow_min = rep(NA, length(list.upas)),
  flow_max = rep(NA, length(list.upas)),
  flow_mean = rep(NA, length(list.upas)),
  compliance_hours = rep(NA, length(list.upas)),
  compliance_percent = rep(NA, length(list.upas)),
  compliance_hours_daytime = rep(NA, length(list.upas)),
  compliance_hours_nighttime = rep(NA, length(list.upas))
)

#colnames(upas_all)
for (i in 1:length(list.upas)) {
  summary_upas$upas_id[i] <- list.upas[[i]]$UPASserial[1]
  summary_upas$subject_id[i] <- list.upas[[i]]$subject_id[1]
  summary_upas$filter_id[i] <- list.upas[[i]]$filter_id[1]
  summary_upas$start_time[i] <- as.character(list.upas[[i]]$DateTimeLocal[1])
  summary_upas$end_time[i] <- as.character(list.upas[[i]]$DateTimeLocal[nrow(list.upas[[i]])])
  summary_upas$run_time_hour[i] <- nrow(list.upas[[i]])/2/60
  
  summary_upas$pm_min[i] <- min(list.upas[[i]]$PM2_5MC, na.rm = T)
  summary_upas$pm_max[i] <- max(list.upas[[i]]$PM2_5MC, na.rm = T)
  summary_upas$pm_median[i] <- median(list.upas[[i]]$PM2_5MC, na.rm = T)
  summary_upas$pm_mean[i] <- mean(list.upas[[i]]$PM2_5MC, na.rm = T)
  
  summary_upas$temp_min[i] <- min(list.upas[[i]]$AtmoT, na.rm = T)
  summary_upas$temp_max[i] <- max(list.upas[[i]]$AtmoT, na.rm = T)
  summary_upas$rh_min[i] <- min(list.upas[[i]]$AtmoRH, na.rm = T)
  summary_upas$rh_max[i] <- max(list.upas[[i]]$AtmoRH, na.rm = T)
  summary_upas$battery_start[i] <- list.upas[[i]]$BatteryCharge[1]
  summary_upas$battery_end[i] <- list.upas[[i]]$BatteryCharge[nrow(list.upas[[i]])]
  summary_upas$filterdp_min[i] <- min(list.upas[[i]]$FilterDP, na.rm = T)
  summary_upas$filterdp_max[i] <- max(list.upas[[i]]$FilterDP, na.rm = T)
  summary_upas$flow_min[i] <- min(list.upas[[i]]$PumpingFlowRate, na.rm = T)
  summary_upas$flow_max[i] <- max(list.upas[[i]]$PumpingFlowRate, na.rm = T)
  summary_upas$flow_mean[i] <- mean(list.upas[[i]]$PumpingFlowRate, na.rm = T)
  
  summary_upas$compliance_hours[i] <- sum(list.upas[[i]]$compliance_rollmean, na.rm = T)/2/60
  summary_upas$compliance_percent[i] <- sum(list.upas[[i]]$compliance_rollmean, na.rm = T)/nrow(list.upas[[i]])
  rawdata_daytime   <- subset(list.upas[[i]], hour >= 5 & hour < 21) 
  rawdata_nighttime <- subset(list.upas[[i]], hour < 5 | hour >= 21) 
  summary_upas$compliance_hours_daytime[i]   <- sum(rawdata_daytime$compliance_rollmean, na.rm = T)/2/60
  summary_upas$compliance_hours_nighttime[i] <- sum(rawdata_nighttime$compliance_rollmean, na.rm = T)/2/60
  }


write.csv(summary_upas, "2022-07-20/summary_upas_20220720.csv")



## merge GPS with each UPAS data
for (i in 1:length(list.upas)) {
list.upas[[i]]$timestamp <- floor_date(list.upas[[i]]$DateTimeLocal, unit = "minutes")
list.upas[[i]] <- merge(x = list.upas[[i]], y = gps_all, 
                        by = c("timestamp", "filter_id", "subject_id"), all.x = T)
}

##test_upas_list1 <- list.upas[[1]]


### function to plot all parameters for one session
function_UPAS_plotly_all_parameters <- function(df) {
  
  df$AccelX_SD <- ifelse(df$AccelXVar < 0, 0, sqrt(df$AccelXVar)/1000)
  df$AccelY_SD <- ifelse(df$AccelYVar < 0, 0, sqrt(df$AccelYVar)/1000)
  df$AccelZ_SD <- ifelse(df$AccelZVar < 0, 0, sqrt(df$AccelZVar)/1000)
  
  df_pm <- subset(df, abs(PM2_5MC)>=0)
  #df_pm = df
  df_pm$PM2_5stdev <- sqrt(df_pm$PM2_5MCVar)
  
  run_hours <- round(nrow(df)/2/60, 1)
  wearing_hours <- round(sum(df$compliance_rollmean, na.rm=T)/2/60, 1)
  mean_pm25 <- round(mean(df$PM2_5MC, na.rm=T), 1)
  
  plot_pm <- plot_ly() %>%
    #add_trace(data = df_pm, x=~DateTimeLocal, y=~PM1MC,   type = "scatter", mode  = "lines", line = list(color ="gray"), name = "PM1 (ug/m3)", opacity=0.5)%>%            
    #add_trace(data = df_pm, x=~DateTimeLocal, y=~PM2_5MC - PM2_5stdev, 
    #          type = "scatter", mode  = "lines", line = list(color ="grey"), 
    #          name = "PM2.5 (ug/m3) - 1 stdev", opacity=0.8)%>%
    #add_trace(data = df_pm, x=~DateTimeLocal, y=~PM2_5MC + PM2_5stdev, 
    #          type = "scatter", mode  = "lines", line = list(color ="grey"), 
    #          name = "PM2.5 (ug/m3) + 1 stdev", opacity=0.8, fill="tonexty", fillcolor="grey")%>%
    #add_trace(data = df_pm, x=~DateTimeLocal, y=~PM4MC,   type = "scatter", mode  = "lines", line = list(color ="purple"), name = "PM4 (ug/m3)", opacity=0.5)%>%            
    #add_trace(data = df_pm, x=~DateTimeLocal, y=~PM10MC,  type = "scatter", mode  = "lines", line = list(color ="red"), name = "PM10 (ug/m3)", opacity=0.5)%>%            
    #add_trace(data = df_pm, x=~DateTimeLocal, y=~PM1MCVar,   type = "scatter", mode  = "lines", line = list(color ="gray"), name = "PM1 Variance (ug/m3)", opacity=0.5)%>%            
    add_trace(data = df_pm, x=~DateTimeLocal, y=~PM2_5MC, 
              type = "scatter", mode  = "lines", line = list(color ="black"), 
              name = "PM2.5 (ug/m3)", opacity=0.8)%>%            
    #add_trace(data = df_pm, x=~DateTimeLocal, y=~PM4MCVar,   type = "scatter", mode  = "lines", line = list(color ="purple"), name = "PM4 Variance (ug/m3)", opacity=0.5)%>%            
    #add_trace(data = df_pm, x=~DateTimeLocal, y=~PM10MCVar,  type = "scatter", mode  = "lines", line = list(color ="red"), name = "PM10 Variance (ug/m3)", opacity=0.5)%>%            
    layout(xaxis = list(tickangle = -0), 
           margin=list(l = 100),
           yaxis = list(title="PM", tickprefix="   "),
           showlegend = T, 
           font = list(size=16))
  
  plot_particlecount <- plot_ly() %>%
    #add_trace(data = df_pm, x=~DateTimeLocal, y=~PM0_5NC,   type = "scatter", mode  = "lines", line = list(color ="blue"), name = "PM0.5 #/cm3", opacity=0.5)%>%            
    #add_trace(data = df_pm, x=~DateTimeLocal, y=~PM1_NC,     type = "scatter", mode  = "lines", line = list(color ="gray"), name = "PM1 #/cm3", opacity=0.5)%>%            
    add_trace(data = df_pm, x=~DateTimeLocal, y=~PM2_5NC,   type = "scatter", mode  = "lines", line = list(color ="black"), name = "PM2.5 #/cm3", opacity=0.8)%>%            
    #add_trace(data = df_pm, x=~DateTimeLocal, y=~PM4NC,     type = "scatter", mode  = "lines", line = list(color ="purple"), name = "PM4 #/cm3", opacity=0.5)%>%            
    #add_trace(data = df_pm, x=~DateTimeLocal, y=~PM10NC,    type = "scatter", mode  = "lines", line = list(color ="red"), name = "PM10 #/cm3", opacity=0.5)%>%            
    layout(xaxis = list(tickangle = -0), 
           margin=list(l = 100),
           yaxis = list(title="counts", tickprefix="   "),
           showlegend = T, 
           font = list(size=16))
  
  plot_particlesize <- plot_ly() %>%
    add_trace(data = df_pm, x=~DateTimeLocal, y=~PMtypicalParticleSize,   type = "scatter", mode  = "lines", line = list(color ="green"), name = "PM typical particle size (um)", opacity=0.8)%>%            
    layout(xaxis = list(tickangle = -0), 
           margin=list(l = 100),
           yaxis = list(title="particle\nsize", tickprefix="   "),
           showlegend = T, 
           font = list(size=16))
  
  plot_filterdp <- plot_ly() %>%
    add_trace(data = df_pm, x=~DateTimeLocal, y=~FilterDP,   type = "scatter", mode  = "lines", 
              line = list(color ="black"), name = "filter DP", opacity=0.8)%>%            
    layout(xaxis = list(tickangle = -0), 
           margin=list(l = 100),
           yaxis = list(title="filter DP", tickprefix="   "),
           showlegend = T, 
           font = list(size=16))
  
  plot_flow_power <- plot_ly() %>%
    add_trace(data = df, x=~DateTimeLocal, y=~PumpingFlowRate,   type = "scatter", mode  = "lines", line = list(color ="black"), name = "Flow rate (Lpm)", opacity=0.8)%>%            
    #add_trace(data = df, x=~DateTimeLocal, y=~MassFlow,   type = "scatter", mode  = "lines", line = list(color ="blue"), name = "Mass Flow (g/m)", opacity=0.5)%>%            
    #add_trace(data = df, x=~DateTimeLocal, y=~PumpV/10,   type = "scatter", mode  = "lines", line = list(color ="purple"), name = "Pump Voltage (V)/10", opacity=0.5)%>%            
    add_trace(data = df, x=~DateTimeLocal, y=~as.numeric(BatteryCharge)/100,   type = "scatter", mode  = "lines", line = list(color ="red"), name = "Battery (%)/100", opacity=0.8)%>%            
    layout(xaxis = list(tickangle = -0), 
           margin=list(l = 100),
           yaxis = list(title="Flow\n&\nBattery", tickprefix="   "),
           showlegend = T, 
           font = list(size=16))
  
  plot_temp_rh <- plot_ly() %>%
    add_trace(data = df, x=~DateTimeLocal, y=~AtmoT,    type = "scatter", mode  = "lines", line = list(color ="green"), name = "Atmosphere Temp (C)", opacity=0.8)%>%            
    add_trace(data = df, x=~DateTimeLocal, y=~AtmoRH,    type = "scatter", mode  = "lines", line = list(color ="blue"), name = "Atmosphere RH (%)", opacity=0.8)%>%            
    #add_trace(data = df, x=~DateTimeLocal, y=~AtmoDensity*10,   type = "scatter", mode  = "lines", line = list(color ="blue"), name = "10 * Air Density (g/L)", opacity=0.5)%>%            
    layout(xaxis = list(tickangle = -0), 
           margin=list(l = 100),
           yaxis = list(title="Temp\n&\nRH", tickprefix="   "),
           showlegend = T, 
           font = list(size=16))
  
  #plot_pressure <- plot_ly() %>%
  #  add_trace(data = df, x=~DateTimeLocal, y=~PCBP,    type = "scatter", mode  = "lines", line = list(color ="green"), name = "PCB pressure (hPa)", opacity=0.5)%>%            
  #  add_trace(data = df, x=~DateTimeLocal, y=~PumpP,   type = "scatter", mode  = "lines", line = list(color ="red"), name = "Pump pressure (hPa)", opacity=0.5)%>%            
  #  add_trace(data = df, x=~DateTimeLocal, y=~FdPdP,    type = "scatter", mode  = "lines", line = list(color ="black"), name = "differential pressure across filter (Pa)", opacity=0.5)%>%            
  #  add_trace(data = df, x=~DateTimeLocal, y=~Alt,   type = "scatter", mode  = "lines", line = list(color ="blue"), name = "Alt (m)", opacity=0.5)%>%            
  #  layout(xaxis = list(tickangle = -0), 
  #         margin=list(l = 100),
  #         yaxis = list(title="Pressure", tickprefix="   "),
  #         showlegend = T, 
  #         font = list(size=16))
  
  
  plot_light <- plot_ly() %>%
    add_trace(data = df, x=~DateTimeLocal, y=~LUX/1000,    type = "scatter", mode  = "lines", line = list(color ="black"), name = "LUX/1000", opacity=0.5)%>%            
    #add_trace(data = df, x=~DateTimeLocal, y=~UVindex,   type = "scatter", mode  = "lines", line = list(color ="purple"), name = "UV index", opacity=0.5)%>%            
    layout(xaxis = list(tickangle = -0), 
           margin=list(l = 100),
           yaxis = list(title="light", tickprefix="   "),
           showlegend = T, 
           font = list(size=16))
  
  #df$vector_sum_XYZ <- sqrt((df$AccelX)^2 + (df$AccelY)^2 + (df$AccelZ)^2)
  plot_activity <- plot_ly() %>%
    add_trace(data = df, x=~DateTimeLocal, y=~AccelX/1000,    type = "scatter", mode  = "lines", line = list(color ="red"), name = "Accel-X (g)", opacity=0.8)%>%            
    add_trace(data = df, x=~DateTimeLocal, y=~AccelY/1000,    type = "scatter", mode  = "lines", line = list(color ="darkgreen"), name = "Accel-Y (g)", opacity=0.8)%>%            
    add_trace(data = df, x=~DateTimeLocal, y=~AccelZ/1000,    type = "scatter", mode  = "lines", line = list(color ="blue"), name = "Accel-Z (g)", opacity=0.8)%>%            
    
    add_trace(data = df, x=~DateTimeLocal, y=~compliance,    type = "scatter", mode  = "lines", line = list(color ="black"), name = "compliance", opacity=0.8)%>%            
    add_trace(data = df, x=~DateTimeLocal, y=~compliance_rollmean,    type = "scatter", mode  = "lines", line = list(color ="purple"), name = "compliance rollmean", opacity=0.8)%>%            
    #add_trace(data = df, x=~DateTimeLocal, y=~vector_sum_XYZ/1000,   type = "scatter", mode  = "lines", line = list(color ="black"), name = "Vector sum XYZ (g)", opacity=0.8)%>%            
    add_trace(data = df, x=~DateTimeLocal, y=~AccelX_SD,    type = "scatter", mode  = "lines", line = list(color ="pink"), name = "Accel-X SD (g)", opacity=0.8)%>%            
    add_trace(data = df, x=~DateTimeLocal, y=~AccelY_SD,    type = "scatter", mode  = "lines", line = list(color ="lightgreen"), name = "Accel-Y SD (g)", opacity=0.8)%>%            
    add_trace(data = df, x=~DateTimeLocal, y=~AccelZ_SD,    type = "scatter", mode  = "lines", line = list(color ="lightblue"), name = "Accel-Z SD (g)", opacity=0.8)%>%            
    #add_trace(data = df, x=~DateTimeLocal, y=~StepCount/100,    type = "scatter", mode  = "lines", line = list(color ="purple", size = 2), name = "Step count/100", opacity=0.99)%>%            
    #add_trace(data = df, x=~DateTimeLocal, y=~GPSspeed,   type = "scatter", mode  = "lines", line = list(color ="gray"), name = "GPS speed (m/s)", opacity=0.5)%>%            
    layout(xaxis = list(tickangle = -0), 
           margin=list(l = 100),
           yaxis = list(title="Accele\nrometer", tickprefix="   "),
           showlegend = T, 
           font = list(size=16))
  
  plot_gps <- plot_ly() %>%
    add_trace(data = df, x=~DateTimeLocal, y=~lat, type = "scatter", mode  = "lines", name = "GPS latitude") %>%
    layout(xaxis = list(tickangle = -0), 
           margin=list(l = 100),
           yaxis = list(title="GPS", tickprefix="   "),
           showlegend = T, 
           font = list(size=16))
  
  title <- paste0("UPAS-", df$UPASserial[1], ", run hour = ", run_hours, ", wearing hour = ", 
                  wearing_hours, ", mean PM2.5 = ", mean_pm25, " ug/m3 ,", df[!is.na(df$unit_id), ]$unit_id[1])
  
  subplot(plot_pm,
          plot_particlecount,
          plot_particlesize,
          #plot_pressure,
          plot_filterdp,
          plot_temp_rh,
          plot_flow_power,
          plot_light,
          plot_activity,
          plot_gps,
          
          nrows = 9, 
          heights = c(0.2, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1), 
          #margin = c(0.04, 0.04, 0.04, 0.04), 
          shareX = T, 
          titleX = F, 
          titleY = T) %>% 
    layout(annotations = list(list(x = 0.5 , y = 1.02, text = title,
                                   font = list(size = 18, color = "black"), 
                                   showarrow = F, 
                                   xref='paper', yref='paper')),
           font = list(size=16)
    )
  
}


function_UPAS_plotly_all_parameters(list.upas[[5]])
test_upas_list8 <- list.upas[[8]]


for (i in 1:length(list.upas)) {
  html_filename <- gsub(".txt", ".html", file_upas[i])
  htmlwidgets::saveWidget(widget = function_UPAS_plotly_all_parameters(list.upas[[i]]), 
                          file = paste0("2022-07-14/", html_filename), 
                          selfcontained = T, libdir = "lib_upas/")
}







