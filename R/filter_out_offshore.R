data=subset(data,data$segment_type=="Fishing")
data$latitude=as.numeric(data$latitude)
data$longitude=as.numeric(data$longitude)
data$location=paste0(data$latitude,data$longitude)
## Specify lat/lon grids

library(dplyr)

## East of 69 and South of 44
newdat=data %>%
  filter(latitude < 44 & longitude > -69)
newdat$OnLand=FALSE

## 42-43 x 70-69
tempdat=data %>%
  filter(latitude < 43 & latitude > 42 & longitude < -69 & longitude > -70)
tempdat$OnLand=FALSE
newdat=rbind(newdat,tempdat)
rm(tempdat)

## East of 72 and South of 41
tempdat=data %>%
  filter(latitude < 41 & longitude > -72)
tempdat$OnLand=FALSE
newdat=rbind(newdat,tempdat)
rm(tempdat)

## East of 74 and South of 40
tempdat=data %>%
  filter(latitude < 40 & longitude > -74)
tempdat$OnLand=FALSE
newdat=rbind(newdat,tempdat)
rm(tempdat)

data=subset(data,data$location%in%newdat$location==FALSE)

write.csv(newdat,"offshore_eMOLT_data.csv")
rm(newdat)
