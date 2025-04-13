## Read in the doppio grid
ncin = ncdf4::nc_open("doppio_grid.nc")

## Extract latitudes and longitudes
lat = ncdf4::ncvar_get(
  ncin,
  "lat_rho"
)
lon = ncdf4::ncvar_get(
  ncin,
  "lon_rho"
)

## Create a single column for each
points=data.frame(
  latitude=as.numeric(),
  longitude=as.numeric()
)
pb=txtProgressBar(char="*",style=3)
for(i in 1:length(lat)){
  x=data.frame(
    latitude=lat[i],
    longitude=lon[i]
  )
  points=rbind(points,x)
  rm(x)
  setTxtProgressBar(pb,i/length(lat))
}

## Make it a spatial object
sf.points=sf::st_as_sf(points, coords=c("longitude","latitude"))
sf::st_crs(sf.points)=4326
## Convert it to UTM 19S
sf.points=sf::st_transform(sf.points,32719)

## Create a grid using the doppio sf object as the area
sf.test=sf::st_make_grid(
  sf.points,
  cellsize=20000,
  what="polygons",
  square=TRUE
)

## Check to see which box an observation falls into
obs=emolt_download(days=365)
sf.obs=sf::st_as_sf(obs, coords=c("longitude..degrees_east.","latitude..degrees_north."))
sf::st_crs(sf.obs)=4326
sf.obs=sf::st_transform(sf.obs,32719)
sf.obs$cell=NA
for(i in 1:nrow(obs)){
  sf.obs$cell[i]=sf::st_intersects(sf.obs[i,],sf.test)[[1]]
}

## Download bathymetric data
bath=marmap::getNOAA.bathy(
  lon1=min(-80.83),
  lon2=max(-56.79),
  lat1=min(35.11),
  lat2=max(46.89),
  resolution=1,
)
sp.bath=marmap::as.raster(bath)
sp.bath=raster::projectRaster(sp.bath,crs=32719)
sp.bath=marmap::as.bathy(sp.bath)
## Create color ramp
blues=c(
  "lightsteelblue4", 
  "lightsteelblue3",
  "lightsteelblue2", 
  "lightsteelblue1"
)
## Plotting the bathymetry with different colors for land and sea
plot(
  sp.bath,
  step=100,
  deepest.isobath=-1000,
  shallowest.isobath=0,
  col="darkgray",
  image = TRUE, 
  land = TRUE, 
  lwd = 0.1,
  bpal = list(
    c(0, max(sp.bath,na.rm=TRUE), "gray"),
    c(min(sp.bath,na.rm=TRUE),0,blues)
  ),
  xlab="",
  ylab=""
)
for(i in 1:nrow(obs)){
  plot(sf.test[sf.obs$cell[i]],col='red',add=TRUE,border='darkred')
}
