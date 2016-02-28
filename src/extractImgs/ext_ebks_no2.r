#install.packages("plyr")
#install.packages("reshape")
## install.packages("gregmisc") unnecessary to install 
#install.packages("foreach")
#install.packages("doParallel")
#install.packages("rgdal")
#install.packages("raster")
#install.packages("maptools")
#install.packages("RPostgreSQL")

library(raster) 
library(rgdal) 
library(RPostgreSQL)   
library(mgcv)   
library(plyr)   
library(reshape)     
## library(gregmisc) unnecessary to install 
library(foreach) 
library(doParallel)
library(maptools) 

basicpath="/mnt/datastore/EPA_data/ext_usc_subs"  

msublayer=readOGR(dsn="PG:host=localhost user=postgres dbname=usc_ebk password=lfr524
port=8432", layer="usc_loc_bk0")  

## get the project information; please ensure the consistent projects between images and the shape file of subject locations 
sub_locs_prj=projection(msublayer, asText=TRUE)    

## set the number of cores in parallel computation  
par_cores=10  

## set the run times; for each time, there are par_cores used in parallel computations  
run_times=nrow(imgfiles)/par_cores    
if(run_times>floor(run_times)){
  run_times=floor(run_times)+1 
}

## parallel version of the function extracting the images for subject locations 
par_extract=function(sub_locs,basic_path,pol,year,exValue){# img_index=1 #  
  aimgpath=paste(basic_path,"\",pol,"_",year,sep="") 
                 x=raster(aimgpath) 
                 xprj=projection(x, asText=TRUE)  
                 v=getValues(x) ##   v=c() ; str(v)
                 v[which(v<0)]=NA # set the abnormal values (<0 and >1) to NA  
                 v[which(v>exValue)]=NA 
                 x[]=v       
                 values=extract(x,sub_locs)   ## serial version of extract function in the raster package   
                 rm(list=c("x","v")) 
                 gc()   
                 return(values)  
}
                 
                 ## major parallel executive functions  
                 ## please make sure your unique id of your subject locations matches the following field. 
                 ## if not, please change TOKEN to your intended field of unique id in the following command
                 combinedExValues=cbind(u_id=sub_locs$u_id)
                 for(i in c(1:run_times)){  # i=1 
                 cores_num=par_cores 
                 low=(i-1)*par_cores+1 
                 high=i*par_cores 
                 images=c(low:high) # 
                 if(i==run_times){
                 cores_num=nrow(imgfiles)-par_cores*(run_times-1) 
                 high=nrow(imgfiles) 
                 images=c(low:high)  
                 }
                 print(paste(i,", ",cores_num," cores_num... ... ",sep=""))  
                 cl=makeCluster(cores_num)
                 registerDoParallel(cl) 
                 ptm=proc.time()  
                 v_ext=foreach(images=images, 
                 .combine='cbind',
                 .packages=c('raster','rgdal')) %dopar%{
                 par_extract(sub_locs,imgfiles,images)  # help(extract)
                 }
                 stopCluster(cl)
                 proc.time()-ptm 
                 
                 for(k in c(1:length(images))){ 
                 if( length(ncol(v_ext))==0 ){
                 combinedExValues=cbind(combinedExValues,v_ext)
                 }else{
                 combinedExValues=cbind(combinedExValues,v_ext[,k])
                 }
                 colnames(combinedExValues)[low+k]=paste("tm_",as.character(imgfiles[low+k-1,"time"]),sep="")
                 }     
                 rm(list=c()) 
                 gc() 
                 }  
                 
                 ## set the output path 
                 ## format: gid (unique_id of subject location), NDVI for time 1, NDVI for time 2, ... ...
                 ## column name indicates the time 
                 output_file=paste(basicpath,"/values_ser.csv",sep="")
                 write.csv(combinedExValues,file=output_file,row.names=FALSE)  
                 
                 