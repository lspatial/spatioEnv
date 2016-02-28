library(raster) 
library(rgdal) 
library(RPostgreSQL)   
library(mgcv)   
library(plyr)   
library(reshape)     
library(foreach) 
library(doParallel)
library(maptools) 

basicpath="/mnt/datastore/EPA_data/ext_usc_subs/no2"  
img_bpath="/mnt/datastore/usc_path/bks/no2"

pol="n2"
exValue=100000 
year_f=1990 
year_t=2013 
#year_t=1992 

errorfl=paste(basicpath,"/",pol,"_error_v.txt",sep="")
imgfiles=data.frame(id=0,fl=NA,date=NA)
id_img=0 
for(iy in c(1:(year_t-year_f+1))){
  ayear=year_f+(iy-1)
  month_f=1
  month_t=12 
  for(amonth in c(month_f:month_t)){
    if(amonth<10){
       aimgfl=paste(pol,"_",as.character(ayear),"_0",as.character(amonth),"_v",sep="")
       adate=paste(as.character(ayear),"_0",as.character(amonth),sep="")
    }else{
       aimgfl=paste(pol,"_",as.character(ayear),"_",as.character(amonth),"_v",sep="")    
       adate=paste(as.character(ayear),"_",as.character(amonth),sep="")
    }
    id_img=id_img+1
    imgfiles[id_img,"id"]=id_img
    imgfiles[id_img,"fl"]=aimgfl
    imgfiles[id_img,"date"]=adate
  }
}

sub_locs=readOGR(dsn="PG:host=localhost user=postgres dbname=usc_ebk password=lfr524
port=8432", layer="usc_loc_unique")  

## get the project information; please ensure the consistent projects between images and the shape file of subject locations 
sub_locs_prj=projection(sub_locs, asText=TRUE)    

## set the number of cores in parallel computation  
par_cores=8  

## set the run times; for each time, there are par_cores used in parallel computations  2006-1990 
run_times=nrow(imgfiles)/par_cores    
if(run_times>floor(run_times)){
  run_times=floor(run_times)+1 
}


##test  
#for (i in c(17:24)){; print(i);atest=par_extract(sub_locs,img_bpath,500,errorfl,imgfiles,i) ;}
## parallel version of the function extracting the images for subject locations 
par_extract=function(sub_locs,img_bpath,exValue,errorfl,imgfiles,img_index){# img_index=1 #  
  aimgpath=paste(img_bpath,"/",imgfiles[img_index,"fl"],sep="") 
  #  aimgpath="/mnt/datastore/usc_path/bks/no2/n2_2000_06" 
  if (!file.exists(aimgpath)){
    aimgpath_org=aimgpath
    aimgpath=paste(aimgpath,"_1",sep="")
    if (!file.exists(aimgpath)){
        bname=basename(aimgpath_org)
        dname=dirname(aimgpath_org)
        gp=c(bname) 
        chfname=gsub("n2", "no2", gp)
        aimgpath=paste(dname,"/",chfname[1],sep="") 
        if (!file.exists(aimgpath)){
          errorinfo=paste(aimgpath," has no image file !!! ",sep="")  
          write(errorinfo,file=errorfl,append=TRUE)
          values=rep(NA,nrow(sub_locs))
          return(values) 
        }
    }
  }
  
  x=try(raster(aimgpath));
  
  if(class(x) == "try-error") {
    errorinfo=paste(aimgpath," has the error!!! ",sep="")  
    write(errorinfo,file=errorfl,append=TRUE)
    values=rep(NA,nrow(sub_locs))
    return(values)  
  }
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
print(as.character(run_times))
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
                 .packages=c('raster','rgdal')) %dopar% {
                     par_extract(sub_locs,img_bpath,exValue,errorfl,imgfiles,images)  # help(extract)
                 }    
   stopCluster(cl)
   proc.time()-ptm 
    # v_ext[1:100,]; combinedExValues[1:100,]
   for(k in c(1:length(images))){ 
       if( length(ncol(v_ext))==0 ){
          combinedExValues=cbind(combinedExValues,v_ext)
       }else{
          combinedExValues=cbind(combinedExValues,v_ext[,k])
       }
       colnames(combinedExValues)[low+k]=imgfiles[low+k-1,"date"]
    }     
    rm(list=c()) 
    gc() 
  }  
                 
combinedExValues2=round(combinedExValues,2)
output_file=paste(basicpath,"/",pol,"_sdv1_full.csv",sep="")
write.csv(combinedExValues2,file=output_file,row.names=FALSE,na="",append=FALSE)  #help(write.csv)


#find the error output and correct 
dates=colnames(combinedExValues) 
dates=dates[2:length(dates)]
nulldates=c()
for(i in c(1:length(dates))){
  adate=dates[i]
  isna_len=nrow(combinedExValues[is.na(combinedExValues[,adate]),])
  if(isna_len==nrow(combinedExValues)){
     print(paste(adate,": ",isna_len,sep=""))
     nulldates=c(nulldates,adate)
  }
}

nulldates

## 
dates=colnames(combinedExValues) 
dates=dates[2:length(dates)]
nullsubs=c()
for(i in c(1:nrow(combinedExValues))){ # i=1  
  asub=combinedExValues[i,"u_id"] 
  isna_len=length(which(is.na(combinedExValues[i,])))
  if(isna_len>=(2/3.0*length(dates))){
    print(paste(asub,": ",isna_len,sep=""))
    nullsubs=c(nullsubs,asub)
  }
}

length(nullsubs)  
nullsubs_file=paste(basicpath,"/",pol,"_nulld_sd_subs.csv",sep="")
write.csv(nullsubs,file=nullsubs_file,row.names=FALSE,na="",append=FALSE)
## check the maximum 

dates=colnames(combinedExValues) 
dates=dates[2:length(dates)]
subs=data.frame(u_id=NA,val_cnt=NA,min=NA,max=NA,mean=NA,sd=NA)
for(i in c(1:nrow(combinedExValues))){ # i=1  
  print(i)
  asub=combinedExValues[i,"u_id"] 
  adata=combinedExValues[i,2:length(dates)]
  rng=range(adata,na.rm=TRUE)
  val_cnt=length(which(!is.na(adata)))
  subs[i,"u_id"]=asub 
  subs[i,"val_cnt"]=val_cnt 
  subs[i,"min"]=rng[1] 
  subs[i,"max"]=rng[2]
  subs[i,"mean"]=mean(adata) 
  subs[i,"sd"]=sd(adata) 
}

aa=subs[subs$max<1,];  nrow(aa)  ;str(combinedExValues)
subs[1,];asub=combinedExValues[combinedExValues[,"u_id"]==1,c(2:length(dates))] 
rng=range(asub,na.rm=TRUE);rng[1]

min(subs$max,na.rm=TRUE)
max(subs$max,na.rm=TRUE)
max(subs$mean,na.rm=TRUE)

## check the time series 
range(combinedExValues[,"1990_01"],na.rm=T)
u_id=200
hist(combinedExValues[combinedExValues[,"u_id"]==u_id,])
aseries=combinedExValues[combinedExValues[,"u_id"]==u_id,2:ncol(combinedExValues)]
aseries
myts=ts(aseries, start=c(1990, 1), end=c(2013, 12), frequency=12) # help(ts) 
plot(myts)
myts2=window(myts, start=c(1990, 1), end=c(1991, 12)) 
plot(myts2)


# Extra test :
aimgpath="/mnt/datastore/usc_path/bks/no2/n2_2011_02" 
file.exists(aimgpath)

arst=raster(aimgpath)
xprj=projection(arst, asText=TRUE)  
v=getValues(arst) ##   v=c() ; str(v)
v[which(v<0)]=NA # set the abnormal values (<0 and >1) to sNA  
v[which(v>exValue)]=NA 
arst[]=v       
values=extract(arst,sub_locs)
range(values)

#compare the mean and standard variance: #help(read.csv)
mean0=read.csv("/mnt/datastore/EPA_data/ext_usc_subs/no2/n2_v1_full.csv",row.names=NULL,check.names=FALSE)
sd0=read.csv("/mnt/datastore/EPA_data/ext_usc_subs/no2/n2_sdv1_full.csv",row.names=NULL,check.names=FALSE)  
colnames(mean0)

write.csv(mean0,file="/home/samba/shared_data/komenprj/no2_kriging/no2_bk_monthly_mean.csv",row.names=FALSE,na="",append=FALSE) 
write.csv(sd0,file="/home/samba/shared_data/komenprj/no2_kriging/no2_bk_monthly_sd.csv",row.names=FALSE,na="",append=FALSE) 
###############
u_id




#find the missing output and correct 
dates=colnames(mean0) 
dates=dates[2:length(dates)]
nullsubs=c()
for(i in c(1:nrow(mean0))){ # i=1  
  asub=mean0[i,"u_id"] 
  isna_len=length(which(is.na(mean0[i,])))
  if(isna_len==length(dates)){  
    print(paste(asub,": ",isna_len,sep=""))
    nullsubs=c(nullsubs,asub)
  }
}

nullsub_num=length(nullsubs)  
nrow(mean0)-nullsub_num ; (nrow(mean0)-nullsub_num)/nrow(mean0)

nullsubs_file=paste(basicpath,"/",pol,"_nulld_sd_subs2.csv",sep="")
write.csv(nullsubs2,file=nullsubs_file,row.names=FALSE,na="",append=FALSE) 



difN=data.frame(nr=NA,nc=NA)
iex=0
for(i in c(1:nrow(mean0))){
  print(paste(as.character(i)," starting ... ...",sep=""))
  for(j in c(2:ncol(mean0))){
   if(is.na(mean0[i,j])){
     if(!is.na(sd0[i,j])){
       iex=iex+1
       difN[iex,"nr"]=i;difN[iex,"nc"]=j
     }
   }else{
     if(is.na(sd0[i,j])){
       iex=iex+1
       difN[iex,"nr"]=i;difN[iex,"nc"]=j
     }
   } 
    
  }
}
difN 



# install.packages("compare")
library(compare) # help(compare)
comparison=compare(mean0,sd0,allowAll=TRUE)
comparison$tM
###


dates=colnames(mean0) 
dates=dates[2:length(dates)]
subs=data.frame(u_id=NA,val_cnt=NA,min=NA,max=NA,mean=NA,sd=NA)
for(i in c(1:nrow(mean0))){ # i=20090 
  print(i)
  asub=mean0[i,"u_id"] 
  adata=mean0[i,2:length(mean0)]
  adata1=t(adata) 
  rng=range(adata1,na.rm=TRUE)
  val_cnt=length(which(!is.na(adata1)))
  # names(adata); t(adate)
  subs[i,"u_id"]=asub 
  subs[i,"val_cnt"]=val_cnt 
  subs[i,"min"]=rng[1] 
  subs[i,"max"]=rng[2]
  subs[i,"mean"]=mean(adata1,na.rm=TRUE) # help(mean); str(adate) 
  subs[i,"sd"]=sd(adata1) # subs[i,]
}

aa=subs[subs$max<1,];  nrow(aa)  ;str(mean0)
subs[1,];asub=mean0[mean0[,"u_id"]==1,c(2:length(dates))] 
rng=range(asub,na.rm=TRUE);rng[1]
boxplot(subs$max);hist(subs$max)
min(subs$max,na.rm=TRUE)
max(subs$max,na.rm=TRUE)
max(subs$mean,na.rm=TRUE); min(subs$mean,na.rm=TRUE);
#  hist(subs$mean)

#time series 

range(mean0[,"1990_01"],na.rm=T)
u_id=1001

values=t(mean0[mean0[,"u_id"]==u_id,2:ncol(mean0)])
hist(values)
aseries=values
aseries
myts=ts(aseries, start=c(1990, 1), end=c(2013, 12), frequency=12) # help(ts) 
plot(myts)
myts2=window(myts, start=c(1990, 1), end=c(1991, 12)) 
plot(myts2)


