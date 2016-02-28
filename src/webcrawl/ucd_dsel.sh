#!/bin/bash

mygasfile='/home/sp/Data/UCD_data/codes/species_gas.csv' 
mypmfile='/home/sp/Data/UCD_data/codes/species_pm.csv' 

ids=()
spes=() 
function freadCSV()
{
  k=0 
  fst=0 
  ids=()
  spes=()  
  while read i
  do
    if ((fst==0)) ; then 
      fst=1 
      continue  
    fi
    ##eval $(echo $i|awk -F',' '{printf("id=%d\n sp=%s\n",$1,$2); }'  | tr -d \(\))
    eval $(echo $i|sed 's/[(\)]/\\&/g' |awk -F',' '{printf "id=%d\n sp=%s\n",$1,$2}')
    ids[$k]=$id 
    spes[$k]=$sp 
    k=$[$k + 1]
  done < $1
}  

freadCSV $mypmfile  
gasid=(${ids[*]})  
gas_spe=(${spes[*]})  
freadCSV $mygasfile 
pmid=(${ids[*]})    
pm_spe=(${spes[*]})   

echo ${gasid[*]} 
echo ${gas_spe[*]}  

echo ${pmid[*]} 
echo ${pm_spe[*]}  

echo ${pmid[1]}
echo ${pm_spe[1]}  
 






