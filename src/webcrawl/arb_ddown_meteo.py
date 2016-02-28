import re
from mechanize import Browser

import urllib2 
import requests
from BeautifulSoup import BeautifulSoup  

import BeautifulSoup
import os
from calendar import monthrange


Soup = BeautifulSoup
webpage = "http://www.arb.ca.gov/aqmis2/metselect.php"
 
allitems=['DEWPNT_F', 'DEWPNT_C', 'ETO', 'PRECIP', 'BPSTA', 'SL_PRESS', 'STAT_PRESS', 'VAPORP', 'NETRAD', 'SORAD', 'UVRAD', 'RELHUM', 'SIGTHETA', 'TEMP_F', 'TEMP_C', 'FTEMP_F', 'FTEMP_C', 'SEA_SFT_F', 'SEA_SFT_C', 'SOILT_F', 'SOILT_C', 'PKSPD_knots', 'PKSPD_mph', 'PKSPD_mps', 'WINSPD_knots', 'WINSPD_mph', 'WINSPD_mps', 'SWINSPD_knots', 'SWINSPD_mph', 'SWINSPD_mps'] 

taritems=["TEMP_C","WINSPD_mps","PRECIP", "RELHUM", "SIGTHETA"]

basic_path="/mnt/quickDB/meteodata/CARB_Met"
for year in range(2012,2014):
    ypath=basic_path+"/"+str(year)
    if not os.path.exists(ypath):
        os.makedirs(ypath)
    print "Extracting the meteorological data for "+str(year)+"... ..."
    errorfile=tarfl=ypath+"/errors.csv" 
    if year==2012: 
       taritems=["TEMP_C","WINSPD_mps","PRECIP", "RELHUM", "SIGTHETA"]
    for aitem in taritems:    
       isfirst=True 
       fl=aitem.lower() 
       tarfl=ypath+"/"+fl+".csv"
       print "               ---"+aitem 
       start_m=1 
       end_m=13
       if aitem=="TEMP_C" and year==2012 : 
          start_m=12
       for month in range(start_m,end_m):
          cc=monthrange(year, month) 
          end_d=cc[1]+1
          print "---------------------------------------------month:"+str(month)
          start_d=1 
          if aitem=="TEMP_C" and year==2012 : 
             start_d=13 
          for day in range(start_d,end_d): 
             print "                                            day:"+str(day)
             bry = Browser()
             bry.set_handle_robots(False)
             response=bry.open(webpage)
             bry.select_form("AQMIS")   
             bry["param"] = [aitem] 
             bry["latitude"] = ["A-Whole State"]
             bry["year"] = [str(year)] 
             bry["county_name"] = ["--COUNTY--"]
             bry["basin"] = ["--AIR BASIN--"]
             bry["latitude"] = ["A-Whole State"]
             bry["report"] = ["PICKDATA"]
             bry["order"] = ["state,basin,county_name,name"] 
             bry["network[]"]=["ALL"]
             bry["mon"] = [str(month)]
             bry["day"] = [str(day)]
             response = bry.submit()
             furl = response.geturl()
             brd = Browser()
             brd.set_handle_robots(False)
             response_1=brd.open(furl)   
             try:
                 brd.select_form("pickdl_hrly")
             except: 
                print "brd.select_form pickdl_hrly error!" 
                ferr = open(errorfile, 'a')
                ferr.write(str(month)+"-"+str(day)+"-"+str(year)+", "+aitem+","+furl+"\n")
                ferr.close() 
                continue 
             for control in brd.form.controls:
                 if control.type=='select' and control.name=='month' :
                       control.value=[str(month)] 
                 if control.type=='select' and control.name=='day' :
                       control.value=[str(day)]  
             brd["qselect"] = ["All"]
             brd["start_mon"] = [str(month)]
             brd["start_day"] = [str(day)]
             response_2=brd.submit(name="submit", label="All Sites")
             furl = response_2.geturl()
             brdf1=Browser()
             brdf1.set_handle_robots(False)
             response_3=brdf1.open(furl)
             try:
                brdf1.select_form("daypick") 
             except Exception as inst:
                print type(inst) 
                ferr = open(errorfile, 'a')
                ferr.write(str(month)+"-"+str(day)+"-"+str(year)+", "+aitem+","+furl+"\n")
                ferr.close() 
                continue 
             brdf1["filefmt"] = ["csv"]
             brdf1["datafmt"] = ["dvd"] # dvd values=['Basic Data Record'];  
             try:
                response_4=brdf1.submit(label="Get Data") 
             except Exception as inst:
                print type(inst) 
                ferr = open(errorfile, 'a')
                ferr.write(str(month)+"-"+str(day)+"-"+str(year)+", "+aitem+","+furl+"\n")
                ferr.close() 
                continue 
             dt_cnt = response_4.read()
             lines=dt_cnt.split('\n')   
             f = open(tarfl, 'a')
             pattern = re.compile("site,")
             for aline in lines:
                chk=pattern.search(aline)
                if aline.strip() =='' :
                   break
                if isfirst:
                   isfirst=False
                   f.write(aline)
                if chk != None:
                   continue
                f.write(aline)
             f.close() 



