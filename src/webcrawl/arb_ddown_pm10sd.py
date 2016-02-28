import re
from mechanize import Browser

import urllib2 
import requests
from BeautifulSoup import BeautifulSoup  

Soup = BeautifulSoup
webpage = "http://www.arb.ca.gov/aqmis2/aqdselect.php?tab=specialrpt"
br = Browser()
br.set_handle_robots(False)

import BeautifulSoup
import os
from calendar import monthrange

basic_path='/mnt/datastore/EPA_data/ARdown/dailypm10sd_2'
for year in range(1990,2014):
    
    tarfl=basic_path+'/pm10sd_'+str(year)+'.csv'
    tar_stfl=basic_path+'/pm10sd_'+str(year)+'_site.csv'
    if os.path.exists(tarfl): 
       os.remove(tarfl) 
    if os.path.exists(tar_stfl): 
       os.remove(tar_stfl)  
    stflexist=False 
    chkdfl=False 
    isfirst=True 
 
    response=br.open(webpage)
    br.select_form("AQMIS") 
    for form in br.forms():
       print 'form ', form.name,form.attrs.get('id') 
       for control in form.controls:
           print '     type=%s, name=%s, value=%s' %(control.type, control.name, control.value) 
    aa=["param"]
    for asel in aa: 
       print asel+"................................................"
       control = br.form.find_control(asel)
       if control.type == "select":  # means it's class ClientForm.SelectControl
          for item in control.items:
             print " name=%s values=%s" % (item.name, str([label.text  for label in item.get_labels()]))

    br["param"] = ["PM10_S"] # local daily ["PM10_L"]; daily: "PM10_S" 
    br["units"] = ["ppm"]
    br["year"] = [str(year)]
    br["county_name"] = ["--COUNTY--"]
    br["basin"] = ["--AIR BASIN--"]
    br["latitude"] = ["A-Whole State"]
    br["report"] = ["PICKDATA"]
    br["order"] = ["basin,county_name,s.name"]
    response = br.submit()
    furl = response.geturl()
    content = response.read() 
    soup = Soup(content)
    links = soup.findAll("a")

    for tag in links:
       link = tag.get("href",None)
       if link != None and tag.text=='Get Additional Information on Sites':
           sub_res_st=br.open(link)
           st_pg = sub_res_st.read()
           soup_st = Soup(st_pg)
           links_st = soup_st.findAll("a")
           for tag_st in links_st :
              link_st = tag_st.get("href",None)
              if link_st != None and tag_st.text=='Quick':
                 sub_res=br.open(link_st)
                 dt_cnt = sub_res.read()
                 lines=dt_cnt.split('\n')   
                 f = open(tar_stfl, 'a')
                 for aline in lines:
                    if aline.strip() =='':
                       break
                    f.write(aline)
                 f.close()

    response_1=br.open(furl)
    #for form in br.forms():
    #   print 'form ', form.name,form.attrs.get('id') 
    #   for control in form.controls:
    #       print '     type=%s, name=%s, value=%s' %(control.type, control.name, control.value) 

    br.select_form("pickdl_hrly")
     


    # aa=["qselect","start_mon","start_day","mon","day"]
    br["qselect"] = ["All"]
    br["start_mon"] = ["1"]
    br["start_day"] = ["1"]
    br["mon"] = ["12"]
    br["day"] = ["31"] 
    response3=br.submit(name="submit", label="All Sites")
    furl = response3.geturl()
    response_1=br.open(furl)
    br.select_form("daypick") 
    #aa=["filefmt","datafmt"]
    #for asel in aa: 
    #   print asel+"................................................"
    #   control = br.form.find_control(asel)
    #   if control.type == "select":  # means it's class ClientForm.SelectControl
    #      for item in control.items:
    #         print " name=%s values=%s" % (item.name, str([label.text  for label in item.get_labels()]))
      
    br["filefmt"] = ["csv"]
    br["datafmt"] = ["dvde"]
    response3=br.submit(label="Get Data") 
    dt = response3.read()
    lines=dt.split('\n')   
    f = open(tarfl, 'w')
    pattern = re.compile("site,")
    for aline in lines:
       #chk=pattern.search(aline)
       if aline.strip() =='' :
          break
       f.write(aline)
    f.close()





                              

