
create language plpythonu ;

 
createlang plpythonu ca_birth   ;

CREATE OR REPLACE function removeRepeatedPoints(geox float[],geoy float[])
  RETURNS text[]  
AS $$
  import os
  import glob
  import csv 
  fields=[]
  opfile=open(csvpath, 'rb') 
  fieldsReader = csv.reader(opfile, delimiter=',', quotechar='|')
  fields_list=fieldsReader.next()
  lp=1 
  for col in fields_list:
    col1=col.lower() 
    col1=col1.lstrip(' ')
    if cmp(col1,'')==0:
      col1='dump_'+str(lp) 
      lp=lp+1 
    else: 
      col1=col1.replace(' ','_')
      col1=col1.replace('%','')
      col1=col1.replace('/','_')
      col1=col1.replace('\\','_')
      col1=col1.replace('.','_')
      col1=col1.replace('(','_')
      col1=col1.replace(')','')
      col1=col1.replace('-','_')
    if col1 in fields:
      col1=col1+'_re'
    fields.append(col1)
  opfile.close()
  return fields 
$$ LANGUAGE plpythonu ; 
--
--


CREATE OR REPLACE function readFields(csvpath text)
  RETURNS text[]  
AS $$
  import os
  import glob
  import csv 
  fields=[]
  opfile=open(csvpath, 'rb') 
  fieldsReader = csv.reader(opfile, delimiter=',', quotechar='|')
  fields_list=fieldsReader.next()
  lp=1 
  for col in fields_list:
    col1=col.lower() 
    col1=col1.lstrip(' ')
    if cmp(col1,'')==0:
      col1='dump_'+str(lp) 
      lp=lp+1 
    else: 
      col1=col1.replace(' ','_')
      col1=col1.replace('%','')
      col1=col1.replace('/','_')
      col1=col1.replace('\\','_')
      col1=col1.replace('.','_')
      col1=col1.replace('(','_')
      col1=col1.replace(')','')
      col1=col1.replace('-','_')
    if col1 in fields:
      col1=col1+'_re'
    fields.append(col1)
  opfile.close()
  return fields 
$$ LANGUAGE plpythonu ; 

select * from explode_array(readFields('E:/Pah_database/UCI/05172011.csv')) field ;  


CREATE OR REPLACE function readTxtFiles(path text)
  RETURNS text[]  
AS $$
  import os
  import glob
  files=[]
  
  for infile in glob.glob( os.path.join(path, '*.txt') ):
    (dirName, fileName)=os.path.split(infile)
    (fileBaseName, fileExtension)=os.path.splitext(fileName)
    files.append(fileBaseName.lower())
  return files
$$ LANGUAGE plpythonu ; 


CREATE OR REPLACE function readTxtFiles_Orgin(path text)
  RETURNS text[]  
AS $$
  import os
  import glob
  files=[]
  
  for infile in glob.glob( os.path.join(path, '*.txt') ):
    (dirName, fileName)=os.path.split(infile)
    (fileBaseName, fileExtension)=os.path.splitext(fileName)
    files.append(fileBaseName)
  return files
$$ LANGUAGE plpythonu ; 

create or replace function explode_array(in_array anyarray) returns setof anyelement as
$$
    select ($1)[s] from generate_series(1,array_upper($1, 1)) as s;
$$
language sql immutable  ;   
---  
drop table if exists wrongfils ; 
create table wrongfils as 
  select * from explode_array(readTxtFiles('/opt/dbbackup/caline_wrong_files')) file ;     

drop table if exists wrongcaline_id ; 
create table wrongcaline_id (fname character varying, gid integer) ;

----start the script
set @basicpath='/opt/dbbackup/caline_wrong_files';  
set @all_fls=select * from explode_array(readTxtFiles_Orgin('@basicpath')) file;     
set @sz=lines(@all_fls)  ;
set @i=0 ;
while @i<@sz 
begin 
  set @afile=@all_fls[@i]['file'];
  set @afile=@basicpath+'/'+@afile+'.txt';
  print @afile; 
  copy wrongcaline_id(fname) from '@afile' CSV  ; 
  set @i=@i+1 ; 
end 


select fname, unnest(regexp_matches(fname,'[0-9]{1,9}')) as id from  wrongcaline_id    ;

update wrongcaline_id  set gid=cast(unnest(regexp_matches(fname,'[0-9]{1,9}')) as integer)   ; 

drop table if exists wrongcaline_id2 ;
select distinct gid as pgid into wrongcaline_id2 from wrongcaline_id ;

create or replace function explode_array(in_array anyarray) returns setof anyelement as
$$
    select ($1)[s] from generate_series(1,array_upper($1, 1)) as s;
$$ 
; 




