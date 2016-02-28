#!/bin/bash 

dbname="usc_ebk"  
username="postgres"   
port=8432  
tartb=' ' 

/usr/local/pgsql/bin/./psql   -p $port   -d $dbname  << 'EOF'       
  
  CREATE OR REPLACE FUNCTION dropallfunct(_name text)
    RETURNS void AS
    $func$
    BEGIN
      EXECUTE (
      SELECT string_agg(format('DROP FUNCTION %s(%s);'
                     ,oid::regproc
                     ,pg_catalog.pg_get_function_identity_arguments(oid))
          ,E'\n')
      FROM   pg_proc
      WHERE  proname =lower(_name)
      AND    pg_function_is_visible(oid) );
   END
  $func$ LANGUAGE plpgsql  ;    

  select dropallfunct(' get_meteohourly') ; 
 
  CREATE OR REPLACE function removeInvalieTxtLines(txtpath text,tmptb text)
    RETURNS integer      
  AS $$
    import os
    import glob
    import io 
  
    (dirName, fileName)=os.path.split(txtpath)
    tmpfile=dirName+"/"+tmptb 
    if os.path.exists(tmpfile):
      os.remove(tmpfile)
    ins = open(txtpath, "r" ) 
    array = []
    for line in ins:
       array.append( line )  
    ins.close()
    wfile = open(tmpfile, "w")
    lns=len(array)-1 
    for i in range(3,lns):
       wfile.write(array[i])
    return 1   
  $$ LANGUAGE plpythonu ; 

  CREATE or replace FUNCTION get_meteohourly(_basicpath text,_tartb text, _year integer, _paras text[],_pfiles text[] ) 
  RETURNS boolean AS $$
  Declare
     apara text; 
     afile text ; 
     sql text; 
     f_dpath text;  
     msite_tb text; 

  begin    
     msite_tb :='msite_'||_year::text ;
     if NOT EXISTS (SELECT relname FROM pg_class WHERE relname=msite_tb) THEN
        sql :='CREATE TABLE IF NOT EXISTS '||msite_tb|| '(basin character varying, cnty_abbr character varying, name character varying, '||
              '  prelim_yn character varying,county_name character varying, site integer, state character varying, latitude double precision,'||
              '  longitude double precision, aqs_id character varying, qa_id character varying,district character varying,'||
              '  met_id character varying, elevation character varying, address character varying,city character varying, '||
              '  zip_code character varying, precip1990obs integer) ' ; 
        RAISE NOTICE ' get the site table ... ...' ;  
        execute sql; 
        execute 'copy '||msite_tb||' from '||quote_literal(_basicpath||'/sites_'||_year::text||'.csv')||' CSV HEADER NULL '||quote_literal(''); 
        execute 'drop table if exists tmp_sites'; 
        execute 'create table tmp_sites as select distinct on (site) * from  ' ||msite_tb;  
        execute 'drop table if exists '||msite_tb; 
        execute 'alter table tmp_sites rename to '||msite_tb; 
     END IF  ;

     execute 'drop table if exists '||_tartb ;  
     execute 'create table '||_tartb||' (site integer,date date,start_hour integer)' ;
    
     FOR i IN 1..array_length(_paras,1) LOOP
        apara :=_paras[i] ;
        afile :=_pfiles[i] ;
        RAISE NOTICE 'Getting the input files from text file %', apara;  
        execute 'drop table if exists hourly_tmp' ;   
        if apara <> 'wv' then 
           sql :='create table hourly_tmp (site integer,date date,start_hour integer, obs double precision, variable character varying, units integer, quality integer, prelim character varying, met_source character varying, obs_type character varying ,minutes integer) '; 
        else  
           sql :='create table hourly_tmp (site integer,date date,start_hour integer, ws double precision,wd double precision, variable character varying, units integer, quality_ws integer,quality_wd integer, prelim character varying, met_source character varying, obs_type character varying ,minutes integer) '; 
        END IF ;

        execute sql ; 
        f_dpath :=_basicpath||'/'||_year::text||'/'||afile;
        begin 
          sql :='copy  hourly_tmp from '||quote_literal(f_dpath)||' CSV HEADER  NULL '||quote_literal('') ;       
          execute sql ; 
        exception when others then  
          raise notice 'SQL error: %; %', SQLERRM, SQLSTATE;
          f_dpath :=_basicpath||'/'||_year::text||'/CommonVariables/'||afile||'.txt';
          sql :='copy  hourly_tmp from '||quote_literal(f_dpath)||' CSV HEADER NULL '||quote_literal('') ;       
          execute sql ; 
        end;

        if apara <> 'wv' then 
       	    execute 'alter table '||_tartb||' add column '||lower(apara)||' double precision '; 
            execute 'drop table if exists hourly_tmp0'; 
            execute 'create table hourly_tmp0 as select site, date, start_hour, obs from hourly_tmp where (quality_ws=0 or quality_ws=1) '||
                '      and (met_source similar to '||quote_literal('RAWS')||' or met_source similar to '||quote_literal('AIRS')||')' ;        
            execute 'drop index if exists hourly_tmp0_ind';
            execute 'create index hourly_tmp0_ind on hourly_tmp0 (site,date,start_hour)'; 
            execute 'update '||_tartb||'  a set '||lower(apara)||'=v.obs from hourly_tmp0 v where a.site=v.site and a.date=v.date and a.start_hour=v.start_hour '; 
            execute 'drop table if exists hourly_tmp1'; 
            execute 'create table hourly_tmp1 as select distinct on (a.*) a.* from hourly_tmp0 a left join '||_tartb||
                    ' b on a.site=b.site and a.date=b.date and a.start_hour=b.start_hour where b.site is null' ;   
            sql :='insert into '||_tartb||'(site, date,start_hour, '||lower(apara)||')'||
              '    select site, date, start_hour, obs from hourly_tmp1 ' ;
            execute sql ; 
        else  
            execute 'alter table '||_tartb||' add column ws double precision '; 
            execute 'alter table '||_tartb||' add column wd double precision ';
 
            execute 'drop table if exists hourly_tmp0'; 
            execute 'create table hourly_tmp0 as select site, date, start_hour, ws, wd from hourly_tmp where (quality=0 or quality=1) '||
                '      and (met_source similar to '||quote_literal('RAWS')||' or met_source similar to '||quote_literal('AIRS')||')' ;        
            execute 'drop index if exists hourly_tmp0_ind';
            execute 'create index hourly_tmp0_ind on hourly_tmp0 (site,date,start_hour)'; 
            execute 'update '||_tartb||'  a set ws=v.ws, wd=v.wd from hourly_tmp0 v where a.site=v.site and a.date=v.date and a.start_hour=v.start_hour '; 
            execute 'drop table if exists hourly_tmp1'; 
            execute 'create table hourly_tmp1 as select distinct on (a.*) a.* from hourly_tmp0 a left join '||_tartb||
                    ' b on a.site=b.site and a.date=b.date and a.start_hour=b.start_hour where b.site is null' ;   
            sql :='insert into '||_tartb||'(site, date,start_hour, ws,wd)'||
                  '   select site, date, start_hour, ws,wd from hourly_tmp1 ' ;
            execute sql ;                
        END IF ;

     END LOOP; 
     return true  ; 
  end 
  $$ language plpgsql ;
  
EOF
  

_basepath="/mnt/quickDB/meteodata/CARB_Met"  
years=(2011 2012 2013) 

for y in "${years[@]}"
do
  echo "Processing "$y"... ..."
  params="{temp,prep,rh,sigmatheta,wv}"
  parampath="{temp_c.csv,precip.csv,relhum.csv,sigtheta.csv,winspd_mps.csv}" 
  tartb="meteo"$y 
/usr/local/pgsql/bin/./psql   -p $port   -d $dbname  << EOF       
    select  get_meteohourly('$_basepath','$tartb',$y, '$params','$parampath') ; 
EOF
done


