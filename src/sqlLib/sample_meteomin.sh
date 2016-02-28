#!/bin/sh 

dbname="sptem_db"  
username="postgres"     
port=8433 
tartb='spt_samples_com_uid' 

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

  select dropallfunct('addanyfld') ; 
  
 CREATE or replace FUNCTION addanyfld(_tartb text, _fldname text, _type text) 
  RETURNS boolean AS $ext$
  Declare
     chkfld integer :=0; 
  Begin  
     execute 'SELECT 1 FROM  pg_attribute  WHERE attrelid = '||quote_literal(_tartb)||'::regclass  AND    attname = '||
                quote_literal(_fldname)||' AND NOT attisdropped ' into  chkfld ;           
     if chkfld is null  then 
        execute 'alter table '||_tartb||' add column '||_fldname ||' '||_type ; 
        return true ;  
     end if ;
     return false  ; 
  END 
  $ext$ LANGUAGE plpgsql  ;   

  select dropallfunct('ext_minmeteoVar0') ; 

  CREATE or replace FUNCTION ext_minmeteoVar0(_tartb text,_missedtb text, _meteovar_tb text,_dlimit integer, aintv integer,   
             _idStr text,_meteo_type text) 
  RETURNS boolean AS $ext$
  Declare
     sz integer ;
     lpsz integer; 
     index integer;
     sql text;  
     lower integer ; 
     upper integer; 
     chkfld integer :=0; 
     tarfld text; 
  Begin 
    EXECUTE 'select  count(*) from '||_missedtb into sz;    
    if (_meteo_type is not distinct from 'tmp') then 
        execute 'select  addanyfld('||quote_literal(_tartb)||','||quote_literal('temp_mean')||','||quote_literal('double precision')||')' ;        
    ELSIF (_meteo_type is not distinct from 'ws') then 
        execute 'select  addanyfld('||quote_literal(_tartb)||','||quote_literal('wnd_mean')||','||quote_literal('double precision')||')' ;
        execute 'select  addanyfld('||quote_literal(_tartb)||','||quote_literal('wndsin_mean')||','||quote_literal('double precision')||')' ;
        execute 'select  addanyfld('||quote_literal(_tartb)||','||quote_literal('wndcos_mean')||','||quote_literal('double precision')||')' ;
    end if ; 
    lpsz :=(sz/aintv)::integer ; 
    index :=0; 
    RAISE NOTICE ' start ' ;
    While index<=lpsz 
    Loop 
      RAISE NOTICE 'execute roadmin % ... in %... ', index,lpsz ;
      lower :=index*aintv ; 
      upper :=(index+1)*aintv ; 
      RAISE NOTICE ' start from % to %',lower::text , upper::text ;
      if index=(lpsz-1) then 
        upper :=sz ; 
      end if ; 
      EXECUTE 'drop table if exists amissedtb cascade  ' ;  
      sql := ' create table  amissedtb as '|| 
             '   select * from '||_missedtb||' where u_id>'||lower::text||' and u_id<='||upper::text ; 
      RAISE NOTICE ' sql2 : % ... ... ',sql ; 
      EXECUTE sql ;        
      EXECUTE 'drop index if exists amissedtb_gindex ' ; 
      EXECUTE 'create index amissedtb_gindex  on amissedtb using GIST(geom)' ; 
       
      EXECUTE 'drop table if exists amissedtb_meteos cascade  ' ; 

       if (_meteo_type is not distinct from 'tmp') then 
         sql :='create table amissedtb_meteos as '||
             '    select distinct on (a.'||_idStr||') a.'||_idStr||', ST_Distance(b.geom,a.geom) as dist,b.avg as value  '||
             '            from amissedtb a inner join '||_meteovar_tb||' b on ST_Distance(b.geom,a.geom)<='||_dlimit|| 
             '            order by '||_idStr||', dist';     
         EXECUTE sql ;  
         sql := ' update  '||_tartb||' t set  temp_mean=v.value   '|| 
             '   from amissedtb_meteos v '||
	      '   where t.'||_idStr||'=v.'||_idStr||' and t.missed=1' ;      
         EXECUTE sql ; 
       ELSIF (_meteo_type is not distinct from 'ws') then 
         sql :='create table amissedtb_meteos as '||
             '    select distinct on (a.'||_idStr||') a.'||_idStr||', ST_Distance(b.the_geom,a.geom) as dist,b.wd_av,b.wd_avsin, b.wd_avcos  '||
             '            from amissedtb a inner join  '||_meteovar_tb||' b on ST_Distance(b.the_geom,a.geom)<='||_dlimit|| 
             '            order by '||_idStr||', dist';     
         EXECUTE sql ;  
         sql := ' update  '||_tartb||' t set wnd_mean=v.wd_av,wndsin_mean=v.wd_avsin,wndcos_mean=v.wd_avcos   '|| 
             '   from amissedtb_meteos v '||
	      '   where t.'||_idStr||'=v.'||_idStr||' and t.missed=1' ;      
         EXECUTE sql ; 
       end if ; 
      RAISE NOTICE ' sql3 : % ... ... ',sql  ; 
      index :=index+1; 
    end loop; 
    return true  ;  
  END 
  $ext$ LANGUAGE plpgsql  ;    
EOF

/usr/local/pgsql/bin/./psql   -p $port   -d $dbname  << EOF
  select ext_minmeteoVar0('$tartb','$tartb','mean_tmp_00_09',500000,500,'site_id','tmp');  
  select ext_minmeteoVar0('$tartb','$tartb','mean_wsd00_09_up1',500000,500,'site_id','ws');  

EOF
