#!/bin/sh 

dbname="usc_ebk"  
username="postgres"     
port=8432 
tartb='spt_samples_com_ext' 
tartb_missed='spt_samples_com_ext_missed32611' 
roadtb='ca_major_roads_2013_p'

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

  select dropallfunct('ext_mindist') ; 

  CREATE or replace FUNCTION ext_mindist(_tartb text,_missedtb text, _line_tb text,_dlimit integer, aintv integer,   
             _idStr text,_line_type integer, _linecol text,  _line_values anyarray, _tarfld text ) 
  RETURNS boolean AS $ext$
  Declare
     sz integer ;
     lpsz integer; 
     index integer;
     sql text;  
     lower integer ; 
     upper integer; 
     line_cat_sz integer;  
     il integer; 
  Begin 
    execute 'select  addanyfld('||quote_literal(_tartb)||','||quote_literal(_tarfld)||','||quote_literal('double precision')||')' ;  
    line_cat_sz :=array_length(_line_values, 1); 
    EXECUTE  'drop table if exists lines_tmp cascade ';
    il :=1;         
    if _line_type=0 then 
      sql :='create table lines_tmp as select * from '||_line_tb||' where '||_linecol||'='||_line_values[1]::text; 
      il :=il+1 ;   
      WHILE il<=line_cat_sz 
      LOOP 
        sql :=sql||' or '||_linecol|| '=' ||_line_values[il]::text; 
        il :=il+1;  
      END LOOP ; 
    elsif _line_type=1 then 
      sql :='create table lines_tmp as select * from '||_line_tb||' where lower('||_linecol||') like '||quote_literal(_line_values[1]); 
      il :=il+1 ;   
      WHILE il<=line_cat_sz 
      LOOP 
        sql :=sql||' or lower('||_linecol|| ') like ' ||quote_literal(_line_values[il]); 
        il :=il+1;  
      END LOOP ; 
    else   
    end if; 
    RAISE NOTICE ' SQL: % ... ... ', sql; 
    EXECUTE sql ; 
    EXECUTE 'drop index if exists lines_tmp_geoindex  ' ; 
    EXECUTE 'create index lines_tmp_geoindex  on  lines_tmp  using GIST(geom) ' ;
    EXECUTE 'select  count(*) from '||_missedtb into sz; 
    lpsz :=(sz/aintv)::integer ; 
    index :=0; 
    While index<=lpsz 
    Loop 
      RAISE NOTICE 'execute roadmin % ... in %... ', index,lpsz ;
      lower :=index*aintv ; 
      upper :=(index+1)*aintv ; 
      if index=(lpsz-1) then 
        upper :=sz ; 
      end if ; 
      EXECUTE 'drop table if exists amissedtb cascade  ' ;  
      sql := ' create table  amissedtb as '|| 
             '   select * from '||_missedtb||' where uuid>'||lower::text||' and uuid<='||upper::text ; 
      RAISE NOTICE ' sql2 : % ... ... ',sql ; 
      EXECUTE sql ;        
      EXECUTE 'drop index if exists amissedtb_gindex ' ; 
      EXECUTE 'create index amissedtb_gindex  on amissedtb using GIST(geom)' ; 
       
       EXECUTE 'drop table if exists amissedtb_values cascade  ' ; 
       sql :='create table amissedtb_values as '||
             '  select distinct on (a.'||_idStr||') a.'||_idStr||', ST_Distance(b.geom,a.geom) as dist  '||
             '            from amissedtb a inner join lines_tmp b on ST_Distance(b.geom,a.geom)<='||_dlimit|| 
             '            order by '||_idStr||', dist';     
       EXECUTE sql ;  
      sql := ' update  '||_tartb||' t set '||_tarfld||'=v.dist  '|| 
             '   from amissedtb_values v '||
	      '   where t.'||_idStr||'=v.'||_idStr ;      
      EXECUTE sql ; 
      RAISE NOTICE ' sql3 : % ... ... ',sql  ; 
      index :=index+1; 
    end loop; 
    return true  ;  
  END 
  $ext$ LANGUAGE plpgsql  ;    

  select dropallfunct('ext_roadlen') ; 

  CREATE or replace FUNCTION ext_roadlen(_tartb text,_missedtb text, _line_tb text,  _bdist integer, aintv integer,   
             _idStr text,_line_type integer, _linecol text,  _line_values anyarray, _tarfld text ) 
  RETURNS boolean AS $ext$
  Declare
     sz integer ;
     lpsz integer; 
     index integer;
     sql text;  
     lower integer ; 
     upper integer; 
     line_cat_sz integer;  
     il integer; 
  Begin 
    execute 'select  addanyfld('||quote_literal(_tartb)||','||quote_literal(_tarfld)||','||quote_literal('double precision')||')' ;  
    line_cat_sz :=array_length(_line_values, 1); 
    EXECUTE  'drop table if exists lines_tmp cascade ';
    il :=1;         
    if _line_type=0 then 
      sql :='create table lines_tmp as select * from '||_line_tb||' where '||_linecol||'='||_line_values[1]::text; 
      il :=il+1 ;   
      WHILE il<=line_cat_sz 
      LOOP 
        sql :=sql||' or '||_linecol|| '=' ||_line_values[il]::text; 
        il :=il+1;  
      END LOOP ; 
    elsif _line_type=1 then 
      sql :='create table lines_tmp as select * from '||_line_tb||' where lower('||_linecol||') like '||quote_literal(_line_values[1]); 
      il :=il+1 ;   
      WHILE il<=line_cat_sz 
      LOOP 
        sql :=sql||' or lower('||_linecol|| ') like ' ||quote_literal(_line_values[il]); 
        il :=il+1;  
      END LOOP ; 
    else   
    end if; 
    RAISE NOTICE ' SQL: % ... ... ', sql; 
    EXECUTE sql ; 
    EXECUTE 'drop index if exists lines_tmp_geoindex  ' ; 
    EXECUTE 'create index lines_tmp_geoindex  on  lines_tmp  using GIST(geom) ' ;
    EXECUTE 'select  count(*) from '||_missedtb into sz; 
    lpsz :=(sz/aintv)::integer ; 
    index :=0; 
    While index<=lpsz 
    Loop 
      RAISE NOTICE 'execute % ... in %... ', index,lpsz ;
      lower :=index*aintv ; 
      upper :=(index+1)*aintv ; 
      if index=(lpsz-1) then 
        upper :=sz ; 
      end if ; 
      EXECUTE 'drop table if exists amissedtb cascade  ' ;  
      sql := ' create table  amissedtb as '|| 
             '   select *, (ST_Dump(ST_Buffer(geom,'||_bdist::text||'))).geom as bgeom from '||
             '      (select * from '||_missedtb||' where uuid>'||lower::text||' and uuid<='||upper::text||')a ' ; 
      RAISE NOTICE ' sql2 : % ... ... ',sql ; 
      EXECUTE sql ;        
      EXECUTE 'drop index if exists amissedtb_gindex ' ; 
      EXECUTE 'create index amissedtb_gindex  on amissedtb using GIST(bgeom)' ; 
      sql := ' update  '||_tartb||' t set '||_tarfld||'=v.len '|| 
             '   from (select b.'||_idStr||', sum(ST_Length(ST_Intersection(b.bgeom,a.geom))) as len ' ||
             '          from   amissedtb  b inner join lines_tmp a on ST_Intersects(b.bgeom,a.geom) group by b.'||_idStr||', b.bgeom'||
             '         )v '|| 
	      '   where t.'||_idStr||'=v.'||_idStr ;      
       EXECUTE sql ; 
       sql := ' update  '||_tartb||'  set '||_tarfld||'=0 where '||_tarfld||' is null'; 
       EXECUTE sql ; 
      RAISE NOTICE ' sql3 : % ... ... ',sql  ; 
      index :=index+1; 
    end loop; 
    return true  ;  
  END 
  $ext$ LANGUAGE plpgsql ; 

 select dropallfunct('ext_aadtall') ; 
  CREATE or replace FUNCTION ext_aadtall(_tartb text,_missedtb text, _aadt_tb text,  _bdist integer, aintv integer,_idStr text, _tarfld text ) 
  RETURNS boolean AS $ext$
  Declare
     sz integer ;
     lpsz integer; 
     index integer;
     sql text;  
     lower integer ; 
     upper integer; 
  Begin 
    execute 'select  addanyfld('||quote_literal(_tartb)||','||quote_literal(_tarfld)||','||quote_literal('double precision')||')' ;  
    EXECUTE 'drop index if exists aadt_tmp_geoindex  ' ; 
    EXECUTE 'create index aadt_tmp_geoindex  on '||_aadt_tb||'  using GIST(geom) ' ;
    EXECUTE 'select  count(*) from '||_missedtb into sz; 
    lpsz :=(sz/aintv)::integer ; 
    index :=0; 
    While index<= lpsz 
    Loop 
      RAISE NOTICE 'execute % ... in %... ', index,lpsz ;
      lower :=index*aintv ; 
      upper :=(index+1)*aintv ; 
      if index=(lpsz-1) then 
        upper :=sz ; 
      end if ; 
      EXECUTE 'drop table if exists amissedtb cascade  ' ;  
      sql := ' create table  amissedtb as '|| 
             '   select *, (ST_Dump(ST_Buffer(geom,'||_bdist::text||'))).geom as bgeom from '||_missedtb||' where uuid>'||lower::text||' and uuid<='||upper::text ; 
      RAISE NOTICE ' sql2 : % ... ... ',sql ; 
      EXECUTE sql ;        
      EXECUTE 'drop index if exists amissedtb_gindex ' ; 
      EXECUTE 'create index amissedtb_gindex  on amissedtb using GIST(bgeom)' ; 
       
      EXECUTE 'drop table if exists amissedtb_values cascade  ' ; 
      sql :='create table amissedtb_values as '||
               '  select b.'||_idStr||', sum(l.aadt*ST_Length(ST_Intersection(b.bgeom, l.geom))) as numerator, '|| 
             '         sum(ST_Length(ST_Intersection(b.bgeom, l.geom))) as denominator '||
             '     from  amissedtb  b,'||_aadt_tb||' l where ST_Intersects(b.bgeom, l.geom)'||
	      '     group by b.'||_idStr||', b.bgeom' ; 
       EXECUTE sql; 

      sql :='update  '||_tartb||' t set '||_tarfld||'=(case when v.denominator=0 then 0 else v.numerator/v.denominator end) '|| 
              '  from amissedtb_values  v '|| 
	       '  where t.'||_idStr||'=v.'||_idStr ;      
      EXECUTE sql ; 
      sql :='update  '||_tartb||' set '||_tarfld||'=0 where '||_tarfld||' is null'; 
      EXECUTE sql ; 
      RAISE NOTICE ' sql3 : % ... ... ',sql  ; 
      index :=index+1; 
    end loop; 
    sql :='update '||_tartb||'  set  '||_tarfld||'=0 where '||_tarfld||' is null ';
    execute sql ; 
    return true  ;  
  END 
  $ext$ LANGUAGE plpgsql ; 
EOF

/usr/local/pgsql/bin/./psql   -p $port   -d $dbname  << EOF       

  select ext_mindist('$tartb','$tartb_missed','$roadtb',500000,5000,'site_id',0,'frc', ARRAY[0],'frc_0');  
  select ext_mindist('$tartb','$tartb_missed','$roadtb',500000,5000,'site_id',0,'frc', ARRAY[1],'frc_1');  
  select ext_mindist('$tartb','$tartb_missed','$roadtb',500000,5000,'site_id',0,'frc', ARRAY[2],'frc_2');  
  select ext_mindist('$tartb','$tartb_missed','$roadtb',500000,5000,'site_id',0,'frc', ARRAY[3],'frc_3');  
  select ext_mindist('$tartb','$tartb_missed','$roadtb',500000,5000,'site_id',0,'frc', ARRAY[4],'frc_4');  
  select ext_mindist('$tartb','$tartb_missed','$roadtb',500000,5000,'site_id',0,'frc', ARRAY[5],'frc_5');  

  select ext_roadlen('$tartb','$tartb_missed','$roadtb',5000,100,'site_id',0,'frc', ARRAY[0,1],'roadfr01_len_b5000');
  select ext_roadlen('$tartb','$tartb_missed','$roadtb',5000,5000,'site_id',0,'frc', ARRAY[1,2],'roadfr12_len_b5000');
  select ext_roadlen('$tartb','$tartb_missed','$roadtb',5000,5000,'site_id',0,'frc', ARRAY[0,1,2,3,4,5],'roadfrall_len_b5000');

  select ext_aadtall('$tartb','$tartb_missed','aadt05',5000,1000,'site_id','waadt_b5000') ;

EOF
