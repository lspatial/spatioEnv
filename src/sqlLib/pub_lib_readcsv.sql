
drop function if exists tb_post1(atb text)     ; 


select tb_post1('ca_pah_2011_13_apuf','original_name',ARRAY['SCAQMD','S','V','G'],ARRAY['S','W'],ARRAY['']) ;  

select tb_post1('ca_pah_2011_13_apuf','decriptions',ARRAY['SCAQMD','S','V','G','P'],ARRAY['SU','W'],ARRAY['WK']) ; 


select tb_post1('all_pass_tt','all_pass_tt',ARRAY['SCAQMD','S','V','G'],ARRAY['SU','W'],ARRAY['WK']) ;  

select select tb_post1('ca_pah_2011_13_apuf','decriptions',ARRAY['SCAQMD','S','V','G','P'],ARRAY['SU','W'],ARRAY['WK']) ;   

select * from pah_apbo_2days_passive_tb1 t inner join  
  ( select *, (regexp_matches(id,'(^S[0-9]{1,5}|^SCAQMD[0-9]{1,5}|^V[0-9]{1,5})'))[1] as stationid from   monitors_noxno2 )v 
  on t.stationid=v.stationid   ; 

update pah_apbo_2days_passive_tb1 t 
  set geom=v.geom  
  from (select *, (regexp_matches(id,'(^S[0-9]{1,5}|^SCAQMD[0-9]{1,5}|^V[0-9]{1,5})'))[1] as stationid 
          from   monitors_noxno2 )v  
  where t.stationid=v.stationid ; 

select * from pah_apbo_2days_passive_tb1 where geom is null ; 


Set @ss=select getDivStt('pah_apbo_2days_passive_tb1','pah_apbo_2days_passive',ARRAY['nap','acen','ace','fln','phe','an','fl','py'],
                ARRAY['_S1','_S2'],ARRAY['_W1','_W2'])  ;
print @ss; 

select getDivStt('pah_apbo_puf_tb1','pah_apbo_puf',ARRAY['nap','acen','ace','fln','phe','an','fl','py'],
                ARRAY['_S1','_S2'],ARRAY['_W1','_W2'])
                
select getDivStt('pah_apbo_filter_tb1','pah_apbo_filter',ARRAY['nap','acen','ace','fln','phe','an','fl','py'],
                ARRAY['SU11'],ARRAY['W11'])


select getDivStt2('ca_pah_2011_13_afr_tb1','ca_pah_2011_13_afr','weekid',ARRAY['napthalene','acenaphthylene','acenapthene','fluorene',
                 'phenanthrene','anthracene','fluoranthene','pyrene'])  ; 
select getDivStt2('ca_pah_2011_13_psr_tb1','ca_pah_2011_13_psr','weekid',ARRAY['napthalene','acenaphthylene','acenapthene','fluorene',
                 'phenanthrene','anthracene','fluoranthene','pyrene'])  ; 
select getDivStt2('ca_pah_2011_13_apuf_tb1','ca_pah_2011_13_apuf','weekid',ARRAY['napthalene','acenaphthylene','acenapthene','fluorene',
                 'phenanthrene','anthracene','fluoranthene','pyrene'])  ; 
                 
select dropallfunct('getDivStt')  ;

create or replace function getDivStt(tartb text,srctb text,  _comStr text[] default '{}', _Sn text[] default '{}', _Wn text[] default '{}') 
returns boolean as $$ 
declare 
  i integer:=0; 
  astr text ;
  rev boolean:=true;
  sqlstr text:=''; 
  substr text:=''; 
begin 
  IF _comStr <> '{}'::text[] THEN 
    i:=1; 
    FOREACH astr IN ARRAY _comStr
      LOOP
        if i=1 then 
          substr:=quote_ident(astr);
        else 
          substr:=substr||'+'||quote_ident(astr);
        end if;   
        i:=i+1; 
      END LOOP;
    RAISE NOTICE ' % ... ... ', substr;  
  else     
    rev :=false;
  end if ;

  execute 'drop table if exists '||quote_ident(tartb); 
  sqlstr='create table '||quote_ident(tartb)||' as select distinct stationid from '||quote_ident(srctb)||' order by stationid'; 
  execute sqlstr ; 
  
  IF _Sn <> '{}'::text[] THEN 
    i:=1; 
    FOREACH astr IN ARRAY _Sn
      LOOP
        execute 'alter table '||quote_ident(tartb)||' drop column if exists s'||i::text ; 
        execute 'alter table '||quote_ident(tartb)||' add column s'||i::text||' double precision' ; 
        sqlstr:='update '||quote_ident(tartb)||' t set s'||i::text||'='||substr||
                '  from '||quote_ident(srctb)||' v where t.stationid=v.stationid and v.season='||quote_literal(astr); 
        execute   sqlstr ;       
        i:=i+1; 
      END LOOP;
      RAISE NOTICE ' % ... ... ', sqlstr;  
  end if ;

  IF _Wn <> '{}'::text[] THEN 
    i:=1; 
    FOREACH astr IN ARRAY _Wn
      LOOP
        execute 'alter table '||quote_ident(tartb)||' drop column if exists w'||i::text ; 
        execute 'alter table '||quote_ident(tartb)||' add column w'||i::text||' double precision' ; 
        sqlstr:='update '||quote_ident(tartb)||' t set w'||i::text||'='||substr||
                '  from '||quote_ident(srctb)||' v where t.stationid=v.stationid and v.season='||quote_literal(astr); 
        execute   sqlstr ;       
        i:=i+1; 
      END LOOP;
      RAISE NOTICE ' % ... ... ', sqlstr;  
  end if ;

  execute 'select AddGeometryColumn('||quote_literal(tartb)||','||quote_literal('geom')||',32611,'||quote_literal('Point')||',2)'   ;  
  sqlstr='update '||quote_ident(tartb)||' t set geom=v.geom'|| 
         '  from (select *, (regexp_matches(id,'||quote_literal('(^S[0-9]{1,5}|^SCAQMD[0-9]{1,5}|^V[0-9]{1,5})')||'))[1] as stationid from   monitors_noxno2 )v '||
         '  where t.stationid=v.stationid ' ;
  RAISE NOTICE ' % ... ... ', sqlstr; 
  execute sqlstr  ;
  execute 'create index '||quote_ident(tartb)||'_spindex on '||quote_ident(tartb)||' using GIST(geom)'   ;   
  return rev; 
end;
$$ 
language plpgsql ;  

--xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
select distinct weekid from ca_pah_2011_13_afr  where weekid is not null order by   weekid ; 


create or replace function getDivStt2(tartb text,srctb text, weekcol text, _comStr text[] default '{}') 
returns boolean as $$ 
declare 
  i integer:=0; 
  astr text ;
  rev boolean:=true;
  sqlstr text:=''; 
  substr text:=''; 
  wks text[]; 
begin 
  IF _comStr <> '{}'::text[] THEN 
    i:=1; 
    FOREACH astr IN ARRAY _comStr
      LOOP
        if i=1 then 
          substr:=quote_ident(astr);
        else 
          substr:=substr||'+'||quote_ident(astr);
        end if;   
        i:=i+1; 
      END LOOP;
    RAISE NOTICE ' % ... ... ', substr;  
  else     
    rev :=false;
  end if ;

  execute 'drop table if exists '||quote_ident(tartb); 
  sqlstr='create table '||quote_ident(tartb)||' as select distinct stationid from '||quote_ident(srctb)||' order by stationid'; 
  execute sqlstr ; 
  
  EXECUTE 'SELECT ARRAY(SELECT distinct '||quote_ident(weekcol)||' as wkd FROM '||quote_ident(srctb)||' where '||quote_ident(weekcol)||' is not null order by wkd )' INTO wks; 
  
  IF wks <> '{}'::text[] THEN 
    FOREACH astr IN ARRAY wks
      LOOP
        execute 'alter table '||quote_ident(tartb)||' drop column if exists '||quote_ident(astr) ; 
        execute 'alter table '||quote_ident(tartb)||' add column '||quote_ident(astr)||' double precision' ; 
        sqlstr:='update '||quote_ident(tartb)||' t set '||quote_ident(astr)||'='||substr||
                '  from '||quote_ident(srctb)||' v where t.stationid=v.stationid and v.'||quote_ident(weekcol)||'='||quote_literal(astr); 
        execute   sqlstr ;  
      END LOOP;
      RAISE NOTICE ' % ... ... ', sqlstr;  
  end if ;

  execute 'select AddGeometryColumn('||quote_literal(tartb)||','||quote_literal('geom')||',32611,'||quote_literal('Point')||',2)'   ;  
  sqlstr='update '||quote_ident(tartb)||' t set geom=v.geom'|| 
         '  from (select *, (regexp_matches(id,'||quote_literal('(^S[0-9]{1,5}|^SCAQMD[0-9]{1,5}|^V[0-9]{1,5})')||'))[1] as stationid from   monitors_noxno2 )v '||
         '  where t.stationid=v.stationid ' ;
  RAISE NOTICE ' % ... ... ', sqlstr; 
  execute sqlstr  ;
  execute 'create index '||quote_ident(tartb)||'_spindex on '||quote_ident(tartb)||' using GIST(geom)'   ;   
  return rev; 
end;
$$ 
language plpgsql ;  


--xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx


create or replace function tb_post1(atb text,fldname text, _smpset anyarray,_snset anyarray,_wkset anyarray)  
returns boolean as $body$
declare
  sqlstr text; 
  astr text;
  len integer;   
  index  integer :=1; 
  cnt integer :=-1 ;  
begin 
  len :=array_length(_smpset, 1);  
  if len>0 then 
      EXECUTE  'alter table '||quote_ident(atb)||' drop column if exists stationid' ; 
      EXECUTE  'alter table '||quote_ident(atb)||' add column stationid character varying' ;
      index :=1; 
      astr='(^'||_smpset[index]||'[0-9]{1,100}';
      index :=index+1; 
      WHILE index<=len 
      LOOP 
        astr :=astr||'|^'||_smpset[index]||'[0-9]{1,100}'; 
        index :=index+1;  
      END LOOP ; 
      astr=astr||')'; 
      sqlstr='update '||quote_ident(atb)||' t set stationid=v.val from '||
             '  (select '||quote_ident(fldname)||',(regexp_matches('||quote_ident(fldname)||','||quote_literal(astr)||'))[1] as val from '||
             '     '||quote_ident(atb)||')v where t.'||quote_ident(fldname)||'=v.'||quote_ident(fldname)  ;   
      RAISE NOTICE ' % ... ... ', sqlstr;          
      EXECUTE sqlstr ;
  end if; 

  len :=array_length(_snset, 1);  
  if len>0 then 
    EXECUTE  'alter table '||quote_ident(atb)||' drop column if exists season' ; 
    EXECUTE  'alter table '||quote_ident(atb)||' add column season character varying' ;
    index :=1; 
    astr='(_'||_snset[index]||'[0-9]{1,2}';
    index :=index+1; 
    WHILE index<=len 
    LOOP 
      astr :=astr||'|_'||_snset[index]||'[0-9]{1,2}'; 
      index :=index+1;  
    END LOOP ; 
    astr=astr||')'; 
    sqlstr='update '||quote_ident(atb)||' t set season=v.val from '||
         '  (select '||quote_ident(fldname)||',(regexp_matches('||quote_ident(fldname)||','||quote_literal(astr)||'))[1] as val from '||
         '     '||quote_ident(atb)||')v where t.'||quote_ident(fldname)||'=v.'||quote_ident(fldname)  ;   
    RAISE NOTICE ' % ... ... ', sqlstr;          
    EXECUTE sqlstr ;
  end if ; 

  len :=array_length(_wkset, 1);  
  if len>0 then 
    EXECUTE  'alter table '||quote_ident(atb)||' drop column if exists weekid' ; 
    EXECUTE  'alter table '||quote_ident(atb)||' add column weekid character varying' ;
    index :=1; 
    astr='(_'||_wkset[index]||'[0-9]{1,100}';
    index :=index+1; 
    WHILE index<=len 
    LOOP 
      astr :=astr||'|_'||_wkset[index]||'[0-9]{1,100}'; 
      index :=index+1;  
    END LOOP ; 
    astr=astr||')'; 
    sqlstr='update '||quote_ident(atb)||' t set weekid=v.val from '||
         '  (select '||quote_ident(fldname)||',(regexp_matches('||quote_ident(fldname)||','||quote_literal(astr)||'))[1] as val from '||
         '     '||quote_ident(atb)||')v where t.'||quote_ident(fldname)||'=v.'||quote_ident(fldname) ;   
    RAISE NOTICE ' % ... ... ', sqlstr;          
    EXECUTE sqlstr ;
  end if ;
  return true; 
end ;
$body$ language plpgsql ; 



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


drop table if exists all_pass_tt; 
create table all_pass_tt (subdate character varying, date_time timestamp, b_volts double precision, degc double precision, 
             pahavg double precision,pahmax double precision, amv double precision, bmv double precision, bma double precision,pma double precision);

select dropallfunct('readPASSCsv')  ; 


create or replace function readPASSCsv(f_path text) 
returns boolean as $$  
Declare 
  sqlstr text; 
  subid RECORD; 
  subidstr text ;
begin 
  sqlstr='select unnest(regexp_matches(fname,'||quote_literal('([^.]+)')||')) as subid '|| 
         '  from ( select unnest(regexp_matches('||quote_literal(f_path)||','||quote_literal('([_0-9a-zA-Z.]{1,200}$)')||')) as fname)a'  ;  
  execute sqlstr into subid ; 
  subidstr:=subid.subid ;
  RAISE NOTICE ' % ... ... ', subidstr;  
  execute 'drop table if exists mysubpass'; 
  execute 'create table mysubpass (date_time character varying, b_volts double precision, degc double precision, 
             pahavg double precision,pahmax double precision, amv double precision, bmv double precision,bma double precision, pma double precision)';   
  execute 'copy mysubpass from '||quote_literal(f_path)||' CSV HEADER NULL '||quote_literal('') ;
  sqlstr='insert into all_pass_tt select '||quote_literal(subidstr)||', cast(date_time as timestamp) as date_time, b_volts,degc,pahavg,pahmax,
              amv,bmv,bma, pma from mysubpass'    ;
  execute sqlstr ;       
  return TRUE; 
end ; 
$$ language plpgsql  ; 


select readPASSCsv('D:/ZData/PAH/data/PAS/PAHVS_SU11_PASData/csv/P103_SU11_WK08_PAS1.csv') ; 
select * from mysubpass ; 
 

select dropallfunct('getPASTTable')  ;

create or replace function getPASTTable(path text)
returns text[] as $$
declare 
  file RECORD; 
  files text[];
  i integer:=1; 
begin  
  for file in  select pg_ls_dir(path) as ql order by ql loop 
    files[i]:=file.ql ; 
    i:=i+1; 
  end loop; 
  return files ; 
end 
$$ language plpgsql ; 

select unnest(getPASTTable('.')) ; 

select pg_ls_dir('.') as ql order by ql 




select substring(a.fname,1,2),fname, unnest(regexp_matches(fname,'([^.]+)')) 
  from (
          select unnest(regexp_matches('D:/ZData/PAH/data/PAS/PAHVS_SU11_PASData/csv/SCAQMD09_SU11_WK04_PAS3.csv','([_0-9a-zA-Z.]{1,200}$)')) as fname   
       )a  ;

-----


select site_id, sampling_period from pahvs_su11_master_datesheet_101411 ; 

drop table if exists sampling_tmp ; 
create table sampling_tmp as 
  select *,  cast('2011-'||month||'-'||dd1 as date) as begin_day, cast('2011-'||month||'-'||dd2 as date) as end_day,
    (case when pas_time_start<>'-' then cast('2011-'||month||'-'||dd1||' '||pas_time_start as timestamp) else NULL end) as begin_time,   
    (case when pas_time_end<>'-' then cast('2011-'||month||'-'||dd2||' '||pas_time_end as timestamp) else NULL end) as end_time  from 
     (select site_id,sampling_period, upper(unnest(regexp_matches(sampling_period, '[^ ]+'))) as month,
                unnest(regexp_matches(sampling_period, '[0-9]{1,20}')) as dd1,
                trim('-' from unnest(regexp_matches(sampling_period, '-[0-9]{1,20}'))) as dd2 , pas_time_start,pas_time_end   
        from pahvs_su11_master_datesheet_101411    
     )a    ;

 
----
update pahvs_w11_master_datesheet_041812 set sampling_period='Feb  28 - Mar 02' where sampling_period='Feb  29 - Mar 02'   ;

drop table if exists sampling_tmp ; 
create table sampling_tmp as 
  select *,  cast('2011-'||month||'-'||dd1 as date) as begin_day, cast('2011-'||month||'-'||dd2 as date) as end_day,
    (case when puf_date_time_install<>'-' then cast('2011-'||month||'-'||dd1||' '||puf_date_time_install as timestamp) else NULL end) as begin_time,   
    (case when puf_date_time_end<>'-' then cast('2011-'||month||'-'||dd2||' '||puf_date_time_end as timestamp) else NULL end) as end_time  from 
     (select site_id,sampling_period, upper(unnest(regexp_matches(sampling_period, '[^ ]+'))) as month,
                unnest(regexp_matches(sampling_period, '[0-9]{1,20}')) as dd1,
                unnest(regexp_matches(unnest(regexp_matches(sampling_period, '-[a-zA-Z0-9 ]{1,20}')),'[0-9]{1,3}')) as dd2 , puf_date_time_install,puf_date_time_end   
        from pahvs_su11_master_datesheet_101411    
     )a    ;


drop table if exists sampling_tmp ; 
create table sampling_tmp as 
  select *,  cast('2011-'||month||'-'||dd1 as date) as begin_day, cast('2011-'||month||'-'||dd2 as date) as end_day,
    (case when puf_time_install<>'-' then cast('2011-'||month||'-'||dd1||' '||puf_time_install as timestamp) else NULL end) as begin_time,   
    (case when puf_time_end<>'-' then cast('2011-'||month||'-'||dd2||' '||puf_time_end as timestamp) else NULL end) as end_time  from 
     (select site_id,sampling_period, upper(unnest(regexp_matches(sampling_period, '[^ ]+'))) as month,
                unnest(regexp_matches(sampling_period, '[0-9]{1,20}')) as dd1,
                unnest(regexp_matches(unnest(regexp_matches(sampling_period, '-[a-zA-Z0-9 ]{1,20}')),'[0-9]{1,3}')) as dd2 , puf_time_install,puf_time_end   
        from pahvs_w11_master_datesheet_041812    
     )a    ;

set @atb='pahvs_w11_master_datesheet_041812'; 
alter table @atb drop column if exists  begin_day ; 
alter table @atb drop column if exists  end_day ;
alter table @atb add column begin_day date  ; 
alter table @atb add column end_day date  ;   

alter table @atb drop column if exists  begin_time ; 
alter table @atb drop column if exists  end_time;
alter table @atb add column begin_time timestamp  ; 
alter table @atb add column end_time timestamp  ;   

update @atb t 
  set begin_day=v.begin_day,end_day=v.end_day, begin_time=v.begin_time, end_time=v.end_time 
  from  sampling_tmp v 
  where t.site_id=v.site_id and t.sampling_period=v.sampling_period   ;




alter table ca_pah_2011_13_afr add column begin_time timestamp ;

alter table ca_pah_2011_13_afr add column end_time timestamp ; 

update ca_pah_2011_13_afr t 
  set begin_time=v.begin_time, end_time=v.end_time 
  from pahvs_su11_master_datesheet_101411 v 
  where v.quartz_filter_sample_label=t.decriptions   ;


update ca_pah_2011_13_afr t 
  set begin_time=v.begin_time, end_time=v.end_time 
  from pahvs_w11_master_datesheet_041812 v 
  where v.quartz_filter_sample_label=t.decriptions   ; 

drop table if exists all_pass_tts  ; 
create table   all_pass_tts as 
  select split_part(subdate,'_',1) as stationid,split_part(subdate,'_',2) as snid,split_part(subdate,'_',3) as wkid,*  from all_pass_tt ; 


select stationid, snid , avg(pahmax) 
  from (select distinct a.* from all_pass_tts a inner join ca_pah_2011_13_afr b 
          on a.stationid=b.stationid  and  a.date_time between b.begin_time and b.end_time  )a 
  group by stationid, snid   ;

alter table   ca_pah_2011_13_afr drop column if exists sum ; 
alter table   ca_pah_2011_13_afr add column sum double precision ;
update ca_pah_2011_13_afr set 
  sum=(napthalene+acenaphthylene+acenapthene+fluorene+phenanthrene+anthracene+fluoranthene+pyrene+benzo_a_anthracene+chrysene)   ; 

update pah_apbo_7days_summer_covs3 t set pop=v.pop 
  from pah_apbo_7days_summer_covs2 v where t.stationid=v.stationid ; 


update pah_apbo_7days_winter_covs3 t set pop=v.pop 
  from pah_apbo_7days_winter_covs2 v where t.stationid=v.stationid ; 


select * from pah_apbo_7days_summer_covs2 where stationid='SCAQMD04'  
select * from pah_apbo_7days_summer_covs3 where pop is null 

alter table   ca_pah_2011_13_afr drop column if exists sum_m1 ; 
alter table   ca_pah_2011_13_afr add column sum_m1 double precision ; 
update ca_pah_2011_13_afr t 
  set sum_m1=v.avg  
  from (select stationid, snid , avg(pahavg)  as avg 
          from (select distinct a.* from all_pass_tts a inner join ca_pah_2011_13_afr b 
            on a.stationid=b.stationid  and  a.date_time between b.begin_time and b.end_time  )a 
          group by stationid, snid  )v 
  where t.stationid=v.stationid and v.snid=t.season  ;

select from pah_apbo_7days_winter_covs3 

alter table   ca_pah_2011_13_afr drop column if exists max_m1 ; 
alter table   ca_pah_2011_13_afr add column max_m1 double precision ; 
update ca_pah_2011_13_afr t 
  set max_m1=v.max  
  from (select stationid, snid , max(pahmax)  as max 
          from (select distinct a.* from all_pass_tts a inner join ca_pah_2011_13_afr b 
            on a.stationid=b.stationid  and  a.date_time between b.begin_time and b.end_time  )a 
          group by stationid, snid  )v 
  where t.stationid=v.stationid and v.snid=t.season ; 

select 'summer' as sn, min(met_ws) as min, max(met_ws) as max from pah_apbo_7days_summer_covs3   
union 
select 'winter' as sn, min(met_ws) as min , max(met_ws) as max from pah_apbo_7days_winter_covs3  ; 

select 'summer' as sn, min(met_tmp) as min, max(met_tmp) as max from pah_apbo_7days_summer_covs3  
union 
select 'winter' as sn, min(met_tmp) as min, max(met_tmp) as max from pah_apbo_7days_winter_covs3   ;
 

alter table   ca_pah_2011_13_afr drop column if exists min_m1 ; 
alter table   ca_pah_2011_13_afr add column min_m1 double precision ;  
update ca_pah_2011_13_afr t 
  set min_m1=v.min  
  from (select stationid, snid , min(pahavg)  as min 
          from (select distinct a.* from all_pass_tts a inner join ca_pah_2011_13_afr b 
            on a.stationid=b.stationid  and  a.date_time between b.begin_time and b.end_time  )a 
          group by stationid, snid  )v 
  where t.stationid=v.stationid and v.snid=t.season ; 
  
select corr(sum_m1,sum) from ca_pah_2011_13_afr ;

select * from ca_pah_2011_13_afr 

