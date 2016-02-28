create index usainfo_geomp2_index on usainfo using GIST (geom_p2)     ; 

drop function if exists NearPtUSAInfo(_usatb text, _inputtb text, _outcol text, _tarcol text ,_values anyarray);
 
create  or replace function NearUSAInfo(_usatb text, _inputtb text,_maxdist integer, _outcol text,
                                        _tarcol text ,_values anyarray) 
returns boolean as $$ 
declare 
  index integer ; 
  len integer; 
  sqlstr text; 
  cnt integer ; 
begin 
  len:=array_length(_values,1); 
  execute 'drop table if exists usainfo_tmp cascade '; 
  sqlstr:='create table usainfo_tmp as select * from usainfo where ';
  index:=0 ; 
  While index<len loop 
    if index==0 then 
       sqlstr:=sqlstr||' naics6='||_values[index]::text   ;
    else 
       sqlstr:=sqlstr||' or naics6='||_values[index]::text   ;  
     end if ;   
    index:=index+1; 
  end loop;  
  Raise notice 'SQL query: %s', sqlstr; 
  execute sqlstr  ; 
  execute 'alter table '||quote_ident(_inputtb)||' drop column if exists '||quote_ident(_outcol) ; 
  execute 'alter table '||quote_ident(_inputtb)||' add column '||quote_ident(_outcol)||' double precision ' ;
  sqlstr:='update '|| quote_ident(_inputtb)||' t set '||quote_ident(_outcol)||'=v.dist '||
          '  from (select distinct on (a.stationid) a.stationid, ST_Distance(a.geom, b.geom) as dist  from  '||quote_ident(_inputtb)||
          '           a left join usainfo b on ST_Distance(a.geom, b.geom)<'||_maxdist::text||' order by stationid, dist )v '||
           ' where t.stationid=v.stationid'   ;
  execute sqlstr  ;  
end ; 
$$ language plpgsql ; 
-- function NearUSAInfo(_usatb text, _inputtb text,_maxdist integer, _outcol text,_tarcol text ,_values anyarray) 

select NearUSAInfo('usainfo',)  ;  

alter table usainfo rename column geom to geom_p1 ; 
alter table usainfo rename column geom_p2 to geom ; 

722110	11. Leisure Hospitality	Full-Service Restaurants
722211	11. Leisure Hospitality	Limited-Service Restaurants
722212	11. Leisure Hospitality	Cafeterias, Grill Buffets, and Buffets
722213	11. Leisure Hospitality	Snack and Nonalcoholic Beverage Bars
722310	11. Leisure Hospitality	Food Service Contractors

alter table la_tracts_covs  drop column if exists cooking_fac_cnt2     ;
alter table la_tracts_covs  add column  cooking_fac_cnt2 integer default (0) ; 

update la_tracts_covs t set cooking_fac_cnt2=v.count 
  from ( select  a.tract,count(b.geom) as count  from (select tract, (ST_Dump(ST_Buffer(geom,500))).geom as bufgeom  from la_tracts_covs) a inner join 
           (select * from usainfo where naics6 in (722110,722211,722212,722310)  )b  
              on ST_Contains(a.bufgeom, b.geom) 
           group by a.tract )v 
  where t.tract=v.tract   ;     

