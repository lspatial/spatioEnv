
alter table spt_samples_com_ext drop column if exists sline_dist  ; 
alter table spt_samples_com_ext add column sline_dist double precision ; 
update spt_samples_com_ext a set sline_dist=w.dist 
  from ( select distinct on (site_id) site_id as site_id,ST_Distance(l.the_geom,s.geom) as dist  from spt_samples_com_ext s
           left join shoreline l on ST_Distance(l.the_geom,s.geom)<500000 
           order by site_id, dist )w 
  where  a.site_id=w.site_id         ;   


select * from spt_samples_com_uid order by sline_dist  ;   


