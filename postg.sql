select * from table_metadata
;
select * from table_products
;
-- 3---
select 
	w.*
from (
	select 
		(select split_part(u.t,':',2) from unnest(string_to_array(log, ',')) as u(t) where 1=1 and split_part(u.t,':',1) ~~ '%position%') 
			AS split_log
		,* 
	from table_products
	) w
where 1=1 and split_log is not null
order by split_log::numeric
limit 5
;
-- 4 --
select brand
	,count(1) as cnt
	,count(distinct name) as cnt_name
from table_products
group by 1
;
-- 5 --
select 
	avg(salepriceu::numeric) as svg
from table_products