delete from public.subcategory;
insert into public.subcategory(
	subcategory_name,
	category_id
)

select 
	md5(random()::text),
	floor(random() * 999 + 1)
from generate_series(1,5000) s(i);