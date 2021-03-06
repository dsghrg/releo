delete from public.product;
insert into public.product(
	name,
	price,
	subcategory_id
)

select 
	md5(random()::text),
	floor(random() * 480 + 20),
	floor(random() * (select (max(id) -1) from public.subcaegory) + (select (min(id)) from public.subcaegory))
from generate_series(1,100000) s(i);