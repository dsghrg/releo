delete from public.order_details;
insert into public.order_details(
	order_id,
	product_id,
	price,
	quantity
)

select 
   	floor(random() * 1999999 +  (select (min(id)) from public.order)),
   	floor(random() * 99999 +  (select (min(id)) from public.product)),
	floor(random() *  480 + 20),
	floor(random() * 19 + 1)
from generate_series(1,6000000) s(i);