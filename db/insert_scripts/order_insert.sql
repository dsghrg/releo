delete from public.order;
insert into public.order(
	customer_id,
	amount,
	deliverer_id,
	discount_id
)

select 
    floor(random() * 499999 +  (select (min(id)) from public.customer)),
	random()*1480 + 20,
	floor(random() * 3 +  (select (min(id)) from public.deliverer)),
	floor(random() * 49 + (select (min(id)) from public.dicount))
from generate_series(1,2000000) s(i);