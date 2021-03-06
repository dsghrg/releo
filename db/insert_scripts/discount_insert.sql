delete from public.discount;
insert into public.discount(
	code,
	discount
)

select 
	md5(random()::text),
	random()
from generate_series(1,50) s(i);