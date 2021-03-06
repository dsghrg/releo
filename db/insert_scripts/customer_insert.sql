delete from public.customer;
insert into public.customer(
	name
)

select 
	md5(random()::text),
from generate_series(1,500000) s(i);