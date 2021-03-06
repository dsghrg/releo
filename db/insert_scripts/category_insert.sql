insert into public.category (
    category_name
)
select
    md5(random()::text)
from generate_series(1, 1000) s(i);