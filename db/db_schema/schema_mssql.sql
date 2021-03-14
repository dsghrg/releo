CREATE TABLE dbo.category (
    id bigint NOT NULL,
    category_name varchar(255) NOT NULL
)

CREATE TABLE dbo.customer (
    id bigint NOT NULL,
    [name] varchar(255) NOT NULL
)

CREATE TABLE dbo.deliverer (
    id bigint NOT NULL,
    [name] varchar(255) NOT NULL
)

CREATE TABLE dbo.discount (
    id bigint NOT NULL,
    code varchar(255) NOT NULL,
    discount float NOT NULL
)

CREATE TABLE dbo.[order] (
    id bigint NOT NULL,
    customer_id bigint NOT NULL,
    amount float NOT NULL,
    deliverer_id bigint NOT NULL,
    discount_id bigint NOT NULL
)

CREATE TABLE dbo.order_details (
    id bigint NOT NULL,
    order_id bigint NOT NULL,
    product_id bigint NOT NULL,
    price float NOT NULL,
    quantity integer NOT NULL
)

CREATE TABLE dbo.product (
    [name] varchar(255) NOT NULL,
    id bigint NOT NULL,
    price bigint NOT NULL,
    subcategory_id bigint NOT NULL
)

CREATE TABLE dbo.subcategory (
    id bigint NOT NULL,
    subcategory_name varchar(255) NOT NULL,
    category_id bigint NOT NULL
)

