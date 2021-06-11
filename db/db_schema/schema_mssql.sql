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

--ALTER TABLE [dbo].[categor]
-- ADD CONSTRAINT FK_Product_ProductCategoryID FOREIGN KEY (ProductCategoryID)
   -- REFERENCES [dbo].[ProductCategory] (ProductCategoryID)

--ALTER TABLE [dbo].category
 --   ADD CONSTRAINT category_pkey PRIMARY KEY (id);


ALTER TABLE  [dbo].customer
    ADD CONSTRAINT customer_pkey PRIMARY KEY (id);


--
-- TOC entry 2748 (class 2606 OID 63701)
-- Name: deliverer deliverer_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE  [dbo].deliverer
    ADD CONSTRAINT deliverer_pkey PRIMARY KEY (id);


--
-- TOC entry 2750 (class 2606 OID 63714)
-- Name: discount discount_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE  [dbo].discount
    ADD CONSTRAINT discount_pkey PRIMARY KEY (id);


--
-- TOC entry 2746 (class 2606 OID 63681)
-- Name: order_details order_details_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE  [dbo].order_details
    ADD CONSTRAINT order_details_pkey PRIMARY KEY (id);


--
-- TOC entry 2744 (class 2606 OID 63669)
-- Name: order order_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE  [dbo].[order]
    ADD CONSTRAINT order_pkey PRIMARY KEY (id);


--
-- TOC entry 2736 (class 2606 OID 63620)
-- Name: product product_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE  [dbo].product
    ADD CONSTRAINT product_pkey PRIMARY KEY (id);


--
-- TOC entry 2740 (class 2606 OID 63642)
-- Name: subcategory subcategory_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE  [dbo].subcategory
    ADD CONSTRAINT subcategory_pkey PRIMARY KEY (id);





--
-- TOC entry 2755 (class 2606 OID 63670)
-- Name: order fk_customer_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE  [dbo].[order]
    ADD CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES [dbo].customer(id);


--
-- TOC entry 2756 (class 2606 OID 63702)
-- Name: [order] fk_deliverer_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE  [dbo].[order]
    ADD CONSTRAINT fk_deliverer_id FOREIGN KEY (deliverer_id) REFERENCES [dbo].deliverer(id);


--
-- TOC entry 2757 (class 2606 OID 63717)
-- Name: [order] fk_discount_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE  [dbo].[order]
    ADD CONSTRAINT fk_discount_id FOREIGN KEY (discount_id) REFERENCES [dbo].discount(id);


--
-- TOC entry 2758 (class 2606 OID 63682)
-- Name: [order]_details fk_[order]_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE  [dbo].order_details
    ADD CONSTRAINT fk_order_id FOREIGN KEY (order_id) REFERENCES [dbo].[order](id);


--
-- TOC entry 2759 (class 2606 OID 63687)
-- Name: [order]_details fk_product_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE  [dbo].order_details
    ADD CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES [dbo].product(id);


--
-- TOC entry 2753 (class 2606 OID 63648)
-- Name: product fk_subcategory_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE  [dbo].product
    ADD CONSTRAINT fk_subcategory_id FOREIGN KEY (subcategory_id) REFERENCES [dbo].subcategory(id);


