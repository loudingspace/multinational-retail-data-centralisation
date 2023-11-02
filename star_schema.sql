-- assign types for the orders_table
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN product_quantity TYPE SMALLINT;
-- assign types for dim_users
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN join_date TYPE DATE;
-- assign types for dim_store_details
ALTER TABLE dim_stores_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN staff_numbers TYPE SMALLINT,
    ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN store_type DROP NOT NULL,
    ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN continent TYPE VARCHAR(255);
-- change NULL values to N/A    
UPDATE dim_stores_details
SET address = 'N/A',
    locality = 'N/A',
    country_code = 'N/A'
WHERE store_code ILIKE 'WEB-1388012W';
-- create weight_class
ALTER TABLE dim_products
ADD weight_class VARCHAR(14);
UPDATE dim_products
SET weight_class = CASE
        WHEN weight < 2 THEN 'Light'
        WHEN weight >= 2
        AND weight < 40 THEN 'Mid_Sized'
        WHEN weight >= 40
        AND weight < 140 THEN 'Heavy'
        WHEN weight > 120 THEN 'Long'
    END;
-- do preparation to make column boolean, then change name to still_available
UPDATE dim_products
SET removed = REPLACE(removed, 'Still_avaliable', 'True');
UPDATE dim_products
SET removed = REPLACE(removed, 'Removed', 'False');
ALTER TABLE dim_products
    RENAME COLUMN "removed" TO still_available;
-- assign types for dim_products 
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT,
    ALTER COLUMN weight TYPE FLOAT,
    ALTER COLUMN "EAN" TYPE VARCHAR(17),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
    ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
    ALTER COLUMN still_available TYPE BOOL USING still_available::BOOL,
    ALTER COLUMN weight_class TYPE VARCHAR(9);
-- assign one type for date
ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;
-- assign types for dim_card_details
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN expiry_date TYPE DATE USING expiry_date::DATE,
    ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;
-- set primary keys for all the tables that are referenced in orders_table
ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);
ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);
ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);
ALTER TABLE dim_stores_details
ADD PRIMARY KEY (store_code);
ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);
-- set foreign keys orders table to reference our newly created primary keys
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_dim_date_times FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_dim_users FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_dim_card_details FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_dim_stores_details FOREIGN KEY (store_code) REFERENCES dim_stores_details(store_code);
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_dim_products FOREIGN KEY (product_code) REFERENCES dim_products(product_code);