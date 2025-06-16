CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	loc.CNTRY AS country,
	ci.cst_marital_status AS marital_status,
	CASE
		WHEN ci.cst_gender != 'Unknown' THEN ci.cst_gender
		ELSE COALESCE(az.gen, 'Unknown')
	END AS gender,
	az.BDATE AS birth_date,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 az ON ci.cst_key = az.CID
LEFT JOIN silver.erp_loc_a101 loc ON ci.cst_key = loc.CID;

CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY p.prd_start_dt, p.prd_key) AS product_key,
	p.prd_id AS product_id,
	p.prd_key AS product_number,
	p.prd_nm AS product_name,
	p.cat_id AS category_id,
	c.CAT AS category,
	c.SUBCAT AS subcategory,
	c.MAINTENANCE AS maintenance,
	p.prd_cost AS cost,
	p.prd_line AS product_line,
	p.prd_start_dt AS start_date
FROM silver.crm_prd_info p
LEFT JOIN silver.erp_px_cat_g1v2 c ON p.cat_id = c.ID
WHERE prd_end_dt IS NULL; -- Filter out old historical data

CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_number,
	p.product_key,
	c.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS ship_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products p ON sd.sls_prd_key = p.product_number
LEFT JOIN gold.dim_customers c ON sd.sls_cust_id = c.customer_id
