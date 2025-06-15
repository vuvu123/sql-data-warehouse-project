-- Check for nulls or duplicates in the primary key
-- Expectation: No results
SELECT 
	cid, 
	COUNT(*) as cnt
FROM bronze.erp_cust_az12
GROUP BY cid
HAVING count(*) > 1 OR cid IS NULL;

-- Check for unwanted spaces
-- Expectation: No results
SELECT 
	prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)


-- Check for invalid cost values
-- Expectation: No results
SELECT 
	prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for Invalid Date Orders
-- Expectation: No Results
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check for invalid dates
SELECT
	NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) <> 8

-- Check Data Consistency: Between Sales, Quantity and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales

-- Identify Out-of-Range dates
-- Over 100 years old and birthdate in the future
SELECT DISTINCT
	bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data Standardization & Consistency
SELECT DISTINCT
	gen,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'Unknown'
	END AS gen_clean
FROM bronze.erp_cust_az12

-- Check for unwanted spaces
SELECT DISTINCT
	*
FROM silver.erp_px_cat_g1v2
WHERE MAINTENANCE != TRIM(MAINTENANCE) OR SUBCAT != TRIM(SUBCAT) OR CAT != TRIM(CAT)

-- Data Standardization & Consistency
SELECT DISTINCT
	SUBCAT
FROM bronze.erp_px_cat_g1v2

SELECT
	ID,
	CAT,
	SUBCAT,
	MAINTENANCE
FROM silver.erp_px_cat_g1v2