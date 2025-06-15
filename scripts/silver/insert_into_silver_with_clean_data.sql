USE DataWarehouse;

PRINT '>> Truncating table silver.crm_cust_info'
TRUNCATE TABLE silver.crm_cust_info;
PRINT '>> Inserting Data Into silver.crm_cust_info'
INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gender,
	cst_create_date
)
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE 
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'Unknown'
	END AS cst_marital_status,
	CASE 
		WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
		ELSE 'Unknown'
	END AS cst_gender,
	cst_create_date
FROM (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
)t
WHERE flag_last = 1;


PRINT '>> Truncating table silver.crm_prd_info'
TRUNCATE TABLE silver.crm_prd_info;
PRINT '>> Inserting Data Into silver.crm_prd_info'
INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'N/A'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;


PRINT '>> Truncating table silver.crm_sales_details'
TRUNCATE TABLE silver.crm_sales_details;
PRINT '>> Inserting Data Into silver.crm_sales_details'
INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
	END AS sls_due_dt,
	CASE 
		WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) 
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE
		WHEN sls_price = 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
		WHEN sls_price < 0 THEN sls_price * -1
		ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details;


PRINT '>> Truncating table silver.erp_cust_az12'
TRUNCATE TABLE silver.erp_cust_az12;
PRINT '>> Inserting Data Into silver.erp_cust_az12'
INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen
)
SELECT
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
		ELSE cid
	END AS cid,
	CASE
		WHEN bdate > GETDATE() THEN NULL -- Remove future birth dates
		ELSE bdate
	END AS bdate,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'Unknown'
	END AS gen -- Standardized gender to Female/Male and handled null or empty values with Unknown
FROM bronze.erp_cust_az12


PRINT '>> Truncating table silver.erp_loc_a101'
TRUNCATE TABLE silver.erp_loc_a101;
PRINT '>> Inserting Data Into silver.erp_loc_a101'
INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT
	REPLACE(cid, '-', '') AS cid, -- Removed extra - on customer IDs
	CASE 
		WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany' 
		WHEN UPPER(TRIM(cntry)) IN ('USA', 'US') THEN 'United States' 
		WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'Unknown'
		ELSE cntry
	END AS CNTRY -- Standardized country names and handled empty/null values with Unknown
FROM bronze.erp_loc_a101


PRINT '>> Truncating table silver.erp_px_cat_g1v2'
TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT '>> Inserting Data Into silver.erp_px_cat_g1v2'
INSERT INTO silver.erp_px_cat_g1v2 (
	ID,
	CAT,
	SUBCAT,
	MAINTENANCE
)
SELECT
	ID,
	CAT,
	SUBCAT,
	MAINTENANCE
FROM bronze.erp_px_cat_g1v2