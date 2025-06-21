-- ===================
-- Change Over Time
-- ===================

-- Total Sales By Year
SELECT
	YEAR(order_date) AS order_year,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customer,
	COUNT(DISTINCT product_key) AS total_products,
	SUM(quantity) AS total_qty,
	AVG(price) AS avg_price
FROM gold.fact_sales
WHERE YEAR(order_date) IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY order_year

-- Total Sales, Customers, Products, Qty sold, Avg price by Year, Month
SELECT
	YEAR(order_date) AS order_year,
	MONTH(order_date) AS order_month,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customer,
	COUNT(DISTINCT product_key) AS total_products,
	SUM(quantity) AS total_qty,
	AVG(price) AS avg_price
FROM gold.fact_sales
WHERE MONTH(order_date) IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY order_year, order_month

-- Date Trunc to combine both year and month
SELECT
	DATETRUNC(month, order_date) AS order_date,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customer,
	COUNT(DISTINCT product_key) AS total_products,
	SUM(quantity) AS total_qty,
	AVG(price) AS avg_price
FROM gold.fact_sales
WHERE MONTH(order_date) IS NOT NULL
GROUP BY DATETRUNC(month, order_date)
ORDER BY DATETRUNC(month, order_date)

-- ===================
-- Cumulative Analysis
-- ===================

-- Calculate the total sales per month and the running total of sales over time, resetting each year
WITH sales_per_month AS (
	SELECT
		DATETRUNC(month, order_date) AS order_date,
		SUM(sales_amount) AS total_sales
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(month, order_date)
)

SELECT
	order_date,
	total_sales,
	SUM(total_sales) OVER(PARTITION BY YEAR(order_date) ORDER BY order_date) AS running_total_sales
FROM sales_per_month;

-- Total sales per year with a running total all time
WITH sales_per_year AS (
	SELECT
		DATETRUNC(YEAR, order_date) AS order_date,
		SUM(sales_amount) AS total_sales
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(YEAR, order_date)
)

SELECT
	order_date,
	total_sales,
	SUM(total_sales) OVER( ORDER BY order_date) AS running_total_sales
FROM sales_per_year;

-- Moving average price

WITH sales_per_year AS (
	SELECT
		DATETRUNC(YEAR, order_date) AS order_date,
		SUM(sales_amount) AS total_sales,
		AVG(price) AS avg_price
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(YEAR, order_date)
)

SELECT
	order_date,
	total_sales,
	SUM(total_sales) OVER( ORDER BY order_date) AS running_total_sales,
	AVG(avg_price) OVER(ORDER BY order_date) AS moving_avg
FROM sales_per_year;

-- ======================
-- Performance Analysis
-- ======================

-- Analyze the yearly performance of products by comparing each product's sales to both its average sales performance
-- and the previous year's sales

WITH yearly_product_sales AS (
	SELECT
		p.product_name,
		YEAR(order_date) AS order_year,
		SUM(sales_amount) AS current_sales
	FROM gold.fact_sales fs
	LEFT JOIN gold.dim_products p ON fs.product_key = p.product_key
	WHERE order_date IS NOT NULL
	GROUP BY YEAR(order_date), p.product_name
)

SELECT
	order_year,
	product_name,
	current_sales,
	LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS prev_year_sales,
	current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS diff_prev_year_sales,
	ROUND(((current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year)) / 
	LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year)) * 100, 2) AS YoY_pct_change,
	CASE 
		WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
		WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
		ELSE 'No Change'
	END AS prev_year_change,
	AVG(current_sales) OVER(PARTITION BY product_name) AS product_sales_avg,
	current_sales - AVG(current_sales) OVER(PARTITION BY product_name) AS diff_avg_sales,
	CASE 
		WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) > 0 THEN 'Above Avg'
		WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) < 0 THEN 'Below Avg'
		ELSE 'Hit Avg'
	END AS hit_avg
FROM yearly_product_sales
ORDER BY product_name, order_year;

-- ====================================
-- Part-to-Whole Proportional Analysis
-- ====================================
SELECT
	p.category,
	SUM(fs.sales_amount) AS total_sales,
	FORMAT(SUM(fs.sales_amount) / (SELECT SUM(sales_amount) FROM gold.fact_sales), 'P') AS part_of_whole
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products p ON fs.product_key = p.product_key
GROUP BY p.category
ORDER BY total_sales DESC

-- ===================
-- Data Segmentation
-- ===================
-- Segment customers into different age range buckets
WITH age_buckets AS (
	SELECT
		DATEDIFF(year, c.birth_date, GETDATE()) AS age,
		CASE 
			WHEN DATEDIFF(year, c.birth_date, GETDATE()) BETWEEN 0 AND 9 THEN 'Child'
			WHEN DATEDIFF(year, c.birth_date, GETDATE()) BETWEEN 10 AND 17 THEN 'Teenager'
			WHEN DATEDIFF(year, c.birth_date, GETDATE()) BETWEEN 18 AND 29 THEN 'Young Adult'
			WHEN DATEDIFF(year, c.birth_date, GETDATE()) BETWEEN 30 AND 50 THEN 'Middle Age'
			WHEN DATEDIFF(year, c.birth_date, GETDATE()) BETWEEN 51 AND 65 THEN 'Close to Retirement'
			ELSE'Retired'
		END AS age_category,
		COUNT(c.customer_key) AS customer_count
	FROM gold.fact_sales fs
	LEFT JOIN gold.dim_customers c ON fs.customer_key = c.customer_key
	WHERE c.birth_date IS NOT NULL
	GROUP BY DATEDIFF(year, c.birth_date, GETDATE())
)

SELECT
	age_category,
	SUM(customer_count) AS total_customers
FROM age_buckets
GROUP BY age_category
ORDER BY total_customers DESC;

-- Segment products into cost ranges and count how many products fall into each segment
WITH cost_ranges AS (
	SELECT
		product_key,
		product_name,
		cost,
		CASE 
			WHEN cost < 100 THEN 'Below 100'
			WHEN cost BETWEEN 100 AND 500 THEN '100-500'
			WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
			ELSE 'Above 1000'
		END AS cost_range
	FROM gold.dim_products
)

SELECT
	cost_range,
	COUNT(product_key) AS product_count
FROM cost_ranges
GROUP BY cost_range
ORDER BY product_count DESC

/*
Group customers into three segments based on their spending behavior:
	-VIP: Customers with atleast 12 months of history and spending more than 5000
	-Regular: Customers with at least 12 months of history but spending 5000 or less
	-New: Customers with a lifespan less than 12 months
And find the total number of customers by each group
*/

WITH cust_spend_data AS (
	SELECT
		c.customer_key,
		DATEDIFF(month, MIN(fs.order_date), MAX(fs.order_date)) AS order_history_length,
		SUM(fs.sales_amount) AS total_spent
	FROM gold.fact_sales fs
	LEFT JOIN gold.dim_customers c ON fs.customer_key = c.customer_key
	GROUP BY c.customer_key
), segmented_custs AS (
	SELECT
		customer_key,
		CASE
			WHEN order_history_length >= 12 AND total_spent > 5000 THEN 'VIP'
			WHEN order_history_length >= 12 AND total_spent BETWEEN 0 AND 5000 THEN 'Regular'
			ELSE 'New'
		END AS cust_level
	FROM cust_spend_data
)

SELECT
	cust_level,
	COUNT(customer_key) AS total_customers
FROM segmented_custs
GROUP BY Cust_Level;

/*
========================================================================
Customer Report
========================================================================
Purpose:
	- This report consolidates key customer metrics and behaviors

Highlights:
	1. Gathers essential fields such as names, ages, and transaction details
	2. Segments customers into categories (VIP, Regular, New) and age groups
	3. Aggregates customer-level metrics:
		- total orders
		- total sales
		- total quantity purchased
		- total products
		- lifespan (in months)
	4. Calculates valuable KPIs:
		- recency (months since last order)
		- average order value
		- average monthly spend
========================================================================
*/
CREATE VIEW gold.customer_report AS
WITH base AS (
/*--------------------------------------------------------
1) Base Query: Retrieve core columns from tables
*/--------------------------------------------------------
	SELECT
		s.order_number,
		s.order_date,
		s.ship_date,
		s.quantity,
		s.price,
		s.sales_amount,
		c.customer_key,
		c.customer_number,
		c.first_name + ' ' + c.last_name AS full_name,
		DATEDIFF(year, birth_date, GETDATE()) AS age,
		p.product_key,
		p.product_name
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p ON s.product_key = p.product_key
	LEFT JOIN gold.dim_customers c ON s.customer_key = c.customer_key
	WHERE s.order_date IS NOT NULL
), customer_aggregation AS (
	SELECT 
		customer_key,
		customer_number,
		full_name,
		age,
		COUNT(DISTINCT order_number) AS total_orders,
		SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_qty_purchased,
		COUNT(DISTINCT product_key) AS total_products,
		MIN(order_date) AS first_order_date,
		MAX(order_date) AS last_order_date,
		DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan_months
	FROM base
	GROUP BY 
		customer_key, 
		customer_number, 
		full_name, 
		age
)

SELECT
	customer_key,
	customer_number,
	full_name,
	age,
	CASE 
		WHEN age < 20 THEN 'Under 20'
		WHEN age BETWEEN 20 and 29 THEN '20-29'
		WHEN age BETWEEN 30 and 39 THEN '30-49'
		WHEN age BETWEEN 40 and 49 THEN '40-49'
		ELSE 'Over 50'
	END AS age_category,
	total_orders,
	total_sales,
	total_qty_purchased,
	total_products,
	first_order_date,
	last_order_date,
	CASE
		WHEN lifespan_months >= 12 AND total_sales > 5000 THEN 'VIP'
		WHEN lifespan_months >= 12 AND total_sales BETWEEN 0 AND 5000 THEN 'Regular'
		ELSE 'New'
	END AS customer_status,
	DATEDIFF(month, last_order_date, GETDATE()) AS months_since_last_order,
	CASE 
		WHEN total_orders = 0 THEN 0
		ELSE total_sales / total_orders 
	END AS avg_order_value,
	CASE 
		WHEN lifespan_months = 0 THEN total_sales -- Customer ordered less than a month ago, show total_sales
		ELSE total_sales / lifespan_months 
	END AS avg_monthly_spend
FROM customer_aggregation;

/*
========================================================================
Product Report
========================================================================
Purpose:
	- This report consolidates key product metrics and behaviors

Highlights:
	1. Gathers essential fields such as product name, category, subcategory, and cost.
	2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers
	3. Aggregates product-level metrics:
		- total orders
		- total sales
		- total quantity sold
		- total unique customers
		- lifespan (in months)
	4. Calculates valuable KPIs:
		- recency (months since last sale)
		- average order revenue (AOR)
		- average monthly revenue
========================================================================
*/
ALTER VIEW gold.product_report AS
WITH base AS (
/*--------------------------------------------------------
1) Base Query: Retrieve core columns from tables
*/--------------------------------------------------------
	SELECT
		s.order_number,
		s.order_date,
		s.ship_date,
		s.quantity,
		s.price,
		s.sales_amount,
		c.customer_key,
		c.customer_number,
		c.first_name + ' ' + c.last_name AS full_name,
		DATEDIFF(year, birth_date, GETDATE()) AS age,
		p.product_key,
		p.product_name,
		p.category,
		p.subcategory,
		p.cost
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p ON s.product_key = p.product_key
	LEFT JOIN gold.dim_customers c ON s.customer_key = c.customer_key
	WHERE s.order_date IS NOT NULL
), product_aggregations AS (
	SELECT 
		product_key,
		product_name,
		category,
		subcategory,
		COUNT(DISTINCT order_number) AS total_orders,
		COUNT(DISTINCT customer_key) AS total_unique_customers,
		SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_qty_sold,
		MAX(order_date) AS last_order_date,
		DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan_months
	FROM base
	GROUP BY 
		product_key, 
		product_name,
		category,
		subcategory
)

SELECT
	product_key,
	product_name,
	category,
	subcategory,
	DATEDIFF(month, last_order_date, GETDATE()) AS months_since_last_order,
	CASE 
		WHEN total_orders = 0 THEN 0
		ELSE total_sales / total_orders 
	END AS avg_order_revenue,
	CASE 
		WHEN lifespan_months = 0 THEN total_sales
		ELSE total_sales / lifespan_months
	END AS avg_monthly_revenue,
	CASE 
		WHEN total_sales / lifespan_months < 25000 THEN 'Low-Performer'
		WHEN total_sales / lifespan_months < 60000 THEN 'Mid-Range'
		ELSE 'High-Performer'
	END AS product_level
FROM product_aggregations;