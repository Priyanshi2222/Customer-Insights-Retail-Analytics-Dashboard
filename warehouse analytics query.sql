/*CHANGES OVER TIME ANALYSIS*/
SELECT 
FORMAT(order_date, 'yyyy-MM-01') AS order_date,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_new_customers, 
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY FORMAT(order_date, 'yyyy-MM-01')
ORDER BY FORMAT(order_date, 'yyyy-MM-01')


/*CUMULATIVE ANALYSIS: (by month)*/ 
SELECT 
    CONVERT(DATE, order_date) AS order_date,
    total_sales,
    SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
	AVG(avg_price) OVER (ORDER BY order_date) AS moving_average
FROM 
(
    SELECT 
        DATEADD(MONTH, DATEDIFF(MONTH, 0, order_date), 0) AS order_date,
        SUM(sales_amount) AS total_sales,
		AVG(price) AS avg_price
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATEADD(MONTH, DATEDIFF(MONTH, 0, order_date), 0)
) t

/*CUMULATIVE ANALYSIS: (by year)*/ 

SELECT 
    YEAR(order_date) AS order_year,
    total_sales,
    SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales
FROM 
(
    SELECT 
        DATEADD(YEAR, DATEDIFF(YEAR, 0, order_date), 0) AS order_date,
        SUM(sales_amount) AS total_sales
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATEADD(YEAR, DATEDIFF(YEAR, 0, order_date), 0)
) t

/*PERFORMANCE ANALYSIS*/ 
WITH yearly_product_sales AS 
(
SELECT 
      YEAR(s.order_date) AS order_year, 
	  p.product_name, 
	  SUM(s.sales_amount) AS current_sales
FROM gold.dim_products p
JOIN gold.fact_sales s ON p.product_key=s.product_key
WHERE order_date IS NOT NULL
GROUP BY YEAR(s.order_date), p.product_name
) 

SELECT order_year, product_name, current_sales, 
AVG(current_sales) OVER (PARTITION BY product_name ) avg_sales,
current_sales - AVG(current_sales) OVER (PARTITION BY product_name ) AS diff_avg, 
CASE WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name ) > 0 THEN 'Above Avg' 
     WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name ) < 0 THEN 'Below Avg' 
	 ELSE 'Avg' 
END avg_change, 
-- Year over year analysis--
LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) py_sales,
current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_py,
CASE WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase' 
     WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease' 
	 ELSE 'No change' 
END py_change
FROM yearly_product_sales
ORDER BY product_name, order_year


/* PART TO WHOLE ANALYSIS*/
WITH category_sales AS(
SELECT p.category, SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.category)
SELECT category, total_sales, 
SUM(total_sales) OVER () overall_sales, 
CONCAT(ROUND((CAST (total_sales AS FLOAT)/SUM(total_sales) OVER ())*100, 2), '%') AS percentage_of_total
FROM category_sales
ORDER BY total_sales DESC

/* DATA SEGMENTATION PART 1*/
WITH product_segments AS (
SELECT product_key, 
       product_name, 
	   cost,
	   CASE WHEN cost< 100 THEN 'Below 100'
	        WHEN cost BETWEEN 100 AND 500 THEN '100-500'
	        WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
	        ELSE 'Above 1000'
       END cost_range
FROM gold.dim_products
)
SELECT cost_range , COUNT(product_key) AS number_of_products
FROM product_segments
GROUP BY cost_range
ORDER BY number_of_products DESC

/* DATA SEGMENTATION PART 2*/
WITH customer_spending AS (
SELECT 
c.customer_key,
SUM(f.sales_amount) AS total_spending,
MIN(order_date) AS first_order,
MAX(order_date) AS last_order, 
DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key
)


SELECT customer_segment, COUNT(customer_key) AS total_customers
FROM
(SELECT customer_key, 
       CASE WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
	        WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
	        ELSE 'New'
	   END customer_segment 
FROM customer_spending
)t
GROUP BY customer_segment
ORDER BY total_customers DESC

/* REPORT*/
CREATE PROCEDURE GetReportCustomers AS
BEGIN
 
WITH 
base_query AS 
/*First query: Retrieves core columns from the table.*/
(SELECT f.order_number, 
       f.product_key, 
	   f.order_date, 
	   f.sales_amount, 
	   f.quantity, 
	   c.customer_key, 
	   c.customer_number,
	   CONCAT(c.first_name,' ', c.last_name) AS customer_name,
	   DATEDIFF( YEAR, c.birthdate, GETDATE()) age
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE order_date IS NOT NULL)

, 
customer_aggregation AS 
/* Customer Aggregation: Summarizes key metrics at the customer level. */
(
SELECT 
customer_key, 
customer_number,
customer_name,
age, 
COUNT(DISTINCT(order_number)) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT(product_key)) AS total_products,
MAX(order_date) AS last_order_date, 
DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
FROM base_query
GROUP BY customer_key, customer_number, customer_name, age) 

SELECT 
customer_key, 
customer_number, 
customer_name, 
age, 
CASE WHEN age < 20 THEN 'Under 20'
     WHEN age BETWEEN 20 AND 29 THEN '20-29'
	 WHEN age BETWEEN 30 AND 39 THEN '30-39'
	 WHEN age BETWEEN 40 AND 49 THEN '40-49'
	 ELSE '50 and above'
END AS age_group, 
CASE WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
	        WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
	        ELSE 'New'
END customer_segment,
last_order_date,
DATEDIFF(month, last_order_date, GETDATE()) AS recency, 
total_orders,
total_sales,
total_quantity,
total_products,
last_order_date, 
lifespan, 
-- Compute average order value (AVO) --
CASE WHEN total_sales = 0 THEN 0
     ELSE total_sales/ total_orders
END AS avg_order_value, 
-- Compute average monthly spend
CASE WHEN lifespan = 0 THEN total_sales
     ELSE total_sales/ lifespan
END AS avg_monthly_spend 
FROM customer_aggregation
END

EXEC GetReportCustomers;
