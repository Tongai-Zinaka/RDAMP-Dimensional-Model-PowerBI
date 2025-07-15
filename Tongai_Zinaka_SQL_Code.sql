# Creating and selecting our new Database
create database ace_database;
use ace_database;

# Creating A staging table
create table staging_sales(
	Order_ID varchar(50),
	Order_Date date,
	Order_Mode varchar(50),
	Customer_ID VARCHAR(50),
	City VARCHAR(100),
	Postal_Code VARCHAR(20),
	Country VARCHAR(50),
	Region VARCHAR(100),
	Product_ID VARCHAR(50),
	Product_Name VARCHAR(255),
    Category VARCHAR(100),
    Sub_Category VARCHAR(100),
    Sales DECIMAL(12,2),
    Cost_price DECIMAL(12,3),
    Quantity INT,
    Discount DECIMAL(5,4),
    Needs_Category_Review varchar(5)
    );
    
# Loading the cleaned data into the staging table.
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cleaned_sales.csv'
INTO TABLE staging_sales
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
;

# Creating the Dimension tables
-- 1. dim_customer
CREATE TABLE dim_customer (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_key VARCHAR(50) NOT NULL UNIQUE,
    segment VARCHAR(50) NOT NULL
);

INSERT INTO dim_customer (customer_key, segment)
SELECT DISTINCT customer_id, 'Consumer'  -- Default segment
FROM staging_sales;

-- 2. dim_category (with split columns)
CREATE TABLE dim_category (
    category_id INT AUTO_INCREMENT PRIMARY KEY,
    main_category VARCHAR(100) NOT NULL,
    sub_category VARCHAR(100) NOT NULL
);

INSERT INTO dim_category (main_category, sub_category)
SELECT DISTINCT 
    TRIM(Category), 
    TRIM(Sub_Category)
FROM staging_sales
WHERE Category IS NOT NULL AND Sub_Category IS NOT NULL;

    
-- 3. dim_product
CREATE TABLE dim_product (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_key VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    category_id INT,
    FOREIGN KEY (category_id) REFERENCES dim_category(category_id)
);

INSERT INTO dim_product (product_key, product_name, category_id)
SELECT 
    s.Product_ID,
    s.Product_Name,
    c.category_id
FROM staging_sales s
JOIN dim_category c 
    ON TRIM(s.Category) = TRIM(c.main_category)
    AND TRIM(s.Sub_Category) = TRIM(c.sub_category)
GROUP BY s.Product_ID, s.Product_Name, c.category_id;

-- 4. dim_location
CREATE TABLE dim_location (
    location_id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    postal_code VARCHAR(20) NOT NULL,
    country VARCHAR(50) NOT NULL,
    region VARCHAR(100) NOT NULL
);

INSERT INTO dim_location (city, postal_code, country, region)
SELECT DISTINCT city, postal_code, country, region
FROM staging_sales;

-- 5. dim_order_mode
CREATE TABLE dim_order_mode (
    order_mode_id INT AUTO_INCREMENT PRIMARY KEY,
    order_mode_name VARCHAR(50) NOT NULL UNIQUE
);

INSERT INTO dim_order_mode (order_mode_name)
VALUES ('Online'), ('In-Store');

-- 6. dim_date
CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    year SMALLINT NOT NULL,
    month TINYINT NOT NULL,
    quarter TINYINT NOT NULL,
    month_name VARCHAR(20) NOT NULL
);

-- Populate date dimension (2023-2025)
INSERT INTO dim_date
SELECT 
    date_id,
    YEAR(date_id),
    MONTH(date_id),
    QUARTER(date_id),
    MONTHNAME(date_id)
FROM (
    SELECT DISTINCT order_date AS date_id 
    FROM staging_sales
    UNION
    SELECT '2023-01-01' + INTERVAL seq DAY
    FROM (
        SELECT seq FROM (
            SELECT (a.a + (10 * b.a) + (100 * c.a)) AS seq
            FROM (SELECT 0 AS a UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a
            CROSS JOIN (SELECT 0 AS a UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b
            CROSS JOIN (SELECT 0 AS a UNION SELECT 1 UNION SELECT 2 UNION SELECT 3) c
        ) seqs
        WHERE '2023-01-01' + INTERVAL seq DAY <= '2025-12-31'
    ) date_seq
) all_dates;

# Creating Fact Table
CREATE TABLE fact_sales (
    sales_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    location_id INT,
    date_id DATE,
    order_mode_id INT,
    total_sales DECIMAL(12,2) NOT NULL,
    total_cost DECIMAL(12,2) NOT NULL,
    profit DECIMAL(12,2) NOT NULL,
    discount_amount DECIMAL(12,2) NOT NULL,
    quantity INT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (order_mode_id) REFERENCES dim_order_mode(order_mode_id)
);

INSERT INTO fact_sales (
    customer_id, product_id, location_id, date_id, order_mode_id,
    total_sales, total_cost, profit, discount_amount, quantity
)
SELECT
    c.customer_id,
    p.product_id,
    l.location_id,
    s.order_date,
    om.order_mode_id,
    s.sales,
    s.cost_price * s.quantity AS total_cost,
    (s.sales * (1 - s.Discount)) - (s.cost_price * s.quantity) AS profit,
    s.sales * s.discount AS discount_amount,
    s.quantity
FROM staging_sales s
JOIN dim_customer c ON s.customer_id = c.customer_key
JOIN dim_product p ON s.product_id = p.product_key
JOIN dim_location l ON s.postal_code = l.postal_code AND s.city = l.city
JOIN dim_order_mode om ON s.order_mode = om.order_mode_name;

SELECT
  specific_discount_rate AS discount_rate,
  AVG(profit) AS avg_profit,
  SUM(total_sales) AS total_revenue,
  COUNT(*) AS order_count
FROM CalculatedRates
GROUP BY
  specific_discount_rate;

# Creating views

-- Product Seasonality Trends
CREATE OR REPLACE VIEW vw_product_seasonality AS
SELECT
    dd.year,
    dd.month_name,
    MONTH(dd.date_id) AS month_number, -- This is the new column
    dc.main_category,
    SUM(fs.quantity) AS total_quantity,
    SUM(fs.total_sales) AS total_revenue
FROM
    fact_sales fs
    JOIN dim_date dd ON fs.date_id = dd.date_id
    JOIN dim_product dp ON fs.product_id = dp.product_id
    JOIN dim_category dc ON dp.category_id = dc.category_id
GROUP BY
    dd.year,
    dd.month_name,
    MONTH(dd.date_id), 
    dc.main_category;

--  Discount Impact Analysis
CREATE OR REPLACE VIEW vw_discount_impact_analysis AS
WITH
  -- This CTE pre-calculates the discount rate for every single sale
  CalculatedRates AS (
    SELECT
      profit,
      total_sales,
      (discount_amount / NULLIF(total_sales, 0)) AS specific_discount_rate
    FROM
      fact_sales
  )
-- Now we group the results from the CTE
SELECT
  specific_discount_rate AS discount_rate,
  AVG(profit) AS avg_profit,
  SUM(total_sales) AS total_revenue,
  COUNT(*) AS order_count
FROM
  CalculatedRates
WHERE
  specific_discount_rate IS NOT NULL
GROUP BY
  specific_discount_rate;



-- Channel Margin Report
CREATE VIEW vw_channel_margin_report AS
SELECT
    dom.order_mode_name AS channel,
    SUM(fs.total_sales) AS total_revenue,
    SUM(fs.profit) AS total_profit,
    ROUND((SUM(fs.profit) / SUM(fs.total_sales)) * 100, 2) AS profit_margin
FROM fact_sales fs
JOIN dim_order_mode dom ON fs.order_mode_id = dom.order_mode_id
GROUP BY dom.order_mode_name;

CREATE OR REPLACE VIEW vw_channel_margin_report AS
SELECT
    dom.order_mode_name AS channel,
    SUM(fs.total_sales) AS total_revenue,
    SUM(fs.profit) AS total_profit,
    ROUND((SUM(fs.profit) / NULLIF(SUM(fs.total_sales), 0)) * 100, 2) AS profit_margin
FROM fact_sales fs
JOIN dim_order_mode dom ON fs.order_mode_id = dom.order_mode_id
GROUP BY dom.order_mode_name;

--  Region Category Rankings
CREATE VIEW vw_region_category_rankings AS
SELECT
    dl.region,
    dc.main_category,
    RANK() OVER (
        PARTITION BY dl.region 
        ORDER BY SUM(fs.profit) DESC
    ) AS profit_rank,
    SUM(fs.profit) AS total_profit
FROM fact_sales fs
JOIN dim_location dl ON fs.location_id = dl.location_id
JOIN dim_product dp ON fs.product_id = dp.product_id
JOIN dim_category dc ON dp.category_id = dc.category_id
GROUP BY dl.region, dc.main_category;

-- Top 10 Customers (For Dashboard)
CREATE VIEW vw_top_customers AS
    SELECT 
        dc.customer_key, SUM(fs.profit) AS total_profit
    FROM
        fact_sales fs
            JOIN
        dim_customer dc ON fs.customer_id = dc.customer_id
    GROUP BY dc.customer_key
    ORDER BY total_profit DESC
    LIMIT 10;


-- Check view row counts
SELECT 
    'product_seasonality' AS view, COUNT(*) FROM vw_product_seasonality
UNION ALL
SELECT 'discount_impact', COUNT(*) FROM vw_discount_impact_analysis
UNION ALL
SELECT 'customer_patterns', COUNT(*) FROM vw_customer_order_patterns
UNION ALL
SELECT 'channel_margin', COUNT(*) FROM vw_channel_margin_report
UNION ALL
SELECT 'region_rankings', COUNT(*) FROM vw_region_category_rankings
UNION ALL
SELECT 'top_customers', COUNT(*) FROM vw_top_customers;

-- Sample top customers
SELECT * FROM vw_top_customers LIMIT 5;

-- Check discount-profit correlation
SELECT * FROM vw_discount_impact_analysis
WHERE discount_rate BETWEEN 10 AND 20;

# 5 reusable SQL queries

-- Query 1: Top 5 Most and Least Profitable Sub-Categories
(SELECT
    c.main_category,
    c.sub_category,
    SUM(f.profit) AS total_profit
FROM
    fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    JOIN dim_category c ON p.category_id = c.category_id
GROUP BY
    c.main_category, c.sub_category
ORDER BY
    total_profit DESC
LIMIT 5)

UNION ALL

(SELECT
    c.main_category,
    c.sub_category,
    SUM(f.profit) AS total_profit
FROM
    fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    JOIN dim_category c ON p.category_id = c.category_id
GROUP BY
    c.main_category, c.sub_category
ORDER BY
    total_profit ASC
LIMIT 5);

-- Query 2: Quarterly Sales and Profit Performance by Region
SELECT
    d.year,
    d.quarter,
    l.region,
    SUM(f.total_sales) AS quarterly_revenue,
    SUM(f.profit) AS quarterly_profit,
    (SUM(f.profit) / NULLIF(SUM(f.total_sales), 0)) * 100 AS profit_margin_pct
FROM
    fact_sales f
    JOIN dim_date d ON f.date_id = d.date_id
    JOIN dim_location l ON f.location_id = l.location_id
WHERE
    l.region IN ('London', 'Scotland', 'North West') -- Example for key regions
GROUP BY
    d.year, d.quarter, l.region
ORDER BY
    l.region, d.year, d.quarter;
    
-- Query 3: Overall Profitability by Sales Channel (Online vs. In-Store)
-- This query provides a clear comparison of which sales channel is more profitable for the business,
-- helping to guide decisions on marketing spend and operational focus.

SELECT
    om.order_mode_name AS sales_channel,
    SUM(fs.profit) AS total_profit,
    SUM(fs.total_sales) AS total_revenue,
    COUNT(fs.sales_id) AS number_of_orders
FROM
    fact_sales AS fs
JOIN
    dim_order_mode AS om ON fs.order_mode_id = om.order_mode_id
GROUP BY
    om.order_mode_name
ORDER BY
    total_profit DESC;
    
-- Query 4: Customer Purchase Frequency and Lifetime Value
-- This query identifies the most valuable repeat customers based on their total profit contribution.
-- It's essential for creating targeted marketing campaigns and loyalty programs.

SELECT
    c.customer_key,
    c.segment,
    COUNT(fs.sales_id) AS number_of_orders,
    SUM(fs.total_sales) AS total_revenue_generated,
    SUM(fs.profit) AS total_profit_generated,
    AVG(fs.total_sales) AS average_order_value
FROM
    fact_sales AS fs
JOIN
    dim_customer AS c ON fs.customer_id = c.customer_id 
GROUP BY
    c.customer_key,
    c.segment
HAVING
    COUNT(fs.sales_id) > 1 -- Filter for repeat customers only
ORDER BY
    total_profit_generated DESC
LIMIT 25; -- Show the top 25 most profitable repeat customers

-- Query 5: Identifying Top Money-Losing Products with High Discounts
-- This query is highly actionable, creating a "hit list" of specific products
-- that are losing the company money when sold with a steep discount.

SELECT
    p.product_name,
    c.main_category,
    l.region,
    (f.discount_amount / f.total_sales) * 100 AS actual_discount_pct,
    f.profit
FROM
    fact_sales f
JOIN                                         -- This JOIN was missing
    dim_product p ON f.product_id = p.product_id
JOIN
    dim_category c ON p.category_id = c.category_id
JOIN
    dim_location l ON f.location_id = l.location_id
WHERE
    f.profit < 0
    AND (f.discount_amount / f.total_sales) > 0.20 -- Filter for discounts > 20%
ORDER BY
    f.profit ASC -- Order by the most negative profit first
LIMIT 20;
