# RDAMP-Dimensional-Model-PowerBI
## Author

*   **Name:** Tongai Zinaka

# Ace Superstore: Dimensional Modeling & Power BI Reporting

## Project Overview

This project is the second phase of the Ace Superstore sales analysis initiative. Following an initial exploratory data analysis (Task 1), this phase focuses on building a robust, scalable, and query-optimized reporting system. The primary goal was to transform the cleaned dataset into a structured **Star Schema** dimensional model using SQL and then develop an interactive dashboard in Power BI to uncover strategic business insights.

> **Problem Statement:** "Following the foundational analysis in Task 1, Ace Superstore now aims to build a structured, query-optimized reporting system. Your responsibility in this task is twofold: 1. Design and implement a star schema using SQL. 2. Create SQL views from your schema and connect them directly to Power BI to produce advanced interactive dashboards."

---

## 📈 Executive Summary: Key Insights & Recommendations

This analysis has uncovered a **critical profitability crisis** at Ace Superstore. While revenue is being generated, the current pricing and discount strategies are unsustainable, leading to significant financial losses across all operational regions.

### Key Findings:

*   **Widespread Regional Losses:** The business is unprofitable in every single region, accumulating a total net loss of **£-788,871.96**. The "Regional Profitability Overview" chart clearly visualizes this alarming trend, with East Midlands showing the most significant losses.
*   **Ineffective Discount Strategy:** The "Discount vs. Profit Analysis" scatter plot reveals a strong negative correlation. As the discount rate increases, average profit plummets. Many sales, particularly those with high discounts, result in a direct financial loss.
*   **Extreme Negative Profit Margins:** The average profit margin across both In-Store and Online channels is approximately **-260%**. This indicates a severe structural issue with the company's cost of goods, pricing, and discount policies.
*   **Identification of Top Customers:** Despite overall losses, we have identified a cohort of high-value customers. The "Top 10 Customers by Profit Contribution" chart highlights individuals like `QC056257` who, while still part of a loss-making system, represent the most valuable segment.

### Strategic Recommendations:

1.  **Immediate Pricing & Discount Review:** Urgently conduct a full review of the discounting strategy. High-discount promotions (>20%) should be halted immediately on products identified as money-losers.
2.  **Focus on Profit-Driving Products:** Although the overall picture is negative, the "Category Ranking by Region" matrix allows for a granular analysis of the *least unprofitable* products. These should be prioritized in marketing and inventory.
3.  **Enhance Customer Loyalty Programs:** The top-performing customers should be nurtured. A targeted loyalty program could increase their purchase frequency and value, providing a stable, less unprofitable revenue stream.
4.  **Channel-Specific Strategy:** Investigate why both Online and In-Store channels are performing so poorly. Analyze operational costs, return rates, and channel-specific customer behaviors to tailor a recovery strategy for each.

---

## 📊 Power BI Interactive Dashboard

The interactive dashboard provides a multi-faceted view of Ace Superstore's performance.

### Main Dashboard View
<img width="1296" height="728" alt="image" src="https://github.com/user-attachments/assets/93ee33a3-8607-4efa-ad31-924016e3e4b7" />
<img width="1292" height="733" alt="image" src="https://github.com/user-attachments/assets/f7a737dc-ee80-40d2-90c1-bdc65737e8c5" />



### Detailed Visualizations

<details>
<summary><strong>Click to expand and view detailed visuals</strong></summary>

#### Product Seasonality Trends
This heatmap shows the revenue distribution across product categories and months, highlighting that categories like **Kitchen**, **Clothing**, and **Health** generate the most revenue, particularly in Q4.

<img width="1287" height="728" alt="image" src="https://github.com/user-attachments/assets/dccd2391-66c1-48f1-8550-8b9fa2f0eb7a" />



---

#### Discount vs. Profit Analysis
This scatter plot demonstrates the direct, negative impact of discounts on profitability. The downward-sloping trend line proves that higher discounts lead to greater financial losses.

<img width="1286" height="729" alt="image" src="https://github.com/user-attachments/assets/2f0a087a-5383-44c1-b4c9-e4e5c91104d6" />



---

#### Customer & Channel Performance
This section analyzes customer value and channel effectiveness. We can see our top 10 most valuable customers and compare the performance of In-Store vs. Online channels.

<img width="1285" height="729" alt="image" src="https://github.com/user-attachments/assets/aa578a2e-23b2-4831-85b2-4989a2627a2e" />



---

#### Regional & Category Profitability
This section provides a granular breakdown of profitability, ranking regions and the categories within them. It serves as a diagnostic tool to identify the most and least profitable areas of the business.

<img width="1290" height="733" alt="image" src="https://github.com/user-attachments/assets/ed87e231-a8b5-4da8-99ac-c0ff0147cfbe" />



</details>

---

## 🛠️ Technical Implementation

### Data Model & Schema

The foundation of this report is a robust **Star Schema** implemented in a MySQL database. This schema separates descriptive attributes (dimensions) from quantitative measures (facts), which optimizes query performance and simplifies analytical reporting.

#### Schema Diagram
<img width="1287" height="788" alt="image" src="https://github.com/user-attachments/assets/4be96474-6675-4893-9065-4726b9fb9693" />



#### Table & View Descriptions

| Table/View Name               | Type      | Purpose                                                                                             |
| ----------------------------- | --------- | --------------------------------------------------------------------------------------------------- |
| `dim_customer`                | Dimension | Stores unique customer information, including their ID and segment.                                 |
| `dim_product`                 | Dimension | Contains details about each unique product, such as its name and key.                               |
| `dim_category`                | Dimension | Holds the product hierarchy, defining main and sub-categories.                                      |
| `dim_location`                | Dimension | Stores geographical data, including city, region, country, and postal code.                         |
| `dim_date`                    | Dimension | Provides date attributes like year, month, and quarter for time-based analysis.                     |
| `dim_order_mode`              | Dimension | Defines the sales channel (e.g., 'Online' or 'In-Store').                                           |
| `fact_sales`                  | Fact      | The central table containing all transactional data and key measures like `Total_Sales` and `Profit`. |
| `vw_...` (all views)          | View      | Pre-aggregated SQL views designed to power specific visuals in Power BI efficiently.                |

### Setup & Replication Instructions

To replicate this analysis, follow these steps:

#### 1. SQL Database Setup
1.  **Create Database:** Set up a new MySQL database named `ace_database`.
2.  **Load Staging Data:** Create the `staging_sales` table and load the `cleaned_sales.csv` data from Task 1.
3.  **Execute SQL Scripts:** Run the following SQL scripts from the `sql/` folder in the specified order:
    1.  `1_create_tables.sql` - To build the fact and dimension tables.
    2.  `2_populate_dimensions.sql` - To insert data into all `dim_` tables.
    3.  `3_populate_fact_table.sql` - To insert data into `fact_sales` (with the correct profit calculation).
    4.  `4_create_views.sql` - To create all the `vw_` summary views for Power BI.

#### 2. Power BI Connection
1.  **Open Power BI Desktop** and click on "Get Data".
2.  Select **"MySQL database"** from the list of data sources.
3.  Enter the server details (e.g., `localhost`) and the database name (`ace_database`).
4.  In the Navigator window, select all the views (`vw_...`) created in the previous step.
5.  Click **"Load"** to import the data into Power BI and begin building the visuals as described.

---
