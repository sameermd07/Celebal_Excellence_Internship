# E-Commerce Order Analytics System

## Project Overview

This project simulates a real-world e-commerce data engineering workflow. The raw data contains missing values, invalid formats, and inconsistent records. The data is cleaned using Python, analyzed using SQL, and summarized through Python and SQL integration.

---

## Technologies Used

- Python
- Pandas
- SQL
- Databricks
- Spark SQL

---

## Project Phases

### Phase 1 - Data Generation

Generated realistic fake datasets:

- customers.csv
- products.csv
- orders.csv
- order_items.csv

Intentional data quality issues were added to simulate real-world scenarios.

---

### Phase 2 - Data Cleaning

Implemented:

- clean_orders()
- clean_products()
- validate_emails()
- check_referential_integrity()

Generated cleaned CSV files and an issues report.

---

### Phase 3 - SQL Analysis

Solved 16 SQL problems including:

- Revenue Analysis
- Customer Analysis
- Window Functions
- CTEs
- Ranking
- Cohort Analysis
- Self Join

---

### Phase 4 - Python + SQL Integration

Generated reports containing:

- Total Orders
- Revenue
- Unique Customers
- Top Products
- Previous Period Comparison

---

### Phase 5 - Edge Case Handling

Validated:

- Invalid Order IDs
- Invalid Discounts
- Zero Quantity
- Future Order Dates

---

## Outcome

The project demonstrates an end-to-end data engineering workflow involving data generation, cleaning, SQL analysis, reporting, and validation.