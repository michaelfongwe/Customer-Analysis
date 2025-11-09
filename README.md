# End-to-End Customer Feedback Analytics

KoboToolbox → Python ETL → PostgreSQL → Power BI

This project delivers a full workflow that collects customer feedback using KoboToolbox, processes it with Python, loads it into PostgreSQL, and builds analytical dashboards in Power BI.

# Objective

Provide an end-to-end data solution for analyzing customer satisfaction, product performance, and operational efficiency.

# Business Questions

Which product categories generate the highest revenue per branch?

Which products receive the most customer complaints?

What causes low customer satisfaction?

How do sales trends change across seasons or promotions?

What improves product availability and customer experience?

# Architecture
KoboToolbox (Data Collection)
        ↓
Python ETL (main.py)
 - Extract Kobo CSV
 - Clean and standardize columns
 - Create schema/table
 - Load to PostgreSQL
        ↓
PostgreSQL Data Warehouse
        ↓
Power BI (Modeling & Dashboards)

# Tech Stack

KoboToolbox

Python (pandas, requests, psycopg2, python-dotenv)

PostgreSQL

Power BI

# Environment Setup
# .env file

# Install dependencies
pip install pandas requests psycopg2-binary python-dotenv

# Run the ETL
python main.py

# ETL Summary

Authenticated CSV extraction from KoboToolbox.

SQL-compliant column name cleanup.

Schema creation (MyWork).

Dynamic table creation.

Full load into PostgreSQL landing table.

# Power BI Modeling

Connect to PostgreSQL from Power BI.

Assign correct data types.

Build dimensions: Date, Product, Branch, Complaint Type, Satisfaction Reason.

Create fact table and DAX measures.

Visualize revenue, complaints, satisfaction drivers, seasonality, and availability.

# Data Warehouse Notes

Landing table: MyWork.customer_feedback

Ready for extension into a full star schema.

# Future Enhancements

Incremental loading

Scheduled automation

Data quality validation

Star schema restructuring

Docker deployment