# Ecommerce-Analytics-Pipeline

> End-to-End ETL Pipeline built on Databricks Community Edition using Medallion Architecture (Bronze → Silver → Gold).

---

## 📌 Project Overview

This project demonstrates a complete Data Engineering solution for an e-commerce business. It ingests raw transactional data, cleans and enriches it, and generates business-ready analytical tables for dashboards and reporting.

---

## 🏗️ Architecture (Medallion)

| Layer | Description |
| :--- | :--- |
| **Bronze** | Raw data ingestion from CSV files (Transactions, Customers, Products, Stores, Promotions, Feedback) → Stored as Delta Tables. |
| **Silver** | Data cleaning, deduplication, type casting, date formatting, and joining dimensions (Customer, Product, Store) to create an enriched Fact Table. |
| **Gold** | Business aggregations for reporting: Daily Revenue, Category Sales, Region Performance, Top Products, RFM Analysis, Product Sentiment, Promotion Impact, and Fraud Detection. |

---

## 🛠️ Tech Stack

- **Databricks** (PySpark, Spark SQL)
- **Delta Lake** (ACID transactions, Time Travel)
- **Google BigQuery** (Data Warehouse)
- **Looker Studio** (Dashboard & Visualization)

---

## 📊 Key Features

- ✅ **Data Quality (DQ) Checks**: Automated validation for nulls and data integrity.
- ✅ **RFM Segmentation**: Customer segmentation based on Recency, Frequency, and Monetary value.
- ✅ **Fraud Detection**: Identifies suspicious transactions (same customer, different stores, high value, short time window).
- ✅ **Promotion Effectiveness**: Analyzes which discounts generated the most revenue.
- ✅ **Product Sentiment**: Aggregates customer ratings to find top-performing products.

---

## 📁 Project Structure

