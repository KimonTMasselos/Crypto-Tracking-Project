# 🚀 Crypto Tracking Project

This project fetches cryptocurrency data from the **CoinGecko API** for the top 100 coins and builds a modern data pipeline using **Apache Airflow**, **Docker**, **PySpark**, and **Amazon S3**.  
Data is processed following the **Medallion Architecture** (Bronze → Silver → Gold) and stored as partitioned **Iceberg tables**, ready for analytics and visualization.

---

## 📌 Overview

- **Data Source**: [CoinGecko API](https://www.coingecko.com/en/api)  
- **Orchestration**: Apache Airflow  
- **Containerization**: Docker & docker-compose  
- **Processing Engine**: PySpark  
- **Storage**: Amazon S3 (partitioned Iceberg tables)  
- **Architecture**: Medallion (Bronze, Silver, Gold)  

### Current Tables

- **Dimension Tables**
  - `dim_coin_scd` → coin metadata (name, description, market_cap_rank)  
  - `dim_coin_category_scd` → coin category metadata  

- **Fact Table**
  - `fct_coin_price` → daily coin metrics (price, volume, market cap)

### Future Work

- Add a **Gold layer**:
  - New fact table with rolling averages & advanced metrics  
  - Designed for BI tools like Tableau  
- Implement **data quality checks** & **unit tests**  
- Add **CI/CD pipeline** for automated testing, deployments, and quality assurance  
- Expand orchestration & monitoring features  

---

## 🏗️ Architecture

```text
        ┌───────────────┐
        │   CoinGecko   │
        │     API       │
        └───────┬───────┘
                │
         Airflow DAGs
                │
        ┌───────▼───────┐
        │    Bronze     │  → Raw ingestion (JSON, API extracts)
        └───────┬───────┘
                │ PySpark
        ┌───────▼───────┐
        │    Silver     │  → Cleaned & structured Iceberg tables
        └───────┬───────┘
                │ PySpark
        ┌───────▼───────┐
        │     Gold      │  → Aggregated metrics, rolling averages
        └───────────────┘
