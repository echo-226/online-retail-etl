# Online Retail ETL Project

## Overview

This project is an **ETL (Extract, Transform, Load)** pipeline built with **Apache Spark** and **PySpark** to process online retail transaction data.
It cleans raw data, transforms it into meaningful business metrics, and outputs datasets for analysis and reporting.

Data Source:
UCI Machine Learning Repository – Online Retail Dataset
The dataset contains transactional data from a UK-based online retail store between 1/12/2010 and 9/12/2011.

---

## Project Structure

```
online-retail-etl/
├── configs/
│   └── retail_etl_config.json
├── dependencies/
│   ├── spark.py
│   └── logging.py
├── jobs/
│   └── retail_etl_job.py
├── data/
│   └── online_retail.csv
├── output/
│   └── cleaned
│   └── customers
│   └── monthly_trend
│   └── products
├── Makefile
└── README.md

```

---

## Requirements

* Python **3.7+**
* Apache Spark **3.x** (installed and configured)
* Java **8+**
* Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Quick Start

### 1. Run the ETL job

```bash
python jobs/retail_etl_job.py
```

The ETL job will read from `data/online_retail.csv`, clean and transform the data, and produce the following datasets:

* **cleaned** – Cleaned transaction records
* **customers** – Customer-level metrics (total orders, total spent, return rate)
* **products** – Product-level metrics (sales quantity, revenue)
* **monthly\_trend** – Monthly sales trend

---

## Key Features

* **Data Cleaning**

  * Remove invalid orders (returns, missing customer IDs)
  * Normalize date fields and split into **year** and **month**

* **Data Transformation**

  * Calculate **Total Orders**, **Total Spent**, and **Return Rate** per customer
  * Aggregate sales metrics per product
  * Generate monthly sales trend reports

---

## Sample Data

```csv
InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
563112,22607,WOODEN ROUNDERS GARDEN SET,2,2011/8/12 11:09,9.95,14849,United Kingdom
```

> Note:
>
> * `InvoiceDate` is stored as a string (e.g., `2010/10/12` or `2010/9/11`)
> * The pipeline splits the date string by `/` to extract **year** and **month**.

---

## Notes

* When filtering returns, use correct boolean conditions, e.g.:

```python
df.filter(col("Quantity") >= 0)
```

* Spark must be properly configured in your environment (`spark-submit` and `pyspark` available).
* The pipeline is designed to handle inconsistent date formats in the source file.

---

## Contributing

Contributions are welcome!
If you submit a pull request, please ensure:

* Your code includes unit tests
* All existing tests pass
