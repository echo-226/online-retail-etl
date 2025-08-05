from dependencies.spark import start_spark
from pyspark.sql.functions import col, split, count, avg, sum as _sum

# /home/lwt/projects/online-retail-etl/jobs/retail_etal_job.py

def main():
    spark, config, log = start_spark(
        app_name = "online_retail_etl",
        files = ["configs/retail_etl_config.json"]
    )
    log.warn("ETL job started.")

    df_raw = extract_data(spark, config)

    df_transformed = transform_data(df_raw)

    load_data(df_transformed, config)

    log.warn("ETL job finished.")

    spark.stop()


def extract_data(spark,config):
    """
    Extract data from Parquet file
    :param spark: SparkSession
    :param config: dict with 'input_data' 
    :return: Spark DataFrame
    """
    input_path = config['input_data']
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

    return df

def transform_data(df):
    
    # Remove return records (InvoiceNo starts with 'C')
    df_clean = df.filter(~col("InvoiceNo").startswith('C'))
    df_clean = df_clean.filter(col("CustomerID").isNotNull())
    df_clean = df_clean.filter(col("Quantity") >= 0)


    # Derive new columns: total price and invoice month
    df_clean = df_clean.withColumn("TotalPrice",col("Quantity") * col("UnitPrice"))
    df_clean = df_clean.withColumn("InvoiceYear", split(col("InvoiceDate"), "/").getItem(0)) \
       .withColumn("InvoiceMonth", split(col("InvoiceDate"), "/").getItem(1))
    

    # === Customer-level aggregations ===
    df_customers = (
        df_clean.groupBy("CustomerID")
        .agg(
            count("InvoiceNo").alias("TotalOrders"),
            avg("UnitPrice").alias("AvgUnitPrice"),
            _sum("TotalPrice").alias("TotalSpent")
        )
    )

    # Compute return rate from original data
    df_return = (
        df.filter(col("InvoiceNo").startswith("C"))
        .groupBy("CustomerID")
        .count()
        .withColumnRenamed("count", "ReturnCount")
    )
    df_customers = (
        df_customers.join(df_return, on = "CustomerID", how = "left")
        .fillna(0)
        .withColumn("ReturnRate", col("ReturnCount")/(col("ReturnCount") + col("TotalOrders")))
    )

    # === Product-level aggregations ===
    df_products = (
        df_clean.groupBy("StockCode", "Description")
        .agg(
            _sum("Quantity").alias("TotalSold"),
            _sum("TotalPrice").alias("Revenue")
        )
        .orderBy(col("TotalSold").desc())
    )

    # === Monthly sales trend per product ===
    df_monthly_trend = (
        df_clean.groupBy("InvoiceYear", "InvoiceMonth", "Description")
        .agg(
            _sum("Quantity").alias("MonthlyQty"),
            _sum("TotalPrice").alias("MonthlyRevenue")
        )
        .orderBy("InvoiceYear", "InvoiceMonth", "MonthlyRevenue", ascending=[True, False])
    )

    return {
        "cleaned": df_clean,
        "customers": df_customers,
        "products": df_products,
        "monthly_trend": df_monthly_trend
    }


def load_data(dfs, config):
    output_path = config['output_data']

    for name, df in dfs.items():
        path = f"{output_path}/{name}"
        df.coalesce(1).write.csv(path, mode='overwrite', header=True)
        

    
if __name__ == "__main__":
    
    main()



