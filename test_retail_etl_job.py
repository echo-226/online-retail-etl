import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col
from dependencies.spark import start_spark
from jobs.retail_etl_job import transform_data


@pytest.fixture(scope="module")
def spark():
    """
    Setup a SparkSession for testing using the start_spark function.
    """
    # 使用 start_spark 初始化 Spark 环境
    spark, config, log = start_spark(
        app_name="test_online_retail_etl",
        master="local[*]",
        files=[],
        spark_config={"spark.driver.extraJavaOptions": "-Dlog4j.configuration=file:log4j.properties"}
    )
    yield spark
    spark.stop()


def test_transform_data(spark):
    # === Input Data ===
    # input_data = [
    #     {"InvoiceNo": "537001", "StockCode": "85123A", "Description": "Product A", "Quantity": 5,
    #     "InvoiceDate": "2023/01/15", "UnitPrice": 1.5, "CustomerID": 12345, "Country": "UK"},
    #     {"InvoiceNo": "C537002", "StockCode": "71053", "Description": "Product B", "Quantity": -1,
    #     "InvoiceDate": "2023/01/15", "UnitPrice": 2.5, "CustomerID": 12345, "Country": "UK"}
    # ]

    input_data = [
        {"InvoiceNo": "537001", "StockCode": "85123A", "Description": "Product A", "Quantity": 5,
        "InvoiceDate": "2023/01/15", "UnitPrice": 1.5, "CustomerID": 12345, "Country": "UK"},
        {"InvoiceNo": "C537002", "StockCode": "71053", "Description": "Product B", "Quantity": -1,
        "InvoiceDate": "2023/01/15", "UnitPrice": 2.5, "CustomerID": 12345, "Country": "UK"},
        {"InvoiceNo": "537003", "StockCode": "84406B", "Description": "Product C", "Quantity": 2,
        "InvoiceDate": "2023/02/15", "UnitPrice": 3.0, "CustomerID": None, "Country": "Germany"},
        {"InvoiceNo": "537004", "StockCode": "84406B", "Description": "Product C", "Quantity": 10,
        "InvoiceDate": "2023/02/15", "UnitPrice": 3.0, "CustomerID": 12346, "Country": "Germany"}
    ]
    df_input = spark.createDataFrame(input_data)

    # === Call transform_data ===
    df_output = transform_data(df_input)

    # === Expected Output ===
    # Cleaned Data
    # cleaned_expected = [
    #     Row(InvoiceNo="537001", StockCode="85123A", Description="Product A", Quantity=5, InvoiceDate="2023/01/15",
    #         UnitPrice=1.5, CustomerID=12345, Country="UK", TotalPrice=7.5, InvoiceYear="2023", InvoiceMonth="01"),
    # ]

    cleaned_expected = [
        Row(InvoiceNo="537001", StockCode="85123A", Description="Product A", Quantity=5, InvoiceDate="2023/01/15",
            UnitPrice=1.5, CustomerID=12345, Country="UK", TotalPrice=7.5, InvoiceYear="2023", InvoiceMonth="01"),
        Row(InvoiceNo="537004", StockCode="84406B", Description="Product C", Quantity=10, InvoiceDate="2023/02/15",
            UnitPrice=3.0, CustomerID=12346, Country="Germany", TotalPrice=30.0, InvoiceYear="2023", InvoiceMonth="02"),
    ]
    df_cleaned_expected = spark.createDataFrame(cleaned_expected)

    # Validate cleaned data
    df_cleaned = df_output["cleaned"]
    assert df_cleaned.count() == df_cleaned_expected.count()
    assert df_cleaned.where(col("CustomerID").isNull()).count() == 0
    assert df_cleaned.filter(col("Quantity") < 0).count() == 0

    # Customer Aggregations
    customers_expected = [
        Row(CustomerID=12345, TotalOrders=1, AvgUnitPrice=1.5, TotalSpent=7.5, ReturnCount=1, ReturnRate=0.5),
        Row(CustomerID=12346, TotalOrders=1, AvgUnitPrice=3.0, TotalSpent=30.0, ReturnCount=0, ReturnRate=0.0),
    ]
    df_customers_expected = spark.createDataFrame(customers_expected)

    # Validate customer aggregation
    df_customers = df_output["customers"]
    assert df_customers.count() == df_customers_expected.count()
    assert df_customers.filter((col("CustomerID") == 12345) & (col("ReturnRate") == 0.5)).count() == 1

    # Product Aggregations
    products_expected = [
        Row(StockCode="84406B", Description="Product C", TotalSold=10, Revenue=30.0),
        Row(StockCode="85123A", Description="Product A", TotalSold=5, Revenue=7.5),
    ]
    df_products_expected = spark.createDataFrame(products_expected)

    # Validate product aggregation
    df_products = df_output["products"]
    assert df_products.count() == df_products_expected.count()
    assert df_products.filter((col("StockCode") == "84406B") & (col("TotalSold") == 10)).count() == 1

    # Monthly Trend
    monthly_trend_expected = [
        Row(InvoiceYear="2023", InvoiceMonth="01", Description="Product A", MonthlyQty=5, MonthlyRevenue=7.5),
        Row(InvoiceYear="2023", InvoiceMonth="02", Description="Product C", MonthlyQty=10, MonthlyRevenue=30.0),
    ]
    df_monthly_trend_expected = spark.createDataFrame(monthly_trend_expected)

    # Validate monthly trend
    df_monthly_trend = df_output["monthly_trend"]
    assert df_monthly_trend.count() == df_monthly_trend_expected.count()
    assert df_monthly_trend.filter((col("InvoiceYear") == "2023") & (col("InvoiceMonth") == "02")).count() == 1


if __name__ == "__main__":
    import pytest
    pytest.main(["-v", "tests/test_retail_etl_job.py"])