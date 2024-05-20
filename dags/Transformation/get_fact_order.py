from pyspark.sql import SparkSession
from lib.utils import get_spark_app_config
from lib.logger import Logger
from pyspark.sql.functions import col, date_format

if __name__ == "__main__":
    logger = Logger("GetFactSale")

    logger.info("Create Spark Session")
    spark = SparkSession.builder \
        .appName("Get fact order") \
        .config(conf=get_spark_app_config()) \
        .enableHiveSupport() \
        .getOrCreate()

    order_df = spark.sql("select * from bronze.saleorder")
    order_detail_df = spark.sql("select * from bronze.orderdetail")

    fct_sale = order_df.join(order_detail_df, order_df.orderID == order_detail_df.orderID, "left") \
        .withColumn("date_key", date_format(col("orderDate"), "yyyyMMdd").cast("int")) \
        .select(
        col("saleorder.orderID").alias("order_id"),
        col("promotionID").alias("promotion_id"),
        col("customerID").alias("customer_id"),
        col("employeeID").alias("employee_id"),
        col("branchID").alias("branch_id"),
        col("productID").alias("product_id"),
        col("quantity"),
        col("discount"),
        col("unitPrice").alias("unit_price"),
        col("subTotal").alias("sub_total"),
        col("date_key")
    )

    dim_employee_sk_reference = spark.sql("""
        select employee_sk, employee_id 
        from silver.dim_employee
        where current_flag = 1
    """)

    dim_customer_sk_reference = spark.sql("""
        select customer_sk, customer_id
        from silver.dim_customer
        where current_flag = 1
    """)

    dim_product_sk_reference = spark.sql("""
        select product_sk, product_id
        from silver.dim_product
        where current_flag = 1
    """)

    dim_branch_sk_reference = spark.sql("""
        select branch_sk, branch_id
        from silver.dim_branch
        where current_flag = 1
    """)

    dim_promotion_sk_reference = spark.sql("""
        select promotion_sk, promotion_id
        from silver.dim_promotion
    """)

    fct_sale = fct_sale.join(dim_employee_sk_reference, fct_sale.employee_id == dim_employee_sk_reference.employee_id, "left") \
        .join(dim_customer_sk_reference, fct_sale.customer_id == dim_customer_sk_reference.customer_id, "left") \
        .join(dim_product_sk_reference, fct_sale.product_id == dim_product_sk_reference.product_id, "left") \
        .join(dim_branch_sk_reference, fct_sale.branch_id == dim_branch_sk_reference.branch_id, "left") \
        .join(dim_promotion_sk_reference, fct_sale.promotion_id == dim_promotion_sk_reference.promotion_id, "left") \
        .select(
        col("date_key"),
        col("employee_sk"),
        col("customer_sk"),
        col("product_sk"),
        col("branch_sk"),
        col("promotion_sk"),
        col("quantity"),
        col("discount"),
        col("unit_price"),
        col("sub_total")
    )

    fct_sale.write \
        .mode("append") \
        .insertInto("silver.fct_sale")



