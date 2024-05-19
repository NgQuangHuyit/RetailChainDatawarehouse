from pyspark.sql.functions import concat, col, lit
from lib.utils import get_spark_app_config
from lib.scdhandler import SCDHandler
from lib.constant import SCHEMA, EOW_DATE, DATE_FORMAT
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Get dim employee") \
        .config(conf=get_spark_app_config()) \
        .enableHiveSupport() \
        .getOrCreate()

    employee_df = spark.sql("select * from bronze.employee")

    branch_df = spark.sql("select * from bronze.branch")

    dim_employee_current = employee_df.join(branch_df, employee_df.branchID == branch_df.branchID, "left") \
        .withColumnRenamed("employeeID", "employee_id") \
        .withColumnRenamed("hireDate", "hire_date") \
        .withColumnRenamed("branchName", "branch_name") \
        .withColumn("fullname", concat(col("firstName"), lit(" "), col("lastName"))) \
        .select("employee_id", "fullname", "position",
                col("employee.phone").alias("phone"), col("employee.email").alias("email"),
                "branch_name", "hire_date")

    dim_employee_history = spark.sql(f"select * from {SCHEMA}.dim_employee")

    scdHandler = SCDHandler(EOW_DATE, DATE_FORMAT)
    scdHandler.enrich_to_scd2(
        dim_employee_current,
        dim_employee_history,
        ["position", "phone", "email", "branch_name"],
        "employee_id",
        "employee_sk").createOrReplaceTempView("dim_employee_final")

    spark.sql(f"select * from dim_employee_final") \
        .write \
        .mode("overwrite") \
        .insertInto(f"{SCHEMA}.dim_employee")
    spark.stop()
