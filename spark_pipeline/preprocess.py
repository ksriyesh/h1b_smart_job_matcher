from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace, log1p, min as spark_min, max as spark_max
from pyspark.sql.types import IntegerType
import os
import pandas as pd

def preprocess_new_h1b_data(input_path, output_path):
    spark = SparkSession.builder.appName("H1B Preprocessing").getOrCreate()

    # If input is .xlsx, convert to CSV first
    temp_csv = None
    if input_path.endswith('.xlsx'):
        temp_csv = input_path.replace('.xlsx', '_temp.csv')
        df_xlsx = pd.read_excel(input_path)
        df_xlsx.to_csv(temp_csv, index=False)
        input_path = temp_csv

    print(f"📥 Reading new H1B file from: {input_path}")
    df = spark.read.option("header", True).csv(input_path)

    # Select and clean columns
    df = df.select(
        col("Employer (Petitioner) Name").alias("company"),
        col("Industry (NAICS) Code").alias("industry"),
        col("Petitioner City").alias("city"),
        col("Petitioner State").alias("state"),
        col("Petitioner Zip Code").alias("zip_code"),
        col("Initial Approval").cast(IntegerType()),
        col("Initial Denial").cast(IntegerType()),
        col("Continuing Approval").cast(IntegerType()),
        col("Continuing Denial").cast(IntegerType())
    )

    df = df.withColumn("total_filing", 
        col("Initial Approval") + col("Initial Denial") + col("Continuing Approval") + col("Continuing Denial")
    )

    # Clean text
    df = df.withColumn("company", lower(trim(regexp_replace(col("company"), r"\b(llc|inc|corp|ltd|private|co|dba)\b", ""))))
    df = df.withColumn("city", lower(trim(col("city"))))
    df = df.withColumn("state", lower(trim(col("state"))))
    df = df.withColumn("zip_code", col("zip_code").cast(IntegerType()))

    # Industry mapping
    industry_mapping = {
        '44-45 - Retail Trade': 'retail trade',
        '72 - Accommodation and Food Services': 'accommodation and food services',
        '54 - Professional, Scientific, and Technical Services': 'professional and technical services',
        '52 - Finance and Insurance': 'finance and insurance',
        '62 - Health Care and Social Assistance': 'health care and social assistance',
        '51 - Information': 'information',
        '31-33 - Manufacturing': 'manufacturing',
        '81 - Other Services (except Public Administration)': 'other services',
        '42 - Wholesale Trade': 'wholesale trade',
        '53 - Real Estate and Rental and Leasing': 'real estate and rental',
        '56 - Administrative and Support and Waste Management and Remediation Services': 'administrative and support services',
        '23 - Construction': 'construction',
        '61 - Educational Services': 'educational services',
        '71 - Arts, Entertainment, and Recreation': 'arts and entertainment',
        '92 - Public Administration': 'public administration',
        '11 - Agriculture, Forestry, Fishing and Hunting': 'agriculture and forestry',
        '48-49 - Transportation and Warehousing': 'transportation and warehousing',
        '55 - Management of Companies and Enterprises': 'corporate management',
        '22 - Utilities': 'utilities',
        '21 - Mining, Quarrying, and Oil and Gas Extraction': 'mining and extraction'
    }

    mapping_expr = spark.createDataFrame(list(industry_mapping.items()), ["naics_code", "industry_label"])
    df = df.join(mapping_expr, df.industry == mapping_expr.naics_code, "left").drop("naics_code")
    df = df.withColumnRenamed("industry_label", "industry")

    # Normalize filing score by state
    df = df.withColumn("log_filing", log1p(col("total_filing")))

    window = spark.sql("SELECT DISTINCT state FROM temp_view")  # you could add WindowSpec if needed
    min_max = df.groupBy("state").agg(
        spark_min("log_filing").alias("min_log"),
        spark_max("log_filing").alias("max_log")
    )

    df = df.join(min_max, on="state")
    df = df.withColumn("filing_score", (col("log_filing") - col("min_log")) / (col("max_log") - col("min_log")))

    # Final columns
    df_final = df.select("company", "industry", "city", "state", "zip_code", "total_filing", "filing_score")

    # Write to a single CSV file
    temp_output = os.path.join(output_path, "_temp_output")
    df_final.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_output)
    # Find the actual CSV file Spark wrote
    for fname in os.listdir(temp_output):
        if fname.endswith('.csv'):
            os.replace(os.path.join(temp_output, fname), os.path.join(output_path, "processed_employer_data.csv"))
    # Clean up temp output dir
    import shutil
    shutil.rmtree(temp_output)
    if temp_csv:
        os.remove(temp_csv)
    print(f"✅ Preprocessed data written to: {os.path.join(output_path, 'processed_employer_data.csv')}")
    spark.stop()
