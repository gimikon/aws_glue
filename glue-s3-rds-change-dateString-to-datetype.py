import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, [“JOB_NAME”])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args[“JOB_NAME”], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
  database=“default”, table_name=“retail”, transformation_ctx=“S3bucket_node1”)

  df = S3bucket_node1.toDF().withColumn(“invoicedate”, to_date(col(‘invoicedate’), ‘yyyyMMdd’))
  dyf = DynamicFrame.fromDF(df, glueContext, “dyf”)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
  frame=dyf,
  connection_type=“s3",
  format=“glueparquet”,
  connection_options={“path”: “s3://xxxxxxxx”, “partitionKeys”: []},
  format_options={“compression”: “snappy”},
  transformation_ctx=“S3bucket_node3”,)


job.commit() (edited) 
