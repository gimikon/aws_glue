import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Data Catalog table for testing purpose
DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="osa-non-lakeformation",
    table_name="enclosed_value_csv",
    transformation_ctx="DataCatalogtable_node1",
)

DataCatalogtable_node1.printSchema()
DataCatalogtable_node1.show(3)
