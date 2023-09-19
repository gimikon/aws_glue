import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import split, col,substring,regexp_replace

# Initialize SparkContext, GlueContext and SparkSession.
params = []
if '--JOB_NAME' in sys.argv:
    params.append('JOB_NAME')
args = getResolvedOptions(sys.argv, params)
if 'JOB_NAME' in args:
    jobname = args['JOB_NAME']
else:
    jobname = "test"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

job = Job(glueContext)
job.init(jobname, args)

DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="xxxxxx",
    table_name="xxxxx",
    transformation_ctx="DataCatalogtable_node1",
)

df = DataCatalogtable_node1.toDF()

df2 = df.withColumn('day', split(df['time'], '/').getItem(0)) \
       .withColumn('month', split(df['time'], '/').getItem(1)) \
       .withColumn('year', split(df['time'], '/').getItem(2).substr(1, 4))

targetDynamicFrame = DynamicFrame.fromDF(df2, glueContext, "s3_access_logs_enriched")

S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=targetDynamicFrame,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://xxxxx/prefix/converted/",
        "partitionKeys": ["year","month","day"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)

job.commit()
