import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://xxxxxxxx"], "recurse": True},
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("id", "string", "id", "string"),
        ("first_name", "string", "first_name", "string"),
        ("last_name", "string", "last_name", "string"),
        ("email", "string", "email", "string"),
        ("gender", "string", "gender", "string"),
        ("connect_date", "string", "connect_date", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# If directly connect to RDS instead of using glue connection
# MySQLtable_node3 = glueContext.write_from_options(
#     frame_or_dfc=ApplyMapping_node2,
#     connection_type="mysql",
#     connection_options= {"url": "jdbc:mysql://xxxxxxxxxxxxx:3306/city", "user": "admin", "password": "yxxxxxxxx","ssl": "true","dbtable": "xxxxxx"},
#     transformation_ctx="MySQLtable_node3",
# )

JDBCConnection_node1 = glueContext.write_dynamic_frame.from_options(
    ApplyMapping_node2,
    connection_type="mysql",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "testdate",
        "connectionName": "test-rds",
    },
    transformation_ctx="JDBCConnection_node1",
)


job.commit()
