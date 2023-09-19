import sys
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

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

query = "select * from city where ID = 0"
#your query statement

connection_mysql_options = {
    "url": "jdbc:mysql://host_name:3306/database",
    "dbtable": "({}) as t".format(query),
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver",
    "fetchsize": 1000
}

df = spark.read.format("jdbc").options(**connection_mysql_options).load()
