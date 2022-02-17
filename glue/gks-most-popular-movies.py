import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs
import re


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="gk_db", table_name="optimized_movies", transformation_ctx="S3bucket_node1"
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1645087002456 = glueContext.create_dynamic_frame.from_catalog(
    database="gk_db",
    table_name="optimized_ratings",
    transformation_ctx="AWSGlueDataCatalog_node1645087002456",
)

# Script generated for node DropGenres
DropGenres_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("movieid", "long", "movieid", "long"),
        ("title", "string", "title", "string"),
    ],
    transformation_ctx="DropGenres_node2",
)

# Script generated for node DropTimestamp
DropTimestamp_node1645087176164 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1645087002456,
    mappings=[
        ("userid", "long", "userid", "long"),
        ("movieid", "long", "movieid", "long"),
        ("rating", "double", "rating", "double"),
    ],
    transformation_ctx="DropTimestamp_node1645087176164",
)

# Script generated for node Aggregate
Aggregate_node1645087501088 = sparkAggregate(
    glueContext,
    parentFrame=DropTimestamp_node1645087176164,
    groups=["movieid"],
    aggs=[["rating", "avg"], ["userid", "count"]],
    transformation_ctx="Aggregate_node1645087501088",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1645087885403 = ApplyMapping.apply(
    frame=Aggregate_node1645087501088,
    mappings=[
        ("movieid", "long", "(rating) movieid", "long"),
        ("avg(rating)", "double", "(rating) avg(rating)", "double"),
        ("count(userid)", "long", "(rating) count(userid)", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1645087885403",
)

# Script generated for node Join
Join_node1645087700999 = Join.apply(
    frame1=DropGenres_node2,
    frame2=RenamedkeysforJoin_node1645087885403,
    keys1=["movieid"],
    keys2=["(rating) movieid"],
    transformation_ctx="Join_node1645087700999",
)

# Script generated for node Drop Fields
DropFields_node1645088052583 = DropFields.apply(
    frame=Join_node1645087700999,
    paths=["(rating) movieid"],
    transformation_ctx="DropFields_node1645088052583",
)

# Script generated for node Rename Field
RenameField_node1645088122399 = RenameField.apply(
    frame=DropFields_node1645088052583,
    old_name="(rating) avg(rating)",
    new_name="avg_rating",
    transformation_ctx="RenameField_node1645088122399",
)

# Script generated for node Rename Field
RenameField_node1645088171427 = RenameField.apply(
    frame=RenameField_node1645088122399,
    old_name="(rating) count(userid)",
    new_name="total_ratings",
    transformation_ctx="RenameField_node1645088171427",
)

# Script generated for node Filter
Filter_node1645088228968 = Filter.apply(
    frame=RenameField_node1645088171427,
    f=lambda row: (row["total_ratings"] >= 100 and row["avg_rating"] >= 3),
    transformation_ctx="Filter_node1645088228968",
)

# Script generated for node Amazon S3
AmazonS3_node1645088396214 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1645088228968,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://gks-feb2022/optimized/popular-movies/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1645088396214",
)

job.commit()
