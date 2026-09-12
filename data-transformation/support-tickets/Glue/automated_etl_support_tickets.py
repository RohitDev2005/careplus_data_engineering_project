import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame
import re

def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split('.')[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
            elif isinstance(distinct_, str):
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output


def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
    nullColumns = _find_null_fields(
        frame.glue_ctx,
        frame.schema(),
        "",
        [],
        nullStringSet,
        nullIntegerSet,
        frame
    )
    return DropFields.apply(
        frame=frame,
        paths=nullColumns,
        transformation_ctx=transformation_ctx
    )


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_file_path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Default data quality ruleset.
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""


# Read the input CSV file from the S3 location provided to the Glue job.
AmazonS3_node1785590788718 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": "\"",
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [args['input_file_path']],
        "recurse": True
    },
    transformation_ctx="AmazonS3_node1785590788718"
)


# Convert source columns to the required data types.
ChangeSchema_node1785590988814 = ApplyMapping.apply(
    frame=AmazonS3_node1785590788718,
    mappings=[
        ("ticket_id", "string", "ticket_id", "string"),
        ("created_at", "string", "created_at", "timestamp"),
        ("resolved_at", "string", "resolved_at", "timestamp"),
        ("agent", "string", "agent", "string"),
        ("priority", "string", "priority", "string"),
        ("num_interactions", "string", "num_interactions", "int"),
        ("issuecat", "string", "issuecat", "string"),
        ("channel", "string", "channel", "string"),
        ("status", "string", "status", "string"),
        ("agent_feedback", "string", "agent_feedback", "string")
    ],
    transformation_ctx="ChangeSchema_node1785590988814"
)


# Remove fields containing only null or empty values.
DropNullFields_node1785591140830 = drop_nulls(
    glueContext,
    frame=ChangeSchema_node1785590988814,
    nullStringSet={""},
    nullIntegerSet={},
    transformation_ctx="DropNullFields_node1785591140830"
)


# Rename the issue category column.
RenameFieldissue_category_node1785591269694 = RenameField.apply(
    frame=DropNullFields_node1785591140830,
    old_name="issuecat",
    new_name="issue_category",
    transformation_ctx="RenameFieldissue_category_node1785591269694"
)


# Remove records with negative interaction counts.
Filternum_interactions_node1785591350287 = Filter.apply(
    frame=RenameFieldissue_category_node1785591269694,
    f=lambda row: (row["num_interactions"] >= 0),
    transformation_ctx="Filternum_interactions_node1785591350287"
)


# Standardize inconsistent priority values.
SqlQuery117 = '''
select *,
CASE
    WHEN priority = 'Lw' THEN 'Low'
    WHEN priority = 'Medum' THEN 'Medium'
    WHEN priority = 'Hgh' THEN 'High'
    ELSE priority
END AS priority
FROM myDataSource
'''

SQLQuerypriority_node1785591816799 = sparkSqlQuery(
    glueContext,
    query=SqlQuery117,
    mapping={
        "myDataSource": Filternum_interactions_node1785591350287
    },
    transformation_ctx="SQLQuerypriority_node1785591816799"
)


# Select the columns required for the processed dataset.
SelectFields_node1785593101289 = SelectFields.apply(
    frame=SQLQuerypriority_node1785591816799,
    paths=[
        "ticket_id",
        "created_at",
        "resolved_at",
        "agent",
        "priority",
        "num_interactions",
        "issue_category",
        "channel",
        "status"
    ],
    transformation_ctx="SelectFields_node1785593101289"
)


# Evaluate whether the transformed dataset satisfies the configured data quality rule.
EvaluateDataQuality().process_rows(
    frame=SelectFields_node1785593101289,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1785590618602",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)


# Coalesce the output into a single file when records are available.
if SelectFields_node1785593101289.count() >= 1:
    SelectFields_node1785593101289 = SelectFields_node1785593101289.coalesce(1)


# Write the transformed data to the processed S3 location in Parquet format.
AmazonS3_node1785592456695 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFields_node1785593101289,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://***/support-tickets/processed/",
        "partitionKeys": []
    },
    format_options={
        "compression": "snappy"
    },
    transformation_ctx="AmazonS3_node1785592456695"
)


job.commit()
