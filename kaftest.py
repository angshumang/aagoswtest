# Databricks notebook source
import os, json,re
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StringType,StructField,ArrayType,LongType,IntegerType,TimestampType
from delta.tables import *
import boto3, io, json

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /tmp/xxx

# COMMAND ----------

import os, json,re
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StringType,StructField,ArrayType,LongType,IntegerType,TimestampType
from delta.tables import *
import boto3, io, json
from functools import partial
import importlib
import time
import copy

from botocore.config import Config

from workflow.processor import write

config = Config(
    region_name = 'us-east-1'
)

s3 = boto3.client("s3",config=config)
secrets = boto3.client("secretsmanager",config=config)

PROCESS_CONFIG = { "timestamp_column" : "timestamp", "current_column" : "current" }


#this should be in some generic config
#infrastructure bucket/conf
# s3_config_bucket = "aa-data-s3-nonprd"
# s3_config_prefix = "tmp1/delta-poc/conf"
# formatted_table_prefix = '/mnt/aa-data-s3-uat/delta-poc/tmp1'
# formatted_checkpoint_prefix = "/mnt/aa-data-s3-nonprd/tmp1/delta-poc/checkpoint"
# query_poll_interval = 20
#query_poll_interval = 3600

global_config_location_template = '{env}/databricks/kafka/config/global'
cluster_config_location_template = '{env}/databricks/kafka/config/{source}'

def configure_kafka_consumer_options(stream,cluster_config,job_params,**kwargs):
    max_offsets_per_trigger = job_params["max_offsets_per_trigger"]
    kafka_topic_name = job_params["kafka_topic_name"]
    bootstrap_servers = cluster_config["bootstrap_servers"]
    group_id_prefix = cluster_config["group_id_prefix"]
    consumer_group_id = cluster_config["consumer_group_id"]
    return stream \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger) \
        .option("kafka.group.id",consumer_group_id)
        # .option("groupIdPrefix", group_id_prefix)

def get_json_from_s3(s3_bucket,s3_key):
    content = s3.get_object(Bucket=s3_bucket, Key=s3_key)['Body'].read()
    content = content.decode('utf-8')
    content = json.loads(content)
    return content

def put_content_into_s3(content,s3_bucket,s3_key):
    s3.put_object(Bucket=s3_bucket, Key=s3_key,Body=content)

def get_topic_config(job_params,generic_config):
    source = job_params["source"]
    topic_name = job_params["topic_name"]
    s3_config_prefix = generic_config["s3_config_prefix"]
    s3_config_bucket = generic_config["s3_config_bucket"]
    s3_config_key = f'{s3_config_prefix}/{source}/{topic_name}/config.json'
    return get_json_from_s3(s3_config_bucket,s3_config_key)

def inject_ref_types(avro_schema,ref_type):
    avro_schema_str = json.dumps(avro_schema)
    ref_type_template_str = json.dumps(ref_type)
    ref_types = re.findall(r'REF_\w+', avro_schema_str)
    for ref_type in ref_types:
        ref_type_name = ref_type.replace('REF_', '')
        ref_type_str = ref_type_template_str.replace("REF_NAME", ref_type_name)
        avro_schema_str = avro_schema_str.replace(f'"{ref_type}"', ref_type_str)
    return json.loads(avro_schema_str)

def get_avro_schema(job_params,generic_config):
    source = job_params["source"]
    topic_name = job_params["topic_name"]
    s3_config_prefix = generic_config["s3_config_prefix"]
    s3_config_bucket = generic_config["s3_config_bucket"]
    s3_config_key = f'{s3_config_prefix}/{source}/{topic_name}/schema.json'
    avro_schema = get_json_from_s3(s3_config_bucket,s3_config_key)
    s3_config_key_ref = f'{s3_config_prefix}/workflow/ref_type.json'
    ref_type = get_json_from_s3(s3_config_bucket, s3_config_key_ref)
    return inject_ref_types(avro_schema,ref_type)

def get_saved_schema(generic_config,job_params,**kwargs):
    source = job_params["source"]
    topic_name = job_params["topic_name"]
    checkpoint_s3_full_path = str(generic_config["formatted_checkpoint_prefix"]).replace("/mnt/","")
    checkpoint_s3_bucket = checkpoint_s3_full_path.split("/")[0]
    checkpoint_s3_prefix = '/'.join(checkpoint_s3_full_path.split("/")[1:])
    saved_schema_key = f'{checkpoint_s3_prefix}/{source}/{topic_name}/target_schema.json'
    schema_json = get_json_from_s3(checkpoint_s3_bucket,saved_schema_key)
    schema = StructType.fromJson(schema_json)
    return schema

def save_schema(schema_json,generic_config,job_params,**kwargs):
    source = job_params["source"]
    topic_name = job_params["topic_name"]
    checkpoint_s3_full_path = str(generic_config["formatted_checkpoint_prefix"]).replace("/mnt/","")
    checkpoint_s3_bucket = checkpoint_s3_full_path.split("/")[0]
    checkpoint_s3_prefix = '/'.join(checkpoint_s3_full_path.split("/")[1:])
    saved_schema_key = f'{checkpoint_s3_prefix}/{source}/{topic_name}/target_schema.json'
    put_content_into_s3(json.dumps(schema_json).encode("utf-8"),checkpoint_s3_bucket,saved_schema_key)

def get_struct_schemas(job_params,generic_config,topic_config):
    source = job_params["source"]
    topic_name = job_params["topic_name"]
    s3_config_prefix = generic_config["s3_config_prefix"]
    s3_config_bucket = generic_config["s3_config_bucket"]
    s3_config_key = f'{s3_config_prefix}/{source}/{topic_name}/schema.json'
    schema_json = get_json_from_s3(s3_config_bucket,s3_config_key)
    schema = StructType.fromJson(schema_json)
    target_schema_json = schema_json
    target_schema = schema
    has_target_schema = topic_config.get("has_target_schema")
    if has_target_schema:
        s3_config_key = f'{s3_config_prefix}/{source}/{topic_name}/target_schema.json'
        target_schema_json = get_json_from_s3(s3_config_bucket, s3_config_key)
        target_schema = StructType.fromJson(schema_json)

    return schema,target_schema,schema_json,target_schema_json

def configure_kafka_cluster_credentials(stream,cluster_config,**kwargs):
    # .option("kafka.ssl.client.auth", "none") \
    # .option("kafka.security.protocol", "SSL") \
    trust_store_location = cluster_config["trust_store_location"]
    trust_store_password = cluster_config["trust_store_password"]
    sasl_plain_user = cluster_config["sasl_plain_user"]
    sasl_plain_password = cluster_config["sasl_plain_password"]
    sasl_jaas_config = f"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{sasl_plain_user}\" password=\"{sasl_plain_password}\" ;"
    stream \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.ssl.truststore.location", trust_store_location) \
        .option("kafka.ssl.truststore.password", trust_store_password) \
        .option("kafka.ssl.enabled.protocols", "TLSv1.2") \
        .option("kafka.ssl.endpoint.identification.algorithm", "") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config",sasl_jaas_config )

    return stream

def get_generic_config(job_params):
    env = job_params["env"]
    params = {"env": env}
    secret_name = global_config_location_template.format(**params)
    secret = secrets.get_secret_value(SecretId=secret_name)['SecretString']
    return json.loads(secret)

def get_cluster_config(job_params):
    source = job_params["source"]
    env = job_params["env"]
    params = {"env": env, "source": source}
    secret_name = cluster_config_location_template.format(**params)
    secret = secrets.get_secret_value(SecretId=secret_name)['SecretString']
    return json.loads(secret)

def convert_avro_to_struct(spark, avro_schema):
    spark_context = spark.sparkContext
    avro_schema = spark_context._jvm.org.apache.avro.Schema.Parser().setValidate(True).setValidateDefaults(True).parse(
        json.dumps(avro_schema))
    java_schema_type = spark_context._jvm.org.apache.spark.sql.avro.SchemaConverters.toSqlType(avro_schema)
    java_struct_schema = java_schema_type.dataType()
    struct_json_schema = java_struct_schema.json()
    json_schema_obj = json.loads(struct_json_schema)
    schema = StructType.fromJson(json_schema_obj)
    return schema

def get_structured_df(df,struct_schema,topic_config,process_config,**kwargs):
    primary_keys = topic_config["primary_keys"].split(',')
    timestamp_column = process_config["timestamp_column"]
    # df = df.select(col("timestamp"),col("value").alias("value_raw"),
    #                  from_json('value', struct_schema, options={'mode': 'FAILFAST', 'ignoreNullFields': False})
    #                  .alias('value')
    #                  ).select("timestamp",col("value._id").alias("_id"), "value","value_raw")
    df = df.select(col(timestamp_column), col("value").alias("value_raw"),
                   from_json('value', struct_schema, options={'mode': 'FAILFAST'})
                   .alias('value')
                   ).select(timestamp_column,"value", "value_raw",*([f.col(f'value.{c}') for c in primary_keys]))
    return df

def write_df(df,topic_processor,**kwargs):
    job_params = kwargs["job_params"]
    generic_config = kwargs["generic_config"]
    # callable = getattr(topic_processor,"write")
    try:
        callable = getattr(topic_processor, "write")
    except Exception as e:
        print("No custom write function. Using default")
        callable = write

    call = partial(callable,kwargs)

    source = job_params["source"]
    topic_name = job_params["topic_name"]
    formatted_checkpoint_prefix = generic_config["formatted_checkpoint_prefix"]
    query = df.writeStream.format("delta")\
        .outputMode("update")\
        .option("checkpointLocation",f'{formatted_checkpoint_prefix}/{source}/{topic_name}')\
        .foreachBatch(call)\
        .start()
    return query

def transform_df(df,topic_processor,**kwargs):
    callable = getattr(topic_processor,"transform")
    return callable(kwargs,df)

def get_topic_processor(job_params):
    source = job_params["source"]
    topic_name = str(job_params["topic_name"]).lower().replace(".","_")
    topic_processor = importlib.import_module(f"{source}.{topic_name}.processor")
    return topic_processor


def wait_for_query_finish(query,generic_config,**kwargs):
    query_poll_interval_seconds = generic_config["query_poll_interval_seconds"]
    query_poll_timeout_seconds = generic_config["query_poll_timeout_seconds"]
    query_poll_time = 0
    while query.isActive:
        status = query.status
        print(status)
        lastProgress = query.lastProgress
        print(lastProgress)
        if not query.status['isDataAvailable'] \
                and not query.status['isTriggerActive'] \
                and not 'Initializing' in query.status['message']:
            if lastProgress['numInputRows'] == 0:
                query.stop()
        query.awaitTermination(query_poll_interval_seconds)
        # time.sleep(query_poll_interval_seconds)
        query_poll_time += query_poll_interval_seconds
        if(query_poll_time > query_poll_timeout_seconds) :
            raise Exception(f"Timed out waiting for streaming query to finish. Last status:{status}")

def create_or_modify_delta_table(resource_path, spark, struct_schema, table_name, primary_keys, process_config):
    timestamp_column = process_config["timestamp_column"]
    current_column = process_config["current_column"]
    timestamp_field = StructField(timestamp_column,TimestampType(),True)
    current_field = StructField(current_column,StringType(),True)
    extended_schema = copy.deepcopy(struct_schema)
    extended_schema = extended_schema.add(timestamp_field).add(current_field)
    emptyRDD = spark.sparkContext.emptyRDD()
    emptyDF = spark.createDataFrame(emptyRDD, extended_schema)
    emptyDF.write.format("delta").mode("append").option("mergeSchema","true").save(resource_path)
    spark.sql("CREATE TABLE IF NOT EXISTS " + table_name + " USING DELTA LOCATION '" + resource_path + "'")

    table_name = f'{table_name}_src'
    resource_path = f'{resource_path}_src'
    schema = StructType([timestamp_field])
    for key in primary_keys:
        key_field = StructField(key, StringType(), True)
        schema = schema.add(key_field)
    value_raw = StructField("value_raw", StringType(), True)
    schema.add(value_raw)
    exists = DeltaTable.isDeltaTable(spark, resource_path)
    if not exists:
        emptyRDD = spark.sparkContext.emptyRDD()
        emptyDF = spark.createDataFrame(emptyRDD, schema)
        emptyDF.write.format("delta").mode("append").save(resource_path)
        spark.sql("CREATE TABLE IF NOT EXISTS " + table_name + " USING DELTA LOCATION '" + resource_path + "'")

def create_table_if_needed(spark,target_struct_schema,target_struct_schema_json,job_params,generic_config,cluster_config,topic_config,process_config,**kwargs):
    topic_name = str(job_params["topic_name"]).lower().replace(".", "_")
    formatted_table_prefix = generic_config["formatted_table_prefix"]
    catalog_name = generic_config["formatted_table_catalog"]
    resource_name_prefix = cluster_config["resource_name_prefix"]
    resource_name = f'{resource_name_prefix}_{topic_name}'
    resource_path = f"{formatted_table_prefix}/{resource_name}"
    table_name = f"{catalog_name}.{resource_name}"
    primary_keys = topic_config["primary_keys"].split(',')
    table_exists = DeltaTable.isDeltaTable(spark, resource_path)
    is_schema_modified = False
    if table_exists:
        try:
            saved_schema = get_saved_schema(generic_config,job_params,**kwargs)
            is_schema_modified = True if saved_schema != target_struct_schema else False
        except Exception as e:
            if "NoSuchKey" in str(e):
                print(f"Saved schema not found")
                is_schema_modified = True
    if (not table_exists) or is_schema_modified:
        print(f"table_exists:{table_exists},schema_modified:{is_schema_modified}")
        create_or_modify_delta_table(resource_path, spark, target_struct_schema, table_name, primary_keys, process_config)
        save_schema(target_struct_schema_json,generic_config,job_params,**kwargs)
    return table_name,resource_path

def init(spark,generic_config,**kwargs):
    installed_py_files_archive_name = generic_config["installed_py_files_archive_name"].strip()
    installed_py_files_server_location = generic_config["installed_py_files_server_location"].strip()
    spark.sparkContext.addPyFile(f'{installed_py_files_server_location}{installed_py_files_archive_name}')

def run_microbatch_pipeline(spark,job_params):
    generic_config = get_generic_config(job_params)
    cluster_config = get_cluster_config(job_params)
    topic_config = get_topic_config(job_params,generic_config)
    # avro_schema = get_avro_schema(job_params,generic_config)
    # struct_schema = convert_avro_to_struct(spark,avro_schema)

    schema,target_schema,schema_json,target_schema_json = get_struct_schemas(job_params,generic_config,topic_config)
    topic_processor = get_topic_processor(job_params)
    kwargs = {
        "spark" : spark,
        "job_params" : job_params,
        "generic_config" : generic_config,
        "cluster_config" : cluster_config,
        "topic_config" : topic_config,
        "process_config" : PROCESS_CONFIG,
        # "avro_schema" : avro_schema,
        "struct_schema" : schema,
        "target_struct_schema" : target_schema,
        "struct_schema_json" : schema_json,
        "target_struct_schema_json" : target_schema_json,
        "topic_processor" : topic_processor
    }
    
    delta_table_name,resource_path = create_table_if_needed(**kwargs)
    kwargs["delta_table_name"]  = delta_table_name
    kwargs["resource_path"] = resource_path
    init(**kwargs)
    stream = spark.readStream.format("kafka")
    stream = configure_kafka_consumer_options(stream,**kwargs)
    stream = configure_kafka_cluster_credentials(stream,**kwargs)
    df = (stream.load())
    df = df.selectExpr(f"{PROCESS_CONFIG['timestamp_column']}", "CAST(value AS STRING)")
    df = get_structured_df(df,**kwargs)
    df = transform_df(df,**kwargs)
    query = write_df(df,**kwargs)
    wait_for_query_finish(query,**kwargs)

job_params = {"source": "ddp", "env": "nonprd", "topic_name": "curated_premise"}
job_params = {'batch_schedule': '0 0 1 * * ?', 'env': 'nonprd', 'kafka_topic_name': 'dev.curated.Party', 'max_offsets_per_trigger': 1000, 'source': 'ddp', 'source_env': 'dev', 'topic_name': 'curated_party'}
run_microbatch_pipeline(spark, job_params)

# if __name__ == "__main__":
#   job_params = {"source":"ddp", "env" : "nonprd","topic_name" : "dev.curated.Premise"}
#   run_microbatch_pipeline(spark,job_params)