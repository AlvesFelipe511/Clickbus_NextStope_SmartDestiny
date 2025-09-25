import sys
from datetime import datetime
from urllib.parse import urlparse

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


BRT_TZ = "America/Sao_Paulo"


def _ensure_slash(u: str) -> str:
    return u if u.endswith("/") else u + "/"


def parse_s3_uri(s3_uri: str):
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"URI inválida: {s3_uri}")
    p = urlparse(s3_uri)
    bucket = p.netloc
    prefix = p.path.lstrip("/")
    return bucket, prefix


def list_keys(s3, bucket, prefix):
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for c in resp.get("Contents", []):
            yield c["Key"]
        if resp.get("IsTruncated"):
            kwargs["ContinuationToken"] = resp.get("NextContinuationToken")
        else:
            break


def ensure_folder_marker(s3, bucket, prefix, marker_name="_KEEP"):
    if not prefix.endswith("/"):
        prefix += "/"
    key = f"{prefix}{marker_name}"
    s3.put_object(Bucket=bucket, Key=key, Body=b"")


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "silver_prefix",
        "gold_prefix",
        "processed_prefix"
    ],
)

silver_uri = _ensure_slash(args["silver_prefix"])
gold_uri = _ensure_slash(args["gold_prefix"])
processed_uri = _ensure_slash(args["processed_prefix"])

run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
run_date_folder = datetime.now().strftime("%Y-%m-%d")


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


df = (
    spark.read
    .option("header", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("encoding", "UTF-8")
    .option("ignoreLeadingWhiteSpace", True)
    .option("ignoreTrailingWhiteSpace", True)
    .csv(silver_uri)
)


expected = ["title", "venue", "city", "event_date_text",
            "event_date", "source", "ingest_date"]
for col in expected:
    if col not in df.columns:
        df = df.withColumn(col, F.lit(None).cast(T.StringType()))


df = (
    df.withColumn("title", F.trim(F.col("title")))
      .withColumn("venue", F.trim(F.col("venue")))
      .withColumn("city",  F.trim(F.col("city")))
      .withColumn("source", F.trim(F.col("source")))
      .withColumn("ingest_date", F.trim(F.col("ingest_date")))
      .withColumn("event_date",  F.trim(F.col("event_date")))
)


df = df.withColumn("city_norm", F.lower(F.col("city")))
df = df.filter(
    (F.col("city").isNotNull()) &
    (F.length(F.trim(F.col("city"))) > 0) &
    (~F.col("city_norm").isin("null", "none", "n/a", "na"))
).drop("city_norm")


venue_local = F.trim(
    F.regexp_replace(
        F.col("venue"),
        r"\s*[-–—][^-–—]*$",
        ""
    )
)
df = df.withColumn("venue_local", venue_local)


ts_event = F.to_timestamp("event_date")
ts_event_brt = F.from_utc_timestamp(ts_event, BRT_TZ)
col_data_evento = F.date_format(ts_event_brt, "dd/MM/yyyy")
col_hora_evento = F.date_format(ts_event_brt, "HH:mm")

ts_ingest = F.to_timestamp("ingest_date")
ts_ingest_brt = F.from_utc_timestamp(ts_ingest, BRT_TZ)
col_data_ingest = F.date_format(ts_ingest_brt, "dd/MM/yyyy HH:mm")


df_out = (
    df.select(
        F.col("title").alias("Nome do evento"),
        F.col("venue_local").alias("local"),
        F.col("city").alias("cidade"),
        col_data_evento.alias("evento (formato dd/mm/yyyy)"),
        col_hora_evento.alias("hora do evento (horário do brasil)"),
        F.col("source").alias("origem"),
        col_data_ingest.alias("data de ingestão"),
    )
)

gold_bucket, gold_prefix = parse_s3_uri(gold_uri)
gold_prefix = _ensure_slash(gold_prefix)
tmp_prefix = f"{gold_prefix}_tmp_run_{run_stamp}/"

(
    df_out.coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("quoteAll", True)
    .csv(f"s3://{gold_bucket}/{tmp_prefix}")
)

s3 = boto3.client("s3")


part_csv_key = None
for key in list_keys(s3, gold_bucket, tmp_prefix):
    name = key.split("/")[-1]
    if name.endswith(".csv") and name.startswith("part-"):
        part_csv_key = key
        break

if not part_csv_key:

    for key in list_keys(s3, gold_bucket, tmp_prefix):
        if key.endswith(".csv"):
            part_csv_key = key
            break

if not part_csv_key:
    raise RuntimeError("Não encontrei o CSV de saída no diretório temporário.")

final_key = f"{gold_prefix}eventos_padronizados_{run_stamp}.csv"


s3.copy_object(
    Bucket=gold_bucket,
    CopySource={"Bucket": gold_bucket, "Key": part_csv_key},
    Key=final_key
)

to_delete = [{"Key": k} for k in list_keys(s3, gold_bucket, tmp_prefix)]
if to_delete:
    # (lotes pequenos; se um dia passar de 1000, vale paginar por 1000)
    s3.delete_objects(Bucket=gold_bucket, Delete={"Objects": to_delete})

# =================== Move processados da SILVER ===================
silver_bucket, silver_prefix = parse_s3_uri(silver_uri)
silver_prefix = _ensure_slash(silver_prefix)

processed_bucket, processed_prefix = parse_s3_uri(processed_uri)
processed_prefix = _ensure_slash(processed_prefix)
dest_processed_prefix = f"{processed_prefix}{run_date_folder}/"

# Move somente .csv que estão na raiz do prefixo do Silver (ignora _KEEP/_SUCCESS)
to_move = [
    k for k in list_keys(s3, silver_bucket, silver_prefix)
    if k.lower().endswith(".csv")
    and not k.split("/")[-1].startswith("_")
]

for src_key in to_move:
    base_name = src_key.split("/")[-1]
    dst_key = f"{dest_processed_prefix}{base_name}"
    s3.copy_object(
        Bucket=processed_bucket,
        CopySource={"Bucket": silver_bucket, "Key": src_key},
        Key=dst_key
    )
    s3.delete_object(Bucket=silver_bucket, Key=src_key)


ensure_folder_marker(s3, silver_bucket, silver_prefix, marker_name="_KEEP")

print(f"OK! Gerado: s3://{gold_bucket}/{final_key}")
print(
    f"Arquivos do Silver movidos para: s3://{processed_bucket}/{dest_processed_prefix}")

job.commit()
