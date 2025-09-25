import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import re
import time
import datetime as _dt
import urllib.parse as up
import boto3
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, StructType, StructField

# ------------------ Parâmetros ------------------
ARG_KEYS = ['JOB_NAME', 'bucket', 'silver_prefix',
            'database', 'table', 'use_catalog']
defaults = {
    'bucket':        'cbchallenge',
    'silver_prefix': 'Silver/Todos os eventos/',
    'database':      'datalake',
    'table':         'events_silver_unico',
    'use_catalog':   'true'
}
try:
    args = getResolvedOptions(sys.argv, ARG_KEYS)
except Exception:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    for k, v in defaults.items():
        args[k] = v

USE_CATALOG = str(args.get('use_catalog', 'true')
                  ).lower() in ('1', 'true', 'yes', 'y')
BUCKET = args['bucket']
SILVER_PREFIX = args['silver_prefix'].rstrip('/') + '/'
CATALOG_DB = args['database']
CATALOG_TABLE = args['table']
FINAL_FILENAME = "events.csv"


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ------------------ Funções utilitárias ------------------


def normalize_cols(df):
    out = df
    for c in df.columns:
        new = re.sub(r'\s+', '_', c.replace('\ufeff', '')).strip().lower()
        if new != c:
            out = out.withColumnRenamed(c, new)
    return out


file_col = F.input_file_name()
INGEST_EXPR = F.coalesce(
    F.when(F.length(F.regexp_extract(
        file_col, r"date=([0-9]{4}-\d{2}-\d{2})", 1)) == 0, F.lit(None))
     .otherwise(F.regexp_extract(file_col, r"date=([0-9]{4}-\d{2}-\d{2})", 1)),
    F.when(F.length(F.regexp_extract(file_col, r"_(\d{8})-\d{6}\.csv$", 1)) > 0,
           F.concat_ws("-", F.substring(F.regexp_extract(file_col, r"_(\d{8})-\d{6}\.csv$", 1), 1, 4),
                       F.substring(F.regexp_extract(
                           file_col, r"_(\d{8})-\d{6}\.csv$", 1), 5, 2),
                       F.substring(F.regexp_extract(file_col, r"_(\d{8})-\d{6}\.csv$", 1), 7, 2)))
     .otherwise(F.lit(None)),
    F.date_format(F.current_timestamp(), "yyyy-MM-dd")
)

pt_months = {"janeiro": 1, "fevereiro": 2, "marco": 3, "março": 3, "abril": 4, "maio": 5, "junho": 6,
             "julho": 7, "agosto": 8, "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
             "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6, "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12}


def _parse_pt(text):
    if not text:
        return None
    t = str(text).strip().lower()
    m = re.search(
        r"(\d{1,2})\s+de\s+([a-zçãé]+)(?:\s+de\s+(\d{4}))?(?:\s+às\s+(\d{1,2}):(\d{2}))?", t)
    if m:
        d = int(m.group(1))
        mon = pt_months.get(m.group(2))
        y = int(m.group(3)) if m.group(3) else _dt.date.today().year
        hh = int(m.group(4)) if m.group(4) else 0
        mm = int(m.group(5)) if m.group(5) else 0
        if mon:
            try:
                return _dt.datetime(y, mon, d, hh, mm).strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass
    m = re.search(r"(\d{1,2})\s+([a-z]{3})", t)
    if m:
        d = int(m.group(1))
        mon = pt_months.get(m.group(2))
        y = _dt.date.today().year
        if mon:
            try:
                return _dt.datetime(y, mon, d).strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass
    return None


parse_udf = F.udf(_parse_pt, StringType())


def to_ts_any(c):
    return F.coalesce(
        F.to_timestamp(parse_udf(c)),
        F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(c, "yyyy-MM-dd"),
        F.to_timestamp(c, "dd/MM/yyyy HH:mm"),
        F.to_timestamp(c, "dd/MM/yyyy")
    ).cast(TimestampType())


def read_csv(paths):
    dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        format_options={"quoteChar": "\"",
                        "withHeader": True, "separator": ","},
        connection_options={"paths": paths, "recurse": True}
    )
    return dyf.toDF()


STD_SCHEMA = StructType([
    StructField("event_url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("venue", StringType(), True),
    StructField("city", StringType(), True),
    StructField("event_date_text", StringType(), True),
    StructField("event_date", TimestampType(), True),
    StructField("source", StringType(), True),
    StructField("ingest_date", StringType(), True),
    StructField("src_file", StringType(), True),
])


def empty_std_df():
    return spark.createDataFrame([], STD_SCHEMA)


# ------------------ Paths de origem (maiúsculo e minúsculo) ------------------
sympla_paths = [f"s3://{BUCKET}/Bronze/database Sympla/",
                f"s3://{BUCKET}/bronze/database Sympla/"]
t360_paths = [f"s3://{BUCKET}/Bronze/database Ticket360/",
              f"s3://{BUCKET}/bronze/database Ticket360/"]
eventim_paths = [f"s3://{BUCKET}/Bronze/database Eventim/", f"s3://{BUCKET}/bronze/database Eventim/",
                 f"s3://{BUCKET}/Bronze/database Evetim/", f"s3://{BUCKET}/bronze/database Evetim/"]


sympla_raw = read_csv(sympla_paths)
t360_raw = read_csv(t360_paths)
eventim_raw = read_csv(eventim_paths)

sympla_df = normalize_cols(sympla_raw).withColumn("src_file", file_col)
t360_df = normalize_cols(t360_raw).withColumn("src_file", file_col)
eventim_df = normalize_cols(eventim_raw).withColumn("src_file", file_col)

print("Schemas:", {"sympla": sympla_df.columns,
      "ticket360": t360_df.columns, "eventim": eventim_df.columns})


def map_sympla(df):
    cols = set(df.columns)
    req = {"url", "title", "place", "date"}
    if not req.issubset(cols):
        print("[sympla] vazio/faltando -> DF padrão vazio")
        return empty_std_df()
    out = (df
           .withColumn("event_url",       F.col("url"))
           .withColumn("title",           F.col("title"))
           .withColumn("venue",           F.col("place"))
           .withColumn("city",            F.lit(None).cast("string"))
           .withColumn("event_date_text", F.col("date"))
           .withColumn("event_date",      to_ts_any(F.col("date")))
           .withColumn("source",          F.lit("sympla"))
           .withColumn("ingest_date",     INGEST_EXPR)
           .select("event_url", "title", "venue", "city", "event_date_text", "event_date", "source", "ingest_date", "src_file")
           )
    out = out.withColumn(
        "city",
        F.when(
            (F.col("city").isNull()) | (F.length("city") == 0),
            F.trim(F.regexp_extract(F.col("venue"), r"-(?!.*-)\s*(.*)$", 1))
        ).otherwise(F.col("city"))
    )
    return out


def map_t360(df):
    cols = set(df.columns)
    req = {"url", "title", "place", "city", "date"}
    if not req.issubset(cols):
        print("[ticket360] vazio/faltando -> DF padrão vazio")
        return empty_std_df()
    return (df
            .withColumn("event_url",       F.col("url"))
            .withColumn("title",           F.col("title"))
            .withColumn("venue",           F.col("place"))
            .withColumn("city",            F.col("city"))
            .withColumn("event_date_text", F.col("date"))
            .withColumn("event_date",      to_ts_any(F.col("date")))
            .withColumn("source",          F.lit("ticket360"))
            .withColumn("ingest_date",     INGEST_EXPR)
            .select("event_url", "title", "venue", "city", "event_date_text", "event_date", "source", "ingest_date", "src_file")
            )


def map_eventim(df):
    cols = set(df.columns)
    req = {"artist", "url", "location", "date"}
    if not req.issubset(cols):
        print("[eventim] vazio/faltando -> DF padrão vazio")
        return empty_std_df()
    return (df
            .withColumn("event_url",       F.col("url"))
            .withColumn("title",           F.col("artist"))
            .withColumn("venue",           F.col("location"))
            .withColumn("city",            F.lit(None).cast("string"))
            .withColumn("event_date_text", F.col("date"))
            .withColumn("event_date",      to_ts_any(F.col("date")))
            .withColumn("source",          F.lit("eventim"))
            .withColumn("ingest_date",     INGEST_EXPR)
            .select("event_url", "title", "venue", "city", "event_date_text", "event_date", "source", "ingest_date", "src_file")
            )


sympla_norm = map_sympla(sympla_df)
t360_norm = map_t360(t360_df)
eventim_norm = map_eventim(eventim_df)

# ------------------ União e deduplicação ------------------
events_silver = (sympla_norm
                 .unionByName(t360_norm, allowMissingColumns=True)
                 .unionByName(eventim_norm, allowMissingColumns=True)) \
    .filter(F.col("event_url").isNotNull() & (F.length("event_url") > 0)) \
    .dropDuplicates(["event_url"])

print("Unified rows:", events_silver.count())


s3 = boto3.client("s3")
FINAL_DIR = SILVER_PREFIX
FINAL_KEY = FINAL_DIR + FINAL_FILENAME
TMP_DIR = FINAL_DIR + f"_tmp_write_{int(time.time())}/"

cols_out = ["event_url", "title", "venue", "city",
            "event_date_text", "event_date", "source", "ingest_date"]

total = events_silver.count()
if total == 0:
    header = ",".join(cols_out) + "\n"
    s3.put_object(Bucket=BUCKET, Key=FINAL_KEY,
                  Body=header.encode("utf-8"), ContentType="text/csv")
    print(f"✔ CSV vazio com header em s3://{BUCKET}/{FINAL_KEY}")
else:
    (events_silver.select(*cols_out)
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(f"s3://{BUCKET}/{TMP_DIR}"))

    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=TMP_DIR)
    part_key = None
    for o in resp.get("Contents", []):
        k = o["Key"]
        if k.endswith(".csv") and "/_SUCCESS" not in k:
            part_key = k
            break
    if not part_key:
        raise RuntimeError("Não encontrei o arquivo part-*.csv no TMP_DIR")

    s3.copy_object(Bucket=BUCKET, Key=FINAL_KEY, CopySource={
                   "Bucket": BUCKET, "Key": part_key})
    print(f"✔ CSV único em s3://{BUCKET}/{FINAL_KEY}")

    existing = s3.list_objects_v2(Bucket=BUCKET, Prefix=FINAL_DIR)
    for o in existing.get("Contents", []):
        k = o["Key"]
        if k != FINAL_KEY and not k.startswith(TMP_DIR):
            s3.delete_object(Bucket=BUCKET, Key=k)

    resp2 = s3.list_objects_v2(Bucket=BUCKET, Prefix=TMP_DIR)
    for o in resp2.get("Contents", []):
        s3.delete_object(Bucket=BUCKET, Key=o["Key"])


def spark_to_glue_cols(schema):
    out = []
    for f in schema.fields:
        if f.name == "src_file":
            continue
        t = "string"
        if str(f.dataType).lower().startswith("timestamp"):
            t = "timestamp"
        out.append({"Name": f.name, "Type": t})
    return out


if USE_CATALOG:
    glue = boto3.client("glue")
    try:
        glue.get_database(Name=CATALOG_DB)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={
                             "Name": CATALOG_DB, "Description": "DB Silver (arquivo único CSV)"})

    cols = spark_to_glue_cols(events_silver.select(*cols_out).schema)

    table_input = {
        "Name": CATALOG_TABLE,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "csv",
            "skip.header.line.count": "1",
            "compressionType": "none",
            "typeOfData": "file"
        },
        "StorageDescriptor": {
            "Columns": cols,
            "Location": f"s3://{BUCKET}/{FINAL_DIR}",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Parameters": {"field.delim": ",", "escape.delim": "\\", "serialization.format": ","}
            }
        }
    }

    try:
        glue.get_table(DatabaseName=CATALOG_DB, Name=CATALOG_TABLE)
        glue.update_table(DatabaseName=CATALOG_DB, TableInput=table_input)
        print(f"✔ Glue Catalog atualizado: {CATALOG_DB}.{CATALOG_TABLE}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=CATALOG_DB, TableInput=table_input)
        print(f"✔ Glue Catalog criado: {CATALOG_DB}.{CATALOG_TABLE}")

s3 = boto3.client("s3")


def to_bucket_key(s3_uri: str):
    p = s3_uri.replace("s3://", "")
    bkt, key = p.split("/", 1)
    return bkt, key


def detect_source_from_key(key: str):
    if "database Sympla/" in key:
        return "sympla"
    if "database Ticket360/" in key:
        return "ticket360"
    if "database Eventim/" in key or "database Evetim/" in key:
        return "eventim"
    m = re.search(r"source=([^/]+)/", key)
    return m.group(1) if m else "desconhecida"


def extract_date_prefix(key: str):
    m = re.search(r"(.*/date=\d{4}-\d{2}-\d{2}/)", key)
    return m.group(1) if m else None


def extract_date_value(key: str):
    m = re.search(r"date=(\d{4}-\d{2}-\d{2})", key)
    return m.group(1) if m else None


def processed_key_from_raw(key: str):
    root = key.split('/', 1)[0]
    src = detect_source_from_key(key)
    dt = extract_date_value(key) or "0000-00-00"
    fn = key.split('/')[-1]
    return f"{root}/Arquivos Processados/source={src}/date={dt}/{fn}"


def list_date_prefixes(bucket, base_prefix):
    dates = set()
    token = None
    while True:
        kw = dict(Bucket=bucket, Prefix=base_prefix)
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            k = o["Key"]
            p = extract_date_prefix(k)
            if p:
                dates.add(p)
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return sorted(dates)


def move_all_under_prefix(bucket, date_prefix):
    token = None
    keys = []
    while True:
        kw = dict(Bucket=bucket, Prefix=date_prefix)
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            keys.append(o["Key"])
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

    moved = 0
    for key in keys:
        if key.endswith("/"):
            continue
        new_key = processed_key_from_raw(key)
        try:
            s3.copy_object(Bucket=bucket, Key=new_key, CopySource={
                           "Bucket": bucket, "Key": key})
            s3.delete_object(Bucket=bucket, Key=key)
            moved += 1
        except Exception as e:
            print(f"[WARN] não movido {key}: {e}")
    print(
        f"✔ Movidos {moved} arquivos de s3://{bucket}/{date_prefix} para Arquivos Processados")

    try:
        s3.delete_object(Bucket=bucket, Key=date_prefix)
        print(f"✔ Pasta removida: s3://{bucket}/{date_prefix}")
    except Exception:
        pass


SYMPLA_BASES = [f"Bronze/database Sympla/",  f"bronze/database Sympla/"]
T360_BASES = [f"Bronze/database Ticket360/", f"bronze/database Ticket360/"]
EVENTIM_BASES = [f"Bronze/database Eventim/", f"bronze/database Eventim/",
                 f"Bronze/database Evetim/",  f"bronze/database Evetim/"]

date_prefixes_seen = set()
for df in [sympla_df, t360_df, eventim_df]:
    if "src_file" in df.columns:
        for uri in [r[0] for r in df.select("src_file").distinct().collect()]:
            if uri and str(uri).startswith("s3://"):
                _, key = to_bucket_key(uri)
                p = extract_date_prefix(key)
                if p:
                    date_prefixes_seen.add(p)


def ensure_latest_when_empty(bases, label):
    has_any = any(p for p in date_prefixes_seen if f"database {label}/" in p)
    if not has_any:
        all_dates = []
        for base in bases:
            all_dates += list_date_prefixes(BUCKET, base)
        if all_dates:
            latest = sorted(all_dates)[-1]
            date_prefixes_seen.add(latest)
            print(
                f"[{label}] sem linhas; adicionada última pasta para mover: {latest}")


ensure_latest_when_empty(SYMPLA_BASES, "Sympla")
ensure_latest_when_empty(T360_BASES, "Ticket360")
ensure_latest_when_empty(EVENTIM_BASES, "Eventim")
ensure_latest_when_empty(EVENTIM_BASES, "Evetim")

for date_prefix in sorted(date_prefixes_seen):
    move_all_under_prefix(BUCKET, date_prefix)


job.commit()
