import os
import json
import csv
import io
import hashlib
import boto3
from pymongo import MongoClient, UpdateOne

s3 = boto3.client("s3")


def _decode_bytes(b: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return b.decode(enc)
        except Exception:
            pass
    return b.decode("latin-1", errors="ignore")


def _sniff_delimiter(sample: str) -> str:
    try:
        return csv.Sniffer().sniff(sample, delimiters=";,").delimiter
    except Exception:
        return ";"


def _normkey(k: str) -> str:
    return (k or "").strip().lower()


ALIASES = {
    "nome":   ["nome do evento", "nome", "titulo", "title"],
    "local":  ["local", "venue", "local do evento"],
    "cidade": ["cidade", "city"],
    "data":   ["evento (formato dd/mm/yyyy)", "data do evento", "data", "evento"],
    "hora":   ["hora do evento (horário do brasil)", "hora do evento (horario do brasil)", "hora"],
    "origem": ["origem", "source"],
    "ing":    ["data de ingestão", "data de ingestao", "ingestion_date", "ingestion_ts_csv", "data_ingestao"]
}


def _pick(row: dict, names: list[str]):
    for n in names:
        if n in row and row[n] not in ("", None):
            return row[n]
    return None


def _id_hash(*parts) -> str:
    base = "|".join((p or "").strip() for p in parts)
    return hashlib.md5(base.encode("utf-8")).hexdigest()


def _get_mongo_uri(event: dict) -> str:
 
    name = event.get("mongoSecretName")
    if name:
        import boto3
        sm = boto3.client("secretsmanager")
        try:
            return sm.get_secret_value(SecretId=name)["SecretString"]
        except Exception as e:
            print("WARN: secret not found/denied:", str(e))
    uri = os.getenv("MONGO_URI")
    if uri:
        return uri
    return "mongodb+srv://USUARIO:SENHA@SEU-CLUSTER.mongodb.net/clickbus?retryWrites=true&w=majority"


def lambda_handler(event, _):
    print("EVENT:", json.dumps(event))
    bucket = event["bucket"]
    key = event["key"]

    # 1) S3
    print("Reading S3:", bucket, key)
    obj = s3.get_object(Bucket=bucket, Key=key)
    text = _decode_bytes(obj["Body"].read())
    sample = text[:2048]
    delim = _sniff_delimiter(sample)
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)
    headers = [h for h in (reader.fieldnames or [])]
    rows = [{_normkey(k): (v.strip() if isinstance(v, str) else v)
             for k, v in r.items()} for r in reader]
    print(f"Delimiter='{delim}' Headers={headers} Rows={len(rows)}")

    if not rows:
        return {"ok": True, "count": 0, "note": "CSV sem linhas úteis"}

    # 2) Mongo
    mongo_uri = _get_mongo_uri(event)
    print("Have MONGO_URI?", bool(mongo_uri),
          "from_env?", bool(os.getenv("MONGO_URI")))
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
    try:
        client.admin.command("ping")
        print("Mongo ping ok")
    except Exception as e:
        print("Mongo ping FAILED:", str(e))
        raise

    coll = client["clickbus"]["eventos"]

    ops = []
    for r in rows:
        nome = _pick(r, ALIASES["nome"])
        cidade = _pick(r, ALIASES["cidade"])
        local = _pick(r, ALIASES["local"])
        data = _pick(r, ALIASES["data"])
        hora = _pick(r, ALIASES["hora"])
        origem = _pick(r, ALIASES["origem"])
        _id = _id_hash(nome, data, hora, cidade, origem)
        doc = {
            "_id": _id,
            "nome": nome,
            "data": data,
            "hora_local": hora,
            "lugar": {"cidade": cidade, "venue": local, "estado": None},
            "origem": origem,
            "source_key": key
        }
        ops.append(UpdateOne({"_id": _id}, {"$set": doc}, upsert=True))

    if ops:
        res = coll.bulk_write(ops, ordered=False)
        print("Bulk result:", res.bulk_api_result)
    print("UPSERTS:", len(ops))
    return {"ok": True, "count": len(ops)}
