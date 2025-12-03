import os
import json
import csv
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from bson import ObjectId
from pandas import Timestamp

from common.mongo_util import MongoDB

OUT_BASE = "D:/STOCK_PROJECT/batch_out"


def json_safe_value(value):
    # ObjectId → str
    if isinstance(value, ObjectId):
        return str(value)

    # datetime 변환
    if isinstance(value, (datetime, Timestamp)):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    # Mongo weird datetime fallback
    try:
        if "datetime" in str(type(value)).lower():
            return str(value)
    except:
        pass

    return value


def convert_all(obj):
    if isinstance(obj, dict):
        return {k: convert_all(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_all(v) for v in obj]
    else:
        return json_safe_value(obj)


def export_strategy_collection(col_name: str, key_name: str):
    mongo = MongoDB()
    db = mongo.db

    col = db[col_name]

    KST = timezone(timedelta(hours=9))

    today = datetime.now(KST).replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today + timedelta(days=1)
    today_folder = today.strftime("%Y%m%d")

    out_dir = f"{OUT_BASE}/{today_folder}"
    os.makedirs(out_dir, exist_ok=True)

    # 🔥 created_at 기준으로 오늘 데이터만 조회
    docs = list(col.find(
        {"created_at": {"$gte": today, "$lt": tomorrow}},
        {"_id": 0}
    ))

    # 🔥 변경: 전체 데이터 조회
    # docs = list(col.find({}, {"_id": 0})) # 전체 데이터 위한 추가

    if not docs:
        print(f"⚠ {col_name}: 오늘 데이터 없음")
        return None

    # JSON 안전 변환
    docs = convert_all(docs)

    # 🔥 CSV 파일 경로 설정
    file_path = f"{out_dir}/{key_name}_{today_folder}.csv"

    # 🔥 CSV 작성
    with open(file_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)

        # 헤더 생성 (첫 번째 document 기준)
        header = list(docs[0].keys())
        writer.writerow(header)

        # 데이터 작성
        for d in docs:
            row = [d.get(col, "") for col in header]
            writer.writerow(row)

    print(f"✔ CSV 생성 완료: {file_path}")
    mongo.close()
    return file_path
