import os
import json
import csv
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson import ObjectId
from pandas import Timestamp

OUT_BASE = "D:/STOCK_PROJECT/batch_out"


def json_safe_value(value):
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, (datetime, Timestamp)):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value


def convert_all(obj):
    if isinstance(obj, dict):
        return {k: convert_all(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_all(v) for v in obj]
    else:
        return json_safe_value(obj)


def export_daily_price_collection(col_name: str, key_name: str):
    client = MongoClient("mongodb://root:0806@localhost:27017/?authSource=admin")
    db = client["investar"]

    col = db[col_name]

    today_folder = datetime.utcnow().strftime("%Y%m%d")

    out_dir = f"{OUT_BASE}/{today_folder}"
    os.makedirs(out_dir, exist_ok=True)

    # ================================
    # 🔥 오늘 UTC 00:00 ~ 내일 UTC 00:00
    # ================================
    start_utc = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    end_utc = start_utc + timedelta(days=1)

    docs = list(col.find(
        {"last_update": {"$gte": start_utc, "$lt": end_utc}},
        {"_id": 0}   # _id 제거
    ))

    # ============================================
    # 🔥 변경: 전체 데이터 조회
    # ============================================
    # docs = list(col.find({}, {"_id": 0})) # 전체 데이터 위한 추가

    if not docs:
        # print(f"⚠ {col_name}: 오늘 업데이트된 데이터 없음 (UTC 범위: {start_utc} ~ {end_utc})")
        print(f"⚠ {col_name}: 데이터 없음")
        return None

    # JSON 안전 변환
    docs = convert_all(docs)

    # ================================
    # 🔥 CSV 생성
    # ================================
    csv_path = f"{out_dir}/{key_name}_{today_folder}.csv"

    # CSV 헤더는 첫 row의 key 순서 기준
    headers = list(docs[0].keys())

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(docs)

    print(f"✔ CSV 생성 완료: {csv_path}")

    # ================================
    # (JSON 생성은 주석 처리)
    # ================================
    """
    json_path = f"{out_dir}/{key_name}_{today_folder}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=4)
    print(f"✔ JSON 생성 완료: {json_path}")
    """

    return csv_path
