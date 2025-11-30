import os
import csv
import json
import pandas as pd
from datetime import datetime
from pymongo import MongoClient

OUT_BASE = "D:/STOCK_PROJECT/batch_out"


def export_static_collection(col_name: str, key_name: str):
    client = MongoClient("mongodb://root:0806@localhost:27017/?authSource=admin")
    db = client["investar"]

    col = db[col_name]

    today_folder = datetime.today().strftime("%Y%m%d")
    today_date = datetime.today().strftime("%Y-%m-%d")

    out_dir = f"{OUT_BASE}/{today_folder}"
    os.makedirs(out_dir, exist_ok=True)

    # 🔥 모든 데이터 조회 (_id 제외)
    docs = list(col.find({}, {"_id": 0}))

    if not docs:
        print(f"⚠ {col_name}: 데이터 없음")
        return None

    # ============================================
    # 🔥 last_update = 오늘 날짜인지 체크
    # ============================================
    today_docs = []
    for d in docs:
        last_update = d.get("last_update")
        if not last_update:
            continue

        last_str = str(last_update)[:10]  # YYYY-MM-DD

        if last_str == today_date:
            today_docs.append(d)

    if not today_docs:
        print(f"⚠ {col_name}: 오늘 업데이트된 데이터 없음")
        return None

    print(f"✔ {col_name}: 오늘 업데이트 분량 {len(today_docs)}건 → CSV 생성")

    # today_docs = docs # 전체 데이터 위한 추가
    # print(f"✔ {col_name}: 전체 {len(today_docs)}건 → CSV 생성") # 전체 데이터 위한 추가
    # ============================================
    # 🔥 CSV 저장
    # ============================================
    file_path = f"{out_dir}/{key_name}_{today_folder}.csv"

    # 첫 행 = header
    header = list(today_docs[0].keys())

    with open(file_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        for row in today_docs:
            writer.writerow([row.get(col, "") for col in header])

    print(f"✔ CSV 생성 완료: {file_path}")
    return file_path
