import os
import csv
from datetime import datetime
from common.mongo_util import MongoDB
from datetime import datetime

def get_today_base_date():
    return datetime.today().strftime("%Y%m%d")

OUT_BASE = "D:/STOCK_PROJECT/batch_out"


def export_kodex_etf_collection(col_name: str, key_name: str, base_date: str):
    mongo = MongoDB()
    db = mongo.db
    col = db[col_name]

    # =========================
    # 출력 디렉터리 생성
    # =========================
    today_folder = datetime.utcnow().strftime("%Y%m%d")
    out_dir = f"{OUT_BASE}/{today_folder}"
    os.makedirs(out_dir, exist_ok=True)

    # =========================
    # Mongo 조회
    # =========================
    docs = list(col.find(
        {"base_date": base_date},
        {"_id": 0}
    ))

    if not docs:
        print(f"⚠ {col_name}: base_date={base_date} 데이터 없음")
        mongo.close()
        return None

    # =========================
    # CSV 경로
    # =========================
    csv_path = f"{out_dir}/{key_name}_{base_date.replace('.', '')}.csv"

    headers = list(docs[0].keys())

    # =========================
    # 🔥 CSV 생성 (QUOTE_ALL)
    # =========================
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=headers,
            quoting=csv.QUOTE_ALL   # 🔥 모든 컬럼 문자열 처리
        )
        writer.writeheader()
        writer.writerows(docs)

    print(f"✔ ETF CSV 생성 완료: {csv_path}")

    mongo.close()
    return csv_path


# =========================
# 단독 실행 테스트용
# =========================
if __name__ == "__main__":
    BASE_DATE = get_today_base_date()
    print(f"[INFO] ETF BASE_DATE = {BASE_DATE}")

    export_kodex_etf_collection(
        col_name="kodex_etf_holdings",
        key_name="KODEX_ETF_HOLDINGS",
        base_date=BASE_DATE
    )
