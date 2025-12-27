from datetime import datetime
from pymongo import UpdateOne
from common.mongo_util import MongoDB

mongo = MongoDB()
db = mongo.db
col_summary = db["kodex_etf_summary"]
col_holdings = db["kodex_etf_holdings"]


def get_today_gijunYMD():
    return datetime.today().strftime("%Y.%m.%d")


def to_int(v):
    return int(v) if v not in (None, "", "null") else None


def to_float(v):
    return float(v) if v not in (None, "", "null") else None


def save_etf(api_json):
    base_date = api_json["gijunYMD"]
    now = datetime.utcnow()

    for doc in api_json.get("documentList", []):
        etf_id = doc["fId"]
        pdf_list = doc.get("pdfList", [])

        if not pdf_list:
            continue

        # summary
        col_summary.update_one(
            {"etf_id": etf_id, "base_date": base_date},
            {
                "$set": {
                    "etf_name": doc.get("fNm"),
                    "irp_yn": doc.get("irpYn"),
                    "total_cnt": to_int(pdf_list[0].get("totalCnt")),
                    "updated_at": now
                },
                "$setOnInsert": {
                    "created_at": now
                }
            },
            upsert=True
        )

        # holdings
        ops = []
        for h in pdf_list:
            ops.append(
                UpdateOne(
                    {
                        "etf_id": etf_id,
                        "base_date": base_date,
                        "stock_code": h["itmNo"]
                    },
                    {
                        "$set": {
                                "stock_name": h["secNm"],
                                "holding_qty": to_float(h.get("applyQ")),
                                "current_price": to_int(h.get("curp")),
                                "eval_amount": to_int(h.get("evalA")),
                                "weight_ratio": to_float(h.get("ratio")),
                                "updated_at": now
                            },
                        "$setOnInsert": {
                            "created_at": now
                        }
                    },
                    upsert=True
                )
            )

        if ops:
            col_holdings.bulk_write(ops)


if __name__ == "__main__":
    import requests

    url = "https://www.samsungfund.com/api/v1/kodex/product-document.do"

    gijunYMD = get_today_gijunYMD()
    print(f"[INFO] 기준일자(gijunYMD) = {gijunYMD}")

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    page = 1
    while True:
        params = {
            "pageNo": page,
            "gijunYMD": gijunYMD
        }

        print(f"[INFO] 요청 pageNo={page}, gijunYMD={gijunYMD}")

        r = requests.get(url, params=params, headers=headers)
        r.raise_for_status()

        api_json = r.json()

        if not api_json.get("documentList"):
            print("[INFO] 더 이상 데이터 없음, 종료")
            break

        save_etf(api_json)
        print(f"[INFO] page {page} 저장 완료")

        page += 1

    print("🎉 전체 ETF MongoDB 저장 완료")
