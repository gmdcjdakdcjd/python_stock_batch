# ============================================================
# 기존 MariaDB 저장 코드 → 전부 주석 처리
# ============================================================

import pymysql
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))


def now_kst_str():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


# ------------------------------------------------------------
# MariaDB 버전 (사용 안 함 → 전체 주석)
# ------------------------------------------------------------
"""
def save_strategy_summary(strategy_name, signal_date, signal_type,
                          total_return=None, total_risk=None, total_sharpe=None):
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='0806',
        db='INVESTAR',
        charset='utf8'
    )
    try:
        with conn.cursor() as curs:
            sql = '''
                INSERT INTO strategy_result
                (strategy_name, signal_date, signal_type, total_return, total_risk, total_sharpe)
                VALUES (%s, %s, %s, %s, %s, %s)
            '''
            curs.execute(sql, (
                strategy_name,
                signal_date,
                signal_type,
                round(float(total_return), 6) if total_return is not None else None,
                round(float(total_risk), 6) if total_risk is not None else None,
                round(float(total_sharpe), 6) if total_sharpe is not None else None
            ))
            conn.commit()
            return curs.lastrowid
    finally:
        conn.close()


def save_strategy_signal(result_id, code, name,
                         action=None, price=None, old_price=None,
                         returns=None, rank_order=None, signal_date=None):

    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='0806',
        db='INVESTAR',
        charset='utf8'
    )
    try:
        with conn.cursor() as curs:
            sql = '''
                INSERT INTO strategy_signal
                (result_id, signal_date, code, name, action, price, old_price, returns, rank_order)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            curs.execute(sql, (
                result_id,
                signal_date,
                code,
                name,
                action,
                float(price) if price is not None else None,
                float(old_price) if old_price is not None else None,
                round(float(returns), 6) if returns is not None else None,
                int(rank_order) if rank_order is not None else None
            ))
        conn.commit()
    finally:
        conn.close()
"""

# ============================================================
# MongoDB 버전 (실제 사용)
# ============================================================

client = MongoClient("mongodb://root:0806@localhost:27017/?authSource=admin")
mdb = client["investar"]

col_result = mdb["strategy_result"]
col_detail = mdb["strategy_detail"]  # ← 컬렉션명도 일관되게 detail


# -------------------------------------------------------------------------
# 1) SUMMARY 저장
# -------------------------------------------------------------------------
def save_strategy_summary(strategy_name, signal_date, total_data):
    """전략 요약 저장 → MongoDB"""

    doc = {
        "strategy_name": strategy_name,
        "signal_date": signal_date,
        "signal_type": strategy_name,  # 공용
        "total_data": int(total_data),
        "created_at": datetime.now(KST)
    }

    result = col_result.insert_one(doc)
    return result.inserted_id  # ObjectId 반환


# -------------------------------------------------------------------------
# 2) DETAIL 저장 (기존 save_strategy_signal → save_strategy_detail)
# -------------------------------------------------------------------------
def save_strategy_detail(
        result_id,
        code,
        name,
        action,
        price,
        prev_close,
        diff,
        volume,
        signal_date,
        special_value
):

    def to_basic(x):
        if hasattr(x, "item"):
            return x.item()
        return x

    doc = {
        "result_id": str(result_id),  # 🔥 반드시 문자열로!
        "signal_date": signal_date,
        "code": str(code),
        "name": name,
        "action": action,
        "price": to_basic(price),
        "prev_close": to_basic(prev_close),
        "diff": to_basic(diff),
        "volume": to_basic(volume),
        "special_value": to_basic(special_value),
        "created_at": datetime.now(KST)
    }

    col_detail.update_one(
        {
            "signal_date": signal_date,
            "code": str(code),
            "action": action
        },
        {"$set": doc},
        upsert=True
    )

