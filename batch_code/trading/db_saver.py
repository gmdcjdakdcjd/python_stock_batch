import pymysql

def save_strategy_summary(strategy_name, signal_date, signal_type,
                          total_return=None, total_risk=None, total_sharpe=None):
    """전략 요약 저장 (strategy_result 테이블)"""
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='0806',
        db='INVESTAR',
        charset='utf8'
    )
    try:
        with conn.cursor() as curs:
            sql = """
                INSERT INTO strategy_result
                (strategy_name, signal_date, signal_type, total_return, total_risk, total_sharpe)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            curs.execute(sql, (
                strategy_name,
                signal_date,
                signal_type,
                round(float(total_return), 6) if total_return is not None else None,
                round(float(total_risk), 6) if total_risk is not None else None,
                round(float(total_sharpe), 6) if total_sharpe is not None else None
            ))
            conn.commit()
            return curs.lastrowid  # ✅ 방금 저장한 result_id 반환
    finally:
        conn.close()


def save_strategy_signal(result_id, code, name,
                         action=None, price=None, old_price=None,
                         returns=None, rank_order=None, signal_date=None):
    """전략별 세부 결과 저장 (strategy_signal 테이블)"""
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='0806',
        db='INVESTAR',
        charset='utf8'
    )
    try:
        with conn.cursor() as curs:
            sql = """
                INSERT INTO strategy_signal
                (result_id, signal_date, code, name, action, price, old_price, returns, rank_order)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
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
