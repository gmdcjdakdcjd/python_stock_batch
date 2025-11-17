import pandas as pd
from bs4 import BeautifulSoup
import pymysql
import requests
import json
import re


class GoldDailyDBUpdater:
    def __init__(self):
        """DB 연결 + config_fx.json 로드"""
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='0806',
            db='INVESTAR',
            charset='utf8'
        )

        # -----------------------------
        # config_fx.json 로드
        # -----------------------------
        try:
            with open('config_fx.json', 'r', encoding='utf-8') as f:
                config = json.load(f)
                self.pages_to_fetch = config.get("pages_to_fetch", 1)
        except FileNotFoundError:
            self.pages_to_fetch = 1
            with open('config_fx.json', 'w', encoding='utf-8') as f:
                json.dump({"pages_to_fetch": 1}, f, indent=4, ensure_ascii=False)

        print(f"[INFO] GOLD pages_to_fetch = {self.pages_to_fetch}")

    def __del__(self):
        self.conn.close()

    # ----------------------------------------------------------------
    # 1) GOLD 여러 페이지 스크래핑
    # ----------------------------------------------------------------
    def read_gold_daily(self, page=1):
        try:
            url = f"https://finance.naver.com/marketindex/worldDailyQuote.naver?marketindexCd=CMDT_GC&fdtc=2&page={page}"
            headers = {"User-Agent": "Mozilla/5.0"}
            html = requests.get(url, headers=headers).text
            soup = BeautifulSoup(html, "lxml")

            rows = soup.select("table.tbl_exchange.today tbody tr")
            data = []

            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 4:
                    continue

                # 날짜
                date_raw = cols[0].get_text(strip=True)
                if date_raw == "" or date_raw.count('.') != 2:
                    continue
                date = date_raw.replace(".", "-")

                # 종가(close)
                close_raw = cols[1].get_text(strip=True).replace(",", "")
                close = float(close_raw)

                # 전일대비 change_amount
                diff_td = cols[2]
                sign = 1

                img = diff_td.find("img")
                if img and "하락" in img.get("alt", ""):
                    sign = -1

                m = re.search(r"-?\d+\.?\d*", diff_td.get_text(strip=True))
                if not m:
                    continue
                change_amount = sign * float(m.group())

                # 등락률 change_rate
                rate_raw = cols[3].get_text(strip=True)
                rate_raw = rate_raw.replace("%", "").replace("+", "").replace(",", "")
                change_rate = float(rate_raw)

                data.append([date, close, change_amount, change_rate])

            return pd.DataFrame(data,
                                columns=["date", "close", "change_amount", "change_rate"])

        except Exception as e:
            print("Exception (GOLD DAILY page):", e)
            return pd.DataFrame()

    # ----------------------------------------------------------------
    # 2) pages_to_fetch 만큼 전체 수집
    # ----------------------------------------------------------------
    def collect_all_pages(self):
        frames = []

        for page in range(1, self.pages_to_fetch + 1):
            print(f"[INFO] GOLD {page}/{self.pages_to_fetch} 페이지 수집 중...")
            df = self.read_gold_daily(page)
            if df.empty:
                break
            frames.append(df)

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)
        return df_all

    # ----------------------------------------------------------------
    # 3) DB 저장
    # ----------------------------------------------------------------
    def replace_gold_into_db(self, df):
        with self.conn.cursor() as curs:
            df_sorted = df.sort_values("date")

            for r in df_sorted.itertuples():
                sql = f"""
                REPLACE INTO market_indicator
                (code, date, close, change_amount, change_rate)
                VALUES ('GOLD_GLOBAL', '{r.date}', {r.close}, {r.change_amount}, {r.change_rate});
                """
                curs.execute(sql)

        self.conn.commit()
        print(f"[INFO] GOLD {len(df)} rows 저장 완료 (과거 → 최신)")

    # ----------------------------------------------------------------
    # 4) 실행
    # ----------------------------------------------------------------
    def update_gold_daily(self):
        print("[INFO] 국제 금 일별 시세 수집 시작...")
        df = self.collect_all_pages()

        if df is not None and not df.empty:
            self.replace_gold_into_db(df)
            print("[INFO] GOLD 업데이트 완료")
        else:
            print("[WARN] GOLD 데이터 없음")


if __name__ == '__main__':
    updater = GoldDailyDBUpdater()
    updater.update_gold_daily()
