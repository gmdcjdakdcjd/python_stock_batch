from datetime import datetime

from batch_code.json_export.export_kodex_etf_csv import export_kodex_etf_collection


def get_today_gijunYMD():
    return datetime.today().strftime("%Y%m%d")


if __name__ == "__main__":
    gijunYMD = get_today_gijunYMD()
    print(f"[INFO] ETF 기준일자 = {gijunYMD}")

    export_kodex_etf_collection(
        "kodex_etf_summary",
        "KODEX_ETF_SUMMARY",
        gijunYMD
    )

    export_kodex_etf_collection(
        "kodex_etf_holdings",
        "KODEX_ETF_HOLDINGS",
        gijunYMD
    )
