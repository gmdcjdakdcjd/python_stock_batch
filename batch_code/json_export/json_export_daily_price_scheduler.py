from batch_code.json_export.export_daily_price_json import export_daily_price_collection

if __name__ == "__main__":
    export_daily_price_collection("bond_daily_price", "BOND_DAILY_PRICE")
    export_daily_price_collection("daily_price_indicator", "DAILY_PRICE_INDICATOR")
    export_daily_price_collection("daily_price_kr", "DAILY_PRICE_KR")
    export_daily_price_collection("daily_price_us", "DAILY_PRICE_US")
    export_daily_price_collection("etf_daily_price_kr", "ETF_DAILY_PRICE_KR")
    export_daily_price_collection("etf_daily_price_us", "ETF_DAILY_PRICE_US")
