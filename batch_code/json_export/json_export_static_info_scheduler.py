from batch_code.json_export.export_static_info_json import export_static_collection

if __name__ == "__main__":
    print("=== 종목정보 JSON 생성 스케줄러 시작 ===")

    export_static_collection("indicator_info", "INDICATOR_INFO")
    export_static_collection("bond_info", "BOND_INFO")
    export_static_collection("company_info_kr", "COMPANY_INFO_KR")
    export_static_collection("company_info_us", "COMPANY_INFO_US")
    export_static_collection("etf_info_kr", "ETF_INFO_KR")
    export_static_collection("etf_info_us", "ETF_INFO_US")

    print("=== 완료 ===")


