from batch_code.json_export.export_strategy_data_json import export_strategy_collection

if __name__ == "__main__":
    print("=== 전략 정보 JSON 생성 시작 ===")

    export_strategy_collection("strategy_result", "STRATEGY_RESULT")
    export_strategy_collection("strategy_detail", "STRATEGY_DETAIL")

    print("=== 완료 ===")
