from ETFMarketDBUS import MarketDB

def main():
    db = MarketDB()

    print("===== BlackRock ETF 기본 정보 =====")
    print("총 ETF 수:", len(db.codes))

    if len(db.codes) == 0:
        print("⚠ 데이터 없음")
        return

    sample = list(db.codes.keys())[0]
    print("샘플:", sample, db.codes[sample])

    print("\n===== BlackRock ETF 시세 테스트 =====")
    df = db.getDailyPrice(sample, "2025-11-17", "2025-11-19")

    if df is None:
        print("⚠ 시세 없음")
    else:
        print(df.head())
        print("총 데이터 수:", len(df))

if __name__ == "__main__":
    main()
