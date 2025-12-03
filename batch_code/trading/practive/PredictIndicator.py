import os
os.environ["CUDA_VISIBLE_DEVICES"] = "-1"

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pymongo import MongoClient

from tensorflow.keras import Sequential
from tensorflow.keras.layers import Dense, LSTM, Dropout


# ---------------------------------------------------------
# 1) MongoDB에서 특정 기간의 USD 데이터 불러오기
# ---------------------------------------------------------
def load_usd_between(target_date, past_days):
    mongo = MongoClient("mongodb://root:0806@localhost:27017/?authSource=admin")
    col = mongo["investar"]["daily_price_indicator"]

    td = datetime.strptime(target_date, "%Y-%m-%d")
    start_date = td - timedelta(days=past_days)

    cursor = col.find(
        {
            "code": "USD",
            "date": {"$gte": start_date, "$lte": td}
        },
        {"_id": 0, "date": 1, "close": 1}
    ).sort("date", 1)

    df = pd.DataFrame(list(cursor))
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    print(f"[INFO] Loaded {len(df)} rows ({start_date.date()} ~ {td.date()})")
    return df


# ---------------------------------------------------------
# 2) MinMax Scaling
# ---------------------------------------------------------
def MinMaxScaler(data):
    numerator = data - np.min(data, axis=0)
    denominator = np.max(data, axis=0) - np.min(data, axis=0)
    return numerator / (denominator + 1e-7)


# ---------------------------------------------------------
# 3) 미래 30일 예측 (Dense 30개 output)
# ---------------------------------------------------------
def predict_usdkrw(target_date, past_days=120, future_days=30):
    df = load_usd_between(target_date, past_days)

    window = 14

    df_scaled = MinMaxScaler(df[['close']])
    scaled_values = df_scaled.values

    # sliding window dataset
    data_x, data_y = [], []
    for i in range(len(scaled_values) - window - (future_days-1)):
        data_x.append(scaled_values[i:i + window])
        data_y.append(scaled_values[i + window : i + window + future_days].flatten())

    data_x = np.array(data_x)
    data_y = np.array(data_y)

    # ---------------------------------------------------------
    # LSTM 모델: Dense(30) 한 번에 예측
    # ---------------------------------------------------------
    model = Sequential()
    model.add(LSTM(64, activation='relu', input_shape=(window, 1)))
    model.add(Dropout(0.2))
    model.add(Dense(future_days))  # 30일을 한번에 예측

    model.compile(optimizer='adam', loss='mse')

    print("[INFO] Training model...")
    model.fit(data_x, data_y, epochs=50, batch_size=16, verbose=1)

    # 마지막 window 14일 → 미래 30일 예측
    last_window = scaled_values[-window:].reshape(1, window, 1)
    future_scaled = model.predict(last_window, verbose=0)[0]

    # 역변환
    min_close = df["close"].min()
    max_close = df["close"].max()
    future_prices = future_scaled * (max_close - min_close) + min_close

    # ---------------------------------------------------------
    # 실제 미래 데이터 로드
    # ---------------------------------------------------------
    mongo = MongoClient("mongodb://root:0806@localhost:27017/?authSource=admin")
    col = mongo["investar"]["daily_price_indicator"]

    td = datetime.strptime(target_date, "%Y-%m-%d")
    end_date = td + timedelta(days=future_days)

    cursor = col.find(
        {"code": "USD", "date": {"$gt": td, "$lte": end_date}},
        {"_id": 0, "date": 1, "close": 1}
    ).sort("date", 1)

    df_real = pd.DataFrame(list(cursor))
    df_real["date"] = df_real["date"].dt.strftime("%Y-%m-%d")

    # ---------------------------------------------------------
    # Console 출력
    # ---------------------------------------------------------
    print("\n====== 예측된 미래 환율 ======")
    for i, v in enumerate(future_prices, 1):
        print(f"{i}일 후 예상 환율: {v:.2f} KRW")

    print("\n====== 실제 값 ======")
    for _, row in df_real.iterrows():
        print(f"{row['date']} → {row['close']} KRW")

    return future_prices, df_real


# 실행
if __name__ == "__main__":
    predict_usdkrw(
        target_date="2025-11-15",
        past_days=180,
        future_days=30
    )
