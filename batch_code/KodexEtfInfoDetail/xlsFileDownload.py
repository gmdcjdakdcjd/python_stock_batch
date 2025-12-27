import os
import requests

BASE_DIR = r"D:\STOCK_PROJECT\ETF_INFO_DETIAL_DOWNLAOD"
os.makedirs(BASE_DIR, exist_ok=True)

url = "https://www.samsungfund.com/excel_pdf.do"
params = {
    "fId": "2ETF52",
    "gijunYMD": "20251219"
}

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*"
}

r = requests.get(url, params=params, headers=headers, timeout=30)
r.raise_for_status()

data = r.content

# HTML 응답 차단
if data[:15].lower().startswith(b"<!doctype html") or data[:6].lower().startswith(b"<html"):
    raise Exception("❌ HTML 응답 (파일 아님)")

# 파일 시그니처 판별
if data.startswith(b"%PDF"):
    ext = ".pdf"
elif data.startswith(b"PK\x03\x04"):
    ext = ".xlsx"
elif data.startswith(b"\xD0\xCF\x11\xE0"):
    ext = ".xls"
else:
    raise Exception(f"❌ 알 수 없는 파일 시그니처: {data[:10]}")

filename = f"{params['fId']}_{params['gijunYMD']}{ext}"
file_path = os.path.join(BASE_DIR, filename)

with open(file_path, "wb") as f:
    f.write(data)

print(f"✅ 다운로드 완료: {file_path}")
