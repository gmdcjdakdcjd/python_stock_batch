import requests

BASE_URL = "https://www.samsungfund.com/api/v1/kodex/product-document.do"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

gijunYMD = "2025.12.19"
page = 1
items = []

while True:
    params = {
        "pageNo": page,
        "gijunYMD": gijunYMD
    }

    r = requests.get(BASE_URL, params=params, headers=HEADERS)
    r.raise_for_status()

    data = r.json()
    doc_list = data.get("documentList", [])

    if not doc_list:
        break

    for row in doc_list:
        fId = row["fId"]
        fNm = row.get("fNm", "")

        items.append({
            "fId": fId,
            "fNm": fNm,
            "gijunYMD": data["gijunYMD"],
            "download_url": f"https://www.samsungfund.com/excel_pdf.do?fId={fId}&gijunYMD={data['gijunYMD']}",
            "filename": f"ETF_{fId}_{data['gijunYMD']}.xlsx"
        })

    page += 1

print("총 수집 ETF 수:", len(items))  # 👉 224
