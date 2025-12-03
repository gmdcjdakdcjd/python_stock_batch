# common/mongo_util.py
import os
from dotenv import load_dotenv
from pymongo import MongoClient

# ============================================
# 1) 프로젝트 루트 절대경로로 .env.dev 읽기
#    (어디서 실행해도 절대 안 깨짐)
# ============================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env.dev")

load_dotenv(ENV_PATH)

class MongoDB:
    def __init__(self):
        self.client = MongoClient(os.getenv("MONGO_URI"))
        self.db = self.client[os.getenv("MONGO_DB")]

    def close(self):
        self.client.close()