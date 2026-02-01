import os
from dotenv import load_dotenv
from supabase import create_client, Client

# .env 파일 로드 (로컬 개발 환경용)
# .env 파일이 없거나 GitHub Actions 환경에서는 시스템 환경 변수를 사용합니다.
load_dotenv()

url = os.getenv("Project_URL", "").strip()
key = os.getenv("Secret_keys", "").strip()

if not url:
    raise ValueError("❌ Project_URL environment variable is missing or empty.")
if not key:
    raise ValueError("❌ Secret_keys environment variable is missing or empty. (Service Role Key required for RLS bypass)")

# Supabase 클라이언트 생성
try:
    supabase: Client = create_client(url, key)
except Exception as e:
    raise RuntimeError(f"❌ Failed to initialize Supabase client: {e}")

