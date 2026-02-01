import os
from dotenv import load_dotenv
from supabase import create_client, Client

# .env 파일 로드 (로컬 개발 환경용)
# .env 파일이 없거나 GitHub Actions 환경에서는 시스템 환경 변수를 사용합니다.
load_dotenv()

url = os.getenv("Project_URL")
key = os.getenv("Publishable_API_Key")

if not url:
    raise ValueError("Project_URL 환경 변수가 설정되지 않았습니다. .env 파일 또는 GitHub Secrets를 확인해주세요.")
if not key:
    raise ValueError("Publishable_API_Key 환경 변수가 설정되지 않았습니다. .env 파일 또는 GitHub Secrets를 확인해주세요.")

# Supabase 클라이언트 생성
supabase: Client = create_client(url, key)
