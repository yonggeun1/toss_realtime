import os
from dotenv import load_dotenv
from supabase import create_client, Client

# .env 파일 로드 (로컬 개발 환경용)
load_dotenv()

# Supabase 설정 (권한 문제를 피하기 위해 Service Role Key인 'Secret_keys'를 사용합니다)
url = os.getenv("Project_URL", "").strip()
key = os.getenv("Secret_keys", "").strip()

if not url:
    raise ValueError("❌ Project_URL environment variable is missing or empty.")
if not key:
    raise ValueError("❌ Secret_keys environment variable is missing or empty. (RLS 우회를 위해 Service Role Key가 필요합니다)")

# Supabase 클라이언트 생성 (Secret Key를 사용하여 모든 테이블에 대한 CRUD 권한을 확보)
try:
    supabase: Client = create_client(url, key)
except Exception as e:
    raise RuntimeError(f"❌ Supabase 클라이언트 초기화 실패: {e}")


import pandas as pd
from datetime import datetime, timedelta

def load_toss_data_from_supabase():
    """
    Supabase에서 가장 최근 수집된 날짜의 데이터를 로드하여 DataFrame으로 반환합니다.
    """
    try:
        # 1. 가장 최근 수집된 날짜 확인
        res = supabase.table("toss_realtime_top100") \
            .select("collected_at") \
            .order("collected_at", desc=True) \
            .limit(1) \
            .execute()

        if not res.data:
            print("🚨 Supabase에 데이터가 없습니다.")
            return None

        latest_timestamp = res.data[0]['collected_at']
        # 'T' 또는 공백으로 분리하여 날짜 부분만 추출 (Timestamp 대응)
        target_date = latest_timestamp.replace('T', ' ').split(' ')[0]
        print(f"📅 가장 최근 데이터 날짜: {target_date} (데이터 로드 중...)")

        # 해당 날짜의 데이터 범위 설정 (UTC 기준일 수 있으므로 하루를 포함)
        start_date = target_date
        end_date_dt = datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)
        end_date = end_date_dt.strftime("%Y-%m-%d")

        # 2. 해당 날짜 데이터 쿼리
        response = supabase.table("toss_realtime_top100") \
            .select("*") \
            .gte("collected_at", start_date) \
            .lt("collected_at", end_date) \
            .execute()

        if not response.data:
            print(f"🚨 {target_date} 날짜의 데이터를 가져오지 못했습니다.")
            return None

        df = pd.DataFrame(response.data)

        # 3. 데이터 전처리
        # 매수(buy)는 양수, 매도(sell)는 음수로 변환
        df['final_amount'] = df.apply(lambda x: x['amount'] if x['ranking_type'] == 'buy' else -x['amount'], axis=1)

        # 필요한 컬럼만 선택 및 이름 변경
        df_processed = df[['investor', 'stock_name', 'stock_code', 'final_amount', 'collected_at']].copy()
        
        # 중복 제거: 동일 투자자, 종목, 매매타입에 대해 가장 마지막에 수집된 데이터만 사용
        # (실시간 크롤링이 누적될 경우 최신 스냅샷을 사용하기 위함)
        df_dedup = df.sort_values(by=['investor', 'stock_code', 'collected_at']) \
            .drop_duplicates(subset=['investor', 'stock_code', 'ranking_type'], keep='last')

        # 매수/매도 합산 (같은 종목에 대해 매수/매도 모두 있을 수 있음)
        df_total = df_dedup.groupby(['investor', 'stock_name', 'stock_code'], as_index=False)['final_amount'].sum()
        
        # 컬럼명 매핑 (기존 로직과의 호환성을 위해)
        df_total.rename(columns={
            'investor': '투자자', 
            'stock_name': '종목명', 
            'stock_code': '종목코드', 
            'final_amount': '금액'
        }, inplace=True)

        print(f"✅ Supabase 데이터 로드 및 통합 완료: {len(df_total)}건 (기준일: {target_date})")
        return df_total

    except Exception as e:
        print(f"🚨 Supabase 데이터 로드 중 에러 발생: {e}")
        return None

def load_etf_pdf_from_supabase():
    """
    Supabase의 etf_pdf 테이블에서 데이터를 로드하여 DataFrame으로 반환합니다.
    데이터가 많을 경우 페이지네이션을 통해 모두 가져옵니다.
    """
    try:
        all_data = []
        limit = 1000
        offset = 0
        
        print("⏳ Supabase ETF PDF 데이터 로드 중...", end='', flush=True)
        
        while True:
            # 페이지네이션: offset부터 limit만큼 가져오기
            response = supabase.table("ETF_PDF").select("*").range(offset, offset + limit - 1).execute()
            
            if not response.data:
                break
                
            all_data.extend(response.data)
            
            # 가져온 데이터가 limit보다 적으면 더 이상 데이터가 없다는 뜻
            if len(response.data) < limit:
                break
            
            offset += limit
            print(".", end='', flush=True)

        print(f"\n✅ Supabase ETF PDF 데이터 로드 완료: {len(all_data)}건")

        if not all_data:
            print("🚨 Supabase의 'ETF_PDF' 테이블에 데이터가 없습니다.")
            return None

        df_pdf = pd.DataFrame(all_data)
        
        # 컬럼명 통일 (DB 컬럼명 -> 로직용 컬럼명)
        column_mapping = {
            'etf_code': 'ETF종목코드',
            'etf_name': 'ETF종목명',
            'holdings_code': '구성종목코드',
            'holdings_name': '구성종목명',
            'holdings_weight': '구성비중(%)'
        }
        
        # 존재하는 컬럼만 변경
        existing_mapping = {k: v for k, v in column_mapping.items() if k in df_pdf.columns}
        if existing_mapping:
            df_pdf.rename(columns=existing_mapping, inplace=True)

        # 추가적인 컬럼명 호환 처리 (시가총액기준구성비율 등)
        if '시가총액기준구성비율' in df_pdf.columns:
            df_pdf.rename(columns={'시가총액기준구성비율': '구성비중(%)'}, inplace=True)
        
        if '구성비중(%)' not in df_pdf.columns:
            print(f"⚠️ '구성비중(%)' 컬럼을 찾을 수 없습니다. (컬럼: {list(df_pdf.columns)})")

        df_pdf['구성비중(%)'] = pd.to_numeric(df_pdf['구성비중(%)'], errors='coerce').fillna(0)
        
        # 종목코드 포맷팅 (6자리 문자열)
        if 'ETF종목코드' in df_pdf.columns:
            df_pdf['ETF종목코드'] = df_pdf['ETF종목코드'].astype(str).str.zfill(6)
        if '구성종목코드' in df_pdf.columns:
            df_pdf['구성종목코드'] = df_pdf['구성종목코드'].astype(str).str.zfill(6)

        return df_pdf

    except Exception as e:
        print(f"🚨 Supabase ETF PDF 로드 중 에러 발생: {e}")
        return None

def save_score_to_supabase(df):
    """
    계산된 YG Score 결과를 Supabase 'score' 테이블에 저장(Upsert)합니다.
    모든 데이터를 강제로 업데이트합니다.
    """
    try:
        if df is None or df.empty:
            print("⚠️ 저장할 데이터가 없습니다.")
            return

        # 1. 저장할 데이터 준비 (컬럼 매핑)
        df_new = df.copy()
        df_new.rename(columns={
            'ETF종목코드': 'etf_code',
            'ETF종목명': 'etf_name',
            'YG_SCORE_합계': 'total_score',
            'YG_SCORE_외국인': 'foreign_score',
            'YG_SCORE_기관': 'institution_score',
            '종목수': 'holdings_count'
        }, inplace=True)
        
        # 2. 업로드할 데이터 구성
        # [수정] 한국 시간(KST) 기준 Timestamp 사용
        kst_now = datetime.utcnow() + timedelta(hours=9)
        current_time = kst_now.isoformat()
        
        upsert_cols = ['etf_code', 'etf_name', 'total_score', 'foreign_score', 'institution_score', 'holdings_count', 'updated_at']
        
        # updated_at 추가
        df_new['updated_at'] = current_time
        
        # 필요한 컬럼만 선택하여 dict 리스트로 변환
        data_to_upsert = df_new[upsert_cols].to_dict(orient='records')

        # 3. 데이터 저장 (전체 Upsert)
        # 데이터가 많을 경우 Supabase 요청 제한에 걸릴 수 있으므로 1000개씩 분할 처리
        batch_size = 1000
        total_count = len(data_to_upsert)
        
        print(f"🚀 총 {total_count}건 데이터 저장을 시작합니다...")
        
        for i in range(0, total_count, batch_size):
            batch = data_to_upsert[i:i+batch_size]
            res = supabase.table("score").upsert(batch).execute()
            print(f"   - {i} ~ {i+len(batch)}건 저장 완료")
            
        print(f"✅ Supabase 'score' 테이블 전체 업데이트 완료: {total_count}건")

    except Exception as e:
        print(f"🚨 Supabase 저장 중 에러 발생: {e}")

def delete_old_scores():
    """
    score 테이블에서 오늘(KST 기준) 이전의 데이터를 모두 삭제합니다.
    즉, 실행일 당일의 데이터만 남깁니다.
    """
    try:
        # KST 기준 오늘 시작 시간 계산
        now_utc = datetime.utcnow()
        now_kst = now_utc + timedelta(hours=9)
        today_start_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # 비교를 위해 UTC로 변환 (updated_at은 UTC로 저장됨)
        # [수정] save_score_to_supabase가 KST ISO 포맷으로 저장하므로, threshold도 KST ISO 포맷으로 설정
        threshold_str = today_start_kst.isoformat()

        # 삭제 쿼리: updated_at < threshold
        response = supabase.table("score").delete().lt("updated_at", threshold_str).execute()
        
        deleted_count = len(response.data) if response.data else 0
        if deleted_count > 0:
            print(f"🧹 지난 Score 데이터 삭제 완료: {deleted_count}건 (기준: {today_start_kst.strftime('%Y-%m-%d')} KST 이전)")
            
    except Exception as e:
        print(f"🚨 지난 데이터 삭제 오류: {e}")
