import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import pandas as pd
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


def get_kst_now():
    """현재 한국 표준시(KST, UTC+9)를 타임존 정보와 함께 반환합니다."""
    return datetime.now(timezone(timedelta(hours=9)))

def check_market_open(today_str: str) -> bool:
    """오늘(today_str, YYYY-MM-DD) 프리마켓 ETF 데이터가 있으면 True를 반환합니다."""
    res = supabase.table("naver_premarket_etf") \
        .select("etf_code") \
        .gte("updated_at", today_str) \
        .limit(1) \
        .execute()
    return bool(res.data)

def load_toss_data_from_supabase():
    """
    Supabase에서 가장 최근 수집된 날짜의 데이터를 로드하여 DataFrame으로 반환합니다.
    (데이터프레임과 데이터의 실제 수집 시각(collected_at)을 함께 반환)
    """
    try:
        # 1. 가장 최근 수집된 날짜 확인
        res = supabase.table("toss_yg_score_stk") \
            .select("collected_at") \
            .order("collected_at", desc=True) \
            .limit(1) \
            .execute()

        if not res.data:
            print("🚨 Supabase에 데이터가 없습니다.")
            return None, None

        latest_timestamp = res.data[0]['collected_at']
        # 'T' 또는 공백으로 분리하여 날짜 부분만 추출 (Timestamp 대응)
        target_date = latest_timestamp.replace('T', ' ').split(' ')[0]
        print(f"📅 가장 최근 데이터 날짜: {target_date} (데이터 로드 중...)")

        # 해당 날짜의 데이터 범위 설정 (KST 기준)
        start_date = target_date
        end_date_dt = datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)
        end_date = end_date_dt.strftime("%Y-%m-%d")

        # 2. 해당 날짜 데이터 쿼리 (페이지네이션 적용)
        all_data = []
        limit = 1000
        offset = 0
        
        print(f"⏳ 데이터 로드 중 (Range: {start_date} ~ {end_date})...", end='', flush=True)

        while True:
            response = supabase.table("toss_yg_score_stk") \
                .select("*") \
                .gte("collected_at", start_date) \
                .lt("collected_at", end_date) \
                .order("id") \
                .range(offset, offset + limit - 1) \
                .execute()

            if not response.data:
                break
            
            all_data.extend(response.data)
            
            if len(response.data) < limit:
                break
                
            offset += limit
            print(".", end='', flush=True)
            
        print(f"\n✅ 데이터 로드 완료: 총 {len(all_data)}건")

        if not all_data:
            print(f"🚨 {target_date} 날짜의 데이터를 가져오지 못했습니다.")
            return None, None

        df = pd.DataFrame(all_data)
        
        # 실제 데이터 중 가장 최신 수집 시각 추출
        actual_latest_at = df['collected_at'].max()

        # 3. 데이터 전처리
        # 매수(buy)는 양수, 매도(sell)는 음수로 변환
        df['final_amount'] = df.apply(lambda x: x['amount'] if x['ranking_type'] == 'buy' else -x['amount'], axis=1)

        # 필요한 컬럼만 선택 및 이름 변경
        df_processed = df[['investor', 'stock_name', 'stock_code', 'final_amount', 'collected_at']].copy()
        
        # 중복 제거: 동일 투자자, 종목, 매매타입에 대해 가장 마지막에 수집된 데이터만 사용
        # (실시간 크롤링이 누적될 경우 최신 스냅샷을 사용하기 위함)
        df_dedup = df.sort_values(by=['investor', 'stock_code', 'stock_name', 'collected_at']) \
            .drop_duplicates(subset=['investor', 'stock_code', 'stock_name', 'ranking_type'], keep='last')

        # 매수/매도 합산 (같은 종목에 대해 매수/매도 모두 있을 수 있음)
        df_total = df_dedup.groupby(['investor', 'stock_name', 'stock_code'], as_index=False)['final_amount'].sum()
        
        # 컬럼명 매핑 (기존 로직과의 호환성을 위해)
        df_total.rename(columns={
            'investor': '투자자', 
            'stock_name': '종목명', 
            'stock_code': '종목코드', 
            'final_amount': '금액'
        }, inplace=True)

        print(f"✅ Supabase 데이터 로드 및 통합 완료: {len(df_total)}건 (기준시각: {actual_latest_at})")
        return df_total, actual_latest_at

    except Exception as e:
        print(f"🚨 Supabase 데이터 로드 중 에러 발생: {e}")
        return None, None

def load_etf_pdf_from_supabase():
    """
    Supabase의 etf_pdf 테이블에서 데이터를 로드하여 DataFrame으로 반환합니다.
    """
    try:
        all_data = []
        limit = 1000
        offset = 0
        
        print("⏳ Supabase ETF PDF 데이터 로드 중...", end='', flush=True)
        
        while True:
            response = supabase.table("ETF_PDF").select("*").range(offset, offset + limit - 1).execute()
            
            if not response.data:
                break
                
            all_data.extend(response.data)
            
            if len(response.data) < limit:
                break
            
            offset += limit
            print(".", end='', flush=True)

        print(f"\n✅ Supabase ETF PDF 데이터 로드 완료: {len(all_data)}건")

        if not all_data:
            print("🚨 Supabase의 'ETF_PDF' 테이블에 데이터가 없습니다.")
            return None

        df_pdf = pd.DataFrame(all_data)
        
        column_mapping = {
            'etf_code': 'ETF종목코드',
            'etf_name': 'ETF종목명',
            'holdings_code': '구성종목코드',
            'holdings_name': '구성종목명',
            'holdings_weight': '구성비중(%)'
        }
        
        existing_mapping = {k: v for k, v in column_mapping.items() if k in df_pdf.columns}
        if existing_mapping:
            df_pdf.rename(columns=existing_mapping, inplace=True)

        if '시가총액기준구성비율' in df_pdf.columns:
            df_pdf.rename(columns={'시가총액기준구성비율': '구성비중(%)'}, inplace=True)
        
        df_pdf['구성비중(%)'] = pd.to_numeric(df_pdf['구성비중(%)'], errors='coerce').fillna(0)
        
        if 'ETF종목코드' in df_pdf.columns:
            df_pdf['ETF종목코드'] = df_pdf['ETF종목코드'].astype(str).str.zfill(6)
        if '구성종목코드' in df_pdf.columns:
            df_pdf['구성종목코드'] = df_pdf['구성종목코드'].astype(str).str.zfill(6)

        return df_pdf

    except Exception as e:
        print(f"🚨 Supabase ETF PDF 로드 중 에러 발생: {e}")
        return None

def save_score_to_supabase(df, target_time=None):
    """
    계산된 YG Score 결과를 Supabase 'toss_yg_score_etf' 테이블에 저장(Upsert)합니다.
    target_time이 제공되면 해당 시간을 updated_at으로 사용하고, 없으면 현재 KST 시간을 사용합니다.
    """
    try:
        if df is None or df.empty:
            print("⚠️ 저장할 데이터가 없습니다.")
            return

        df_new = df.copy()
        df_new.rename(columns={
            'ETF종목코드': 'etf_code',
            'ETF종목명': 'etf_name',
            'YG_SCORE_합계': 'total_score',
            'YG_SCORE_외국인': 'foreign_score',
            'YG_SCORE_기관': 'institution_score',
            '종목수': 'holdings_count'
        }, inplace=True)
        
        # updated_at 설정 (타임존 정보 포함)
        if target_time:
            current_time = target_time
        else:
            current_time = get_kst_now().isoformat()
        
        upsert_cols = ['etf_code', 'etf_name', 'total_score', 'foreign_score', 'institution_score', 'holdings_count', 'updated_at']
        df_new['updated_at'] = current_time
        
        data_to_upsert = df_new[upsert_cols].to_dict(orient='records')

        batch_size = 1000
        total_count = len(data_to_upsert)
        
        print(f"🚀 총 {total_count}건 데이터 저장을 시작합니다...")
        
        for i in range(0, total_count, batch_size):
            batch = data_to_upsert[i:i+batch_size]
            res = supabase.table("toss_yg_score_etf").upsert(batch, on_conflict="etf_code, updated_at").execute()
            print(f"   - {i} ~ {i+len(batch)}건 저장 완료")
            
        print(f"✅ Supabase 'toss_yg_score_etf' 테이블 전체 업데이트 완료: {total_count}건")

    except Exception as e:
        print(f"🚨 Supabase 저장 중 에러 발생: {e}")

def delete_old_scores():
    """
    toss_yg_score_etf 및 toss_yg_score_skt 테이블에서 오늘(KST 기준) 이전의 데이터를 모두 삭제합니다.
    """
    try:
        now_kst = get_kst_now()
        today_start_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
        threshold_str = today_start_kst.isoformat()

        # 삭제 쿼리 1: toss_yg_score_etf 테이블
        response = supabase.table("toss_yg_score_etf").delete().lt("updated_at", threshold_str).execute()
        
        deleted_count = len(response.data) if response.data else 0
        if deleted_count > 0:
            print(f"🧹 지난 Score 데이터 삭제 완료: {deleted_count}건 (기준: {today_start_kst.strftime('%Y-%m-%d')} KST 이전)")

        # 삭제 쿼리 2: toss_yg_score_stk 테이블
        response_top = supabase.table("toss_yg_score_stk").delete().lt("collected_at", threshold_str).execute()

        deleted_count_top = len(response_top.data) if response_top.data else 0
        if deleted_count_top > 0:
            print(f"🧹 지난 STK 데이터 삭제 완료: {deleted_count_top}건")
            
    except Exception as e:
        print(f"🚨 지난 데이터 삭제 오류: {e}")

