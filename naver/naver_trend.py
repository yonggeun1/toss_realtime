import requests
import pandas as pd
import os
import sys
import time
import signal
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup

# Supabase 연동
try:
    from toss_crawling.supabase_client import supabase
except ImportError:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.append(project_root)
    from toss_crawling.supabase_client import supabase

# [설정] 스냅샷 수집 기준 시간
TARGET_TIMES = ["09:30", "10:00", "11:30", "13:20", "14:30", "15:30"]

def get_korea_now():
    """현재 한국 시간을 반환합니다."""
    return datetime.now(timezone(timedelta(hours=9)))

def delete_old_trend_data():
    """
    오늘(KST 기준) 이전의 시장 동향 데이터를 삭제합니다.
    """
    try:
        now_kst = get_korea_now()
        today_start_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
        # '거래일' 컬럼이 date 형식이므로 날짜 문자열로 비교
        threshold_date = today_start_kst.strftime('%Y-%m-%d')

        print(f"🧹 [시장동향] 오늘({threshold_date}) 이전 데이터 삭제 중...")
        
        # '거래일' < threshold_date 인 데이터 삭제
        supabase.table("naver_market_trend").delete().lt("거래일", threshold_date).execute()
        
        print(f"✅ [시장동향] 지난 데이터 삭제 프로세스 완료")
    except Exception as e:
        print(f"🚨 [시장동향] 지난 데이터 삭제 오류: {e}")

def get_nearest_target_time():
    """현재 시간 기준 가장 최근에 도달한(또는 지나간) 타겟 시간을 반환합니다."""
    now = get_korea_now()
    today_date = now.strftime('%Y-%m-%d')
    current_hm = now.strftime('%H:%M')
    
    target = None
    for t in TARGET_TIMES:
        if current_hm >= t:
            target = t
        else:
            break
    return today_date, target

def is_already_recorded_in_supabase(date, target_time):
    """수파베이스에 해당 날짜와 타겟 시간의 데이터가 이미 존재하는지 확인합니다."""
    try:
        # 한글 컬럼명 "거래일", "거래시간" 사용
        response = supabase.table("naver_market_trend") \
            .select("id") \
            .eq("거래일", date) \
            .eq("거래시간", target_time) \
            .execute()
        # KOSPI, KOSDAQ 두 시장 데이터가 모두 있는지 확인
        return len(response.data) >= 2 
    except Exception as e:
        print(f"🚨 수파베이스 중복 체크 오류: {e}")
        return False

def get_market_trend(market_code):
    """시장(KOSPI/KOSDAQ)의 투자자별 매매동향을 크롤링합니다."""
    url = f"https://finance.naver.com/sise/sise_index.naver?code={market_code}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        dl = soup.find('dl', class_='lst_kos_info')
        
        # 한글 키를 사용하는 딕셔너리 구성
        trend = {'시장': market_code, '개인': 0, '외국인': 0, '기관': 0}
        if dl:
            dds = dl.find_all('dd', class_='dd')
            for dd in dds:
                text = dd.get_text(strip=True)
                val_elem = dd.find('span')
                if val_elem:
                    val = val_elem.get_text(strip=True).replace('억', '').replace(',', '').replace('+', '')
                    if "개인" in text: trend['개인'] = int(val)
                    elif "외국인" in text: trend['외국인'] = int(val)
                    elif "기관" in text: trend['기관'] = int(val)
        return trend
    except Exception as e:
        print(f"❌ {market_code} 매매동향 수집 실패: {e}")
        return None

def main():
    # 1. 이전 날짜 데이터 삭제 (오전 실행 시 권장)
    delete_old_trend_data()

    # 2. 수집 대상 시간 확인
    now_kst = get_korea_now()
    today_date, target_time = get_nearest_target_time()

    if target_time is None:
        print(f"🕒 현재 시각(KST) {now_kst.strftime('%H:%M:%S')} - 첫 번째 타겟 시간({TARGET_TIMES[0]}) 전입니다. 종료합니다.")
        return

    # 3. 이미 수집했는지 확인
    if is_already_recorded_in_supabase(today_date, target_time):
        print(f"✅ {today_date} {target_time} 스냅샷이 이미 존재합니다. 작업을 종료합니다.")
        return

    # 4. 데이터 수집 및 저장
    print(f"📸 [{now_kst.strftime('%H:%M:%S')}] {target_time} 스냅샷 수집 및 수파베이스 저장 시작!")
    insert_data = []
    for m in ['KOSPI', 'KOSDAQ']:
        t_data = get_market_trend(m)
        if t_data:
            t_data['거래일'] = today_date
            t_data['거래시간'] = target_time
            t_data['수집시간'] = now_kst.isoformat()
            insert_data.append(t_data)
    
    if insert_data:
        try:
            supabase.table("naver_market_trend").upsert(insert_data).execute()
            print(f"💾 {target_time} 수파베이스 저장 성공 ({len(insert_data)}건)")
        except Exception as e:
            print(f"❌ 수파베이스 저장 오류: {e}")
    
    print("=== 수집 완료 및 프로세스 종료 ===")

if __name__ == "__main__":
    main()
