import requests
from bs4 import BeautifulSoup
import re
import sys
import os
import time
import signal
from datetime import datetime, timedelta

# 전역 변수로 종료 요청 상태 관리
stop_requested = False

def signal_handler(sig, frame):
    global stop_requested
    print(f"\n🛑 종료 신호({sig})를 수신했습니다. 현재 진행 중인 수집 및 계산을 마치고 안전하게 종료합니다...")
    stop_requested = True

# 종료 신호(Ctrl+C 등) 연결
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Supabase 연동
try:
    from toss_crawling.supabase_client import supabase
except ImportError:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.append(project_root)
    from toss_crawling.supabase_client import supabase

def delete_old_naver_data():
    """
    오늘(KST 기준) 이전의 네이버 관련 데이터를 모두 삭제합니다.
    """
    try:
        now_utc = datetime.utcnow()
        now_kst = now_utc + timedelta(hours=9)
        today_start_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
        threshold_str = today_start_kst.isoformat()

        print(f"🧹 [네이버] 오늘({today_start_kst.strftime('%Y-%m-%d')}) 이전 데이터 삭제 중...")
        
        # 원천 데이터 삭제 (오늘 이전 것)
        supabase.table("naver_realtime_stk").delete().lt("collected_at", threshold_str).execute()
        
        # ETF 히스토리 데이터 삭제 (오늘 이전 것)
        supabase.table("naver_realtime_etf_history").delete().lt("collected_at", threshold_str).execute()
        
        print(f"✅ [네이버] 지난 데이터 삭제 프로세스 완료")
    except Exception as e:
        print(f"🚨 [네이버] 지난 데이터 삭제 오류: {e}")

def get_naver_sise(url, market_name, type_name, now_kst):
    """
    네이버 증권 시세 정보 크롤링 (상승/하락)
    """
    print(f"🚀 [{market_name} {type_name}] 크롤링 중: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = 'euc-kr'
        
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', class_='type_2')
        if not table:
            print(f"❌ 테이블을 찾을 수 없습니다: {market_name} {type_name}")
            return []
            
        rows = table.find_all('tr')
        collected_data = []
        
        for row in rows:
            tds = row.find_all('td')
            if len(tds) < 10: continue
            
            a_tag = tds[1].find('a')
            if not a_tag: continue
            
            stk_nm = a_tag.text.strip()
            href = a_tag.get('href', '')
            code_match = re.search(r'code=(\d{6})', href)
            stk_cd = code_match.group(1) if code_match else ""
            
            close_pric_str = tds[2].text.strip().replace(',', '')
            close_pric = float(close_pric_str) if close_pric_str else 0.0
            
            pre_str = tds[3].text.strip().replace(',', '')
            pre_str = re.sub(r'[^0-9.]', '', pre_str)
            pre = float(pre_str) if pre_str else 0.0
            
            flu_rt_str = tds[4].text.strip().replace('%', '').replace(',', '').replace('+', '')
            flu_rt = float(flu_rt_str) if flu_rt_str else 0.0
            
            trde_qty_str = tds[5].text.strip().replace(',', '')
            trde_qty = int(trde_qty_str) if trde_qty_str else 0
            
            if stk_cd:
                if "하락" in type_name and flu_rt > 0:
                    flu_rt = -flu_rt
                    pre = -pre
                
                record = {
                    "stk_cd": stk_cd,
                    "stk_nm": stk_nm,
                    "close_pric": close_pric,
                    "pre": pre,
                    "flu_rt": flu_rt,
                    "trde_qty": trde_qty,
                    "market": market_name,
                    "type": type_name,
                    "collected_at": now_kst
                }
                collected_data.append(record)
                
        return collected_data
        
    except Exception as e:
        print(f"❌ 오류 발생 ({market_name} {type_name}): {e}")
        return []

def main():
    # 인자 확인 (세션 구분)
    is_morning = "morning" in sys.argv
    is_afternoon = "afternoon" in sys.argv

    # 시작 전 이전 날짜 데이터 삭제
    # 오전 세션이거나 일반 실행일 때만 삭제 수행
    if not is_afternoon:
        delete_old_naver_data()

    # 루프 설정
    start_hour, start_minute = 8, 50
    end_hour, end_minute = 15, 20
    
    if is_morning:
        end_hour, end_minute = 12, 0
    elif is_afternoon:
        end_hour, end_minute = 15, 20
    
    print(f"=== 네이버 실시간 시세 수집 루프 시작 (세션: {'오전' if is_morning else '오후' if is_afternoon else '기본'}, 종료 예정: {end_hour:02d}:{end_minute:02d}) ===")

    while True:
        now = datetime.utcnow() + timedelta(hours=9)
        current_time_str = now.strftime("%H%M")
        
        # 시작 시간 체크
        if current_time_str < f"{start_hour:02d}{start_minute:02d}":
            print(f"🕒 현재 시각(KST) {now.strftime('%H:%M:%S')} - 시작 전({start_hour:02d}:{start_minute:02d})입니다. 대기 중...", end='\r')
            time.sleep(30)
            continue
            
        # 종료 시간 체크
        if current_time_str > f"{end_hour:02d}{end_minute:02d}":
            print(f"\n🕒 현재 시각(KST) {now.strftime('%H:%M:%S')} - 종료 시간({end_hour:02d}:{end_minute:02d})이 되어 종료합니다.")
            break

        start_loop_time = time.time()
        turn_timestamp = now.replace(microsecond=0).isoformat()
        
        print(f"\n--- 수집 시작 시각: {turn_timestamp} ---")
        
        urls = [
            ("https://finance.naver.com/sise/sise_rise.naver?sosok=0", "KOSPI", "상승"),
            ("https://finance.naver.com/sise/sise_rise.naver?sosok=1", "KOSDAQ", "상승"),
            ("https://finance.naver.com/sise/sise_fall.naver?sosok=0", "KOSPI", "하락"),
            ("https://finance.naver.com/sise/sise_fall.naver?sosok=1", "KOSDAQ", "하락")
        ]
        
        all_collected = []
        for url, market, type_name in urls:
            if stop_requested: break
            data = get_naver_sise(url, market, type_name, turn_timestamp)
            all_collected.extend(data)
            time.sleep(0.5)
            
        if stop_requested: break

        if all_collected:
            print(f"✨ 총 {len(all_collected)}개 데이터 수집 완료")
            
            try:
                # [수정] 기존 데이터 삭제 (신규 데이터만 유지하기 위함)
                print("🧹 기존 'naver_realtime_stk' 데이터 삭제 중...")
                supabase.table("naver_realtime_stk").delete().neq("stk_cd", "").execute()

                batch_size = 1000
                for i in range(0, len(all_collected), batch_size):
                    batch = all_collected[i:i + batch_size]
                    supabase.table("naver_realtime_stk").upsert(batch).execute()
                print(f"🎉 Supabase 저장 완료 ({len(all_collected)}개)")
                
                # ETF 점수 산출 RPC 호출
                print("📊 [Server-Side] 네이버 ETF 점수 계산 요청 중...")
                supabase.rpc('calculate_naver_etf_score', {}).execute()
                print("✅ [Server-Side] 네이버 ETF 점수 업데이트 완료")
                
            except Exception as e:
                print(f"❌ 저장 및 계산 중 오류: {e}")

        if stop_requested:
            break
        
        # [수정] 시간대별 대기 시간 설정 (09~10시: 1분, 10시 이후: 5분)
        now_after = datetime.utcnow() + timedelta(hours=9)
        current_time_val = now_after.hour * 100 + now_after.minute
        
        if current_time_val < 1000:  # 10시 이전 (08:50 ~ 09:59)
            wait_seconds = 60
        else:  # 10시 이후 (10:00 ~ 15:20)
            wait_seconds = 300
            
        print(f"🔄 수집 완료. {wait_seconds // 60}분 대기 후 다음 수집을 시작합니다... (현재 시각: {now_after.strftime('%H:%M:%S')})")
        
        # 중단 요청 확인하며 대기 (반응성 확보)
        for _ in range(wait_seconds):
            if stop_requested:
                break
            time.sleep(1)

    print("=== 모든 프로세스 종료 ===")
    sys.exit(0)

if __name__ == "__main__":
    main()
