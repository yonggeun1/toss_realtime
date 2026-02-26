import requests
from bs4 import BeautifulSoup
import re
import sys
import os
import time
from datetime import datetime, timedelta

# Supabase 연동
try:
    from toss_crawling.supabase_client import supabase
except ImportError:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.append(project_root)
    from toss_crawling.supabase_client import supabase

def delete_old_premarket_data():
    """
    오늘(KST 기준) 이전의 프리마켓 관련 데이터를 모두 삭제합니다.
    """
    try:
        now_utc = datetime.utcnow()
        now_kst = now_utc + timedelta(hours=9)
        today_start_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
        threshold_str = today_start_kst.isoformat()

        print(f"🧹 [프리마켓] 오늘({today_start_kst.strftime('%Y-%m-%d')}) 이전 데이터 삭제 중...")
        
        # 원천 데이터 삭제
        supabase.table("naver_premarket_stk").delete().lt("collected_at", threshold_str).execute()
        
        # ETF 히스토리 데이터 삭제
        supabase.table("naver_premarket_etf_history").delete().lt("collected_at", threshold_str).execute()
        
        print(f"✅ [프리마켓] 지난 데이터 삭제 프로세스 완료")
    except Exception as e:
        print(f"🚨 [프리마켓] 지난 데이터 삭제 오류: {e}")

def get_naver_sise(url, market_name, type_name, now_kst):
    """
    네이버 증권 시세 정보 크롤링 (Nextrade 프리마켓/장후)
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
    now = datetime.utcnow() + timedelta(hours=9)
    current_time_str = now.strftime("%H%M")
    target_time = "0851"

    print(f"=== 네이버 프리마켓(Nextrade) 최종 집계 시작 (현재: {now.strftime('%H:%M:%S')}) ===")

    # 08:51분 이전이라면 대기 (선택 사항)
    if current_time_str < target_time:
        print(f"🕒 아직 {target_time[:2]}:{target_time[2:]} 전입니다. 대기 후 실행하거나 예약 실행을 권장합니다.")
        # 사용자가 수동 실행했을 때를 위해 잠시 대기 로직 유지 (원치 않으면 바로 종료 가능)
        while True:
            now = datetime.utcnow() + timedelta(hours=9)
            current_time_str = now.strftime("%H%M")
            if current_time_str >= target_time:
                break
            print(f"🕒 대기 중... ({now.strftime('%H:%M:%S')})", end='\r')
            time.sleep(30)
        print("\n🚀 지정된 시간이 되어 수집을 시작합니다.")

    # 1회성 작업 시작
    delete_old_premarket_data()
    turn_timestamp = now.replace(microsecond=0).isoformat()
    
    urls = [
        ("https://finance.naver.com/sise/nxt_sise_rise.naver?sosok=0", "KOSPI", "상승"),
        ("https://finance.naver.com/sise/nxt_sise_rise.naver?sosok=1", "KOSDAQ", "상승"),
        ("https://finance.naver.com/sise/nxt_sise_fall.naver?sosok=0", "KOSPI", "하락"),
        ("https://finance.naver.com/sise/nxt_sise_fall.naver?sosok=1", "KOSDAQ", "하락")
    ]
    
    all_collected = []
    for url, market, type_name in urls:
        data = get_naver_sise(url, market, type_name, turn_timestamp)
        all_collected.extend(data)
        time.sleep(0.5)

    if all_collected:
        print(f"✨ 총 {len(all_collected)}개 프리마켓 데이터 수집 완료")
        
        try:
            # 기존 데이터 삭제 (최종 스냅샷 유지를 위해)
            supabase.table("naver_premarket_stk").delete().neq("stk_cd", "").execute()

            # 데이터 저장
            batch_size = 1000
            for i in range(0, len(all_collected), batch_size):
                batch = all_collected[i:i + batch_size]
                supabase.table("naver_premarket_stk").upsert(batch).execute()
            print(f"🎉 Supabase 저장 완료 ({len(all_collected)}개)")
            
            # 프리마켓 점수 산출 RPC 호출
            print("📊 [Server-Side] 네이버 프리마켓 점수 계산 요청 중...")
            supabase.rpc('calculate_naver_premarket_score', {}).execute()
            print("✅ [Server-Side] 네이버 프리마켓 점수 업데이트 완료")
            
        except Exception as e:
            print(f"❌ 저장 및 계산 중 오류: {e}")

    print("=== 프리마켓 수집 및 집계 완료. 프로세스를 종료합니다. ===")
    sys.exit(0)

if __name__ == "__main__":
    main()
