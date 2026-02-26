import requests
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

def get_naver_etf_info():
    """
    네이버 금융 ETF 내부 API를 호출하여 전 종목 시세를 가져옵니다.
    """
    url = "https://finance.naver.com/api/sise/etfItemList.nhn"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        'Referer': 'https://finance.naver.com/sise/etf.nhn'
    }
    
    try:
        print(f"🌐 네이버 ETF API 호출 중: {url}")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data_json = response.json()
        
        etf_list = data_json.get('result', {}).get('etfItemList', [])
        if not etf_list:
            print("❌ API 응답에 ETF 데이터가 없습니다.")
            return []

        now_kst = (datetime.utcnow() + timedelta(hours=9)).isoformat()
        collected_data = []
        
        for item in etf_list:
            # 필드 매핑: 
            # itemcode(종목코드), itemname(종목명), nowVal(현재가), changeVal(전일비), 
            # changeRate(등락율), nav(NAV), threeMonthLowerQty(3개월수익률), 
            # quant(거래량), amonut(거래대금-백만), marketSum(시가총액-억)
            
            record = {
                "etf_code": str(item.get('itemcode', '')).zfill(6),
                "etf_name": item.get('itemname', ''),
                "current_price": float(item.get('nowVal', 0)),
                "change_price": float(item.get('changeVal', 0)),
                "change_rate": float(item.get('changeRate', 0)),
                "nav": float(item.get('nav', 0)),
                "three_month_return": float(item.get('threeMonthLowerQty', 0)),
                "volume": int(item.get('quant', 0)),
                "trading_value": int(item.get('amonut', 0)),
                "market_cap": int(item.get('marketSum', 0)),
                "updated_at": now_kst
            }
            collected_data.append(record)
            
        return collected_data

    except Exception as e:
        print(f"❌ 데이터 요청 또는 파싱 실패: {e}")
        return []

def delete_old_etf_price_data():
    """
    오늘(KST 기준) 이전의 ETF 시세 히스토리 데이터를 삭제합니다.
    """
    try:
        now_utc = datetime.utcnow()
        now_kst = now_utc + timedelta(hours=9)
        today_start_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
        threshold_str = today_start_kst.isoformat()

        print(f"🧹 [ETF 시세] 오늘({today_start_kst.strftime('%Y-%m-%d')}) 이전 히스토리 데이터 삭제 중...")
        supabase.table("naver_etf_price_history").delete().lt("updated_at", threshold_str).execute()
        print(f"✅ [ETF 시세] 지난 데이터 삭제 완료")
    except Exception as e:
        print(f"🚨 [ETF 시세] 지난 데이터 삭제 오류: {e}")

def main():
    # 인자 확인 (세션 구분)
    is_morning = "morning" in sys.argv
    is_afternoon = "afternoon" in sys.argv

    # 오전 세션이거나 일반 실행일 때만 이전 데이터 삭제
    if not is_afternoon:
        delete_old_etf_price_data()

    # 루프 설정
    start_hour, start_minute = 8, 50
    end_hour, end_minute = 15, 20
    
    if is_morning:
        end_hour, end_minute = 12, 0
    elif is_afternoon:
        end_hour, end_minute = 15, 20
    
    print(f"=== 네이버 ETF 전종목 시세 수집 시작 (세션: {'오전' if is_morning else '오후' if is_afternoon else '기본'}, 종료 예정: {end_hour:02d}:{end_minute:02d}) ===")

    while True:
        try:
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

            print(f"\n--- 수집 시작 시각: {now.replace(microsecond=0).isoformat()} ---")
            data = get_naver_etf_info()
            
            if data:
                print(f"✨ 총 {len(data)}개의 ETF 데이터를 수집했습니다.")
                print(f"💾 Supabase 저장 중...")
                # 1. 최신 시세 업데이트 (Upsert)
                batch_size = 500
                for i in range(0, len(data), batch_size):
                    batch = data[i:i+batch_size]
                    supabase.table("naver_etf_price").upsert(batch).execute()
                
                # 2. 히스토리 기록
                history_data = [
                    {
                        "etf_code": d["etf_code"],
                        "etf_name": d["etf_name"],
                        "current_price": d["current_price"],
                        "change_rate": d["change_rate"],
                        "volume": d["volume"],
                        "updated_at": d["updated_at"]
                    } for d in data
                ]
                for i in range(0, len(history_data), batch_size):
                    batch = history_data[i:i+batch_size]
                    supabase.table("naver_etf_price_history").insert(batch).execute()
                    
                print("✅ Supabase 업데이트 완료")
            else:
                print("❌ 수집된 데이터가 없습니다.")

        except Exception as e:
            print(f"❌ 루프 실행 중 오류 발생: {e}")
            time.sleep(10) # 오류 발생 시 잠시 대기

        print("🔄 30초 후 다음 수집을 시작합니다...")
        time.sleep(30)

    print("=== 모든 프로세스 종료 ===")

if __name__ == "__main__":
    main()
