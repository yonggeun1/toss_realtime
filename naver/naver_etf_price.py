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

def main():
    print("=== 네이버 ETF 전종목 시세 수집 시작 (API 방식) ===")
    data = get_naver_etf_info()
    
    if data:
        print(f"✨ 총 {len(data)}개의 ETF 데이터를 수집했습니다.")
        print(f"샘플 데이터 (상위 2개):")
        for d in data[:2]:
            print(f" - [{d['etf_code']}] {d['etf_name']}: {d['current_price']}원 ({d['change_rate']}%)")
            
        try:
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
        except Exception as e:
            print(f"❌ DB 저장 오류: {e}")
    else:
        print("❌ 수집된 데이터가 없어 종료합니다.")

if __name__ == "__main__":
    main()
