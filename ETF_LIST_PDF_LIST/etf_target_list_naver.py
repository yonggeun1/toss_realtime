import requests
import json
import os
import sys

def get_naver_domestic_sector_etfs():
    """
    네이버 ETF API를 호출하여 '국내 업종/테마' (etfTabCode == 2) 종목들만 추출하여 출력합니다.
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

        # '국내 업종/테마' (etfTabCode == 2) 필터링
        # 참고: 1: 국내 시장지수, 2: 국내 업종/테마, 3: 국내 파생, 4: 해외 주식, 5: 원자재, 6: 채권, 7: 기타
        sector_theme_etfs = [item for item in etf_list if item.get('etfTabCode') == 2]
        
        print(f"✅ '국내 업종/테마' 카테고리에서 총 {len(sector_theme_etfs)}개의 종목을 찾았습니다.\n")
        
        results = []
        for item in sector_theme_etfs:
            record = {
                "etf_code": str(item.get('itemcode', '')).zfill(6),
                "etf_name": item.get('itemname', ''),
                "tab_code": item.get('etfTabCode'),
                "current_price": float(item.get('nowVal', 0)),
                "change_rate": float(item.get('changeRate', 0)),
                "market_cap": int(item.get('marketSum', 0)) # 시가총액(억)
            }
            results.append(record)
            print(f"[{record['etf_code']}] {record['etf_name']} | 등락율: {record['change_rate']}% | 시총: {record['market_cap']}억")
            
        return results

    except Exception as e:
        print(f"❌ 데이터 요청 또는 파싱 실패: {e}")
        return []

def main():
    print("=== 네이버 '국내 업종/테마' ETF 리스트 수집 시작 ===")
    etfs = get_naver_domestic_sector_etfs()
    
    if etfs:
        # 필요 시 파일로 저장하는 로직 추가 가능 (예: CSV 또는 JSON)
        # with open("domestic_sector_etfs.json", "w", encoding="utf-8") as f:
        #     json.dump(etfs, f, ensure_ascii=False, indent=4)
        print(f"\n✨ 총 {len(etfs)}개의 종목 수집 완료")
    else:
        print("❌ 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    main()
