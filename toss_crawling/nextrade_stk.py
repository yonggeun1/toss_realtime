import time
import re
import sys
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Supabase 연동
try:
    from toss_crawling.supabase_client import supabase
except ImportError:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.append(project_root)
    from toss_crawling.supabase_client import supabase

def get_nextrade_data():
    """넥스트레이드 거래현황 테이블 크롤링"""
    url = "https://www.nextrade.co.kr/menu/transactionStatusMain/menuList.do"
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    try:
        print(f"🚀 넥스트레이드 접속 중: {url}")
        driver.get(url)
        wait = WebDriverWait(driver, 20)
        
        # 테이블 로딩 대기
        table_xpath = "/html/body/main/form/div/div/div/div[2]/div/div[3]/div/div/div[3]/div[3]/div/table"
        wait.until(EC.presence_of_element_located((By.XPATH, table_xpath)))
        time.sleep(3) # 추가 렌더링 대기
        
        rows = driver.find_elements(By.XPATH, f"{table_xpath}/tbody/tr")
        print(f"📊 발견된 전체 데이터 행 수: {len(rows)}")
        
        collected_data = []
        now_kst = datetime.now().astimezone().isoformat()
        
        # Row 1은 헤더이거나 비어있을 수 있으므로 Row 2부터 처리
        for idx, row in enumerate(rows):
            tds = row.find_elements(By.TAG_NAME, "td")
            if len(tds) < 10: continue # 유효한 데이터 행이 아님
            
            try:
                # TD 1: 종목코드 (A005930)
                stk_cd_raw = tds[0].text.strip()
                stk_cd = re.sub(r'[^0-9]', '', stk_cd_raw) # 숫자만 추출
                
                # TD 2: 종목명 (삼성전자)
                stk_nm = tds[1].text.strip()
                
                # TD 4: 현재가 (205,500)
                price_str = tds[3].text.strip().replace(',', '')
                price = float(price_str) if price_str else 0.0
                
                # TD 6: 등락율 (2.75%)
                flu_rt_str = tds[5].text.strip().replace('%', '').replace('+', '')
                flu_rt = float(flu_rt_str) if flu_rt_str else 0.0
                
                # TD 10: 거래량
                trde_qty_str = tds[9].text.strip().replace(',', '')
                trde_qty = int(trde_qty_str) if trde_qty_str else 0
                
                # TD 11: 거래대금
                trde_amt_str = tds[10].text.strip().replace(',', '')
                trde_amt = int(trde_amt_str) if trde_amt_str else 0
                
                if stk_cd:
                    record = {
                        "stk_cd": stk_cd,
                        "stk_nm": stk_nm,
                        "close_pric": price,
                        "flu_rt": flu_rt,
                        "trde_qty": trde_qty,
                        "trde_prica": trde_amt,
                        "collected_at": now_kst,
                    }
                    collected_data.append(record)
                    if len(collected_data) <= 5:
                        print(f"✅ {stk_nm}({stk_cd}): {price:,.0f}원 ({flu_rt:+.2f}%)")
            except Exception as e:
                # print(f"⚠️ Row {idx} 파싱 오류: {e}")
                continue

        print(f"✨ 총 {len(collected_data)}개 종목 수집 완료")
        return collected_data

    except Exception as e:
        print(f"❌ 크롤링 중 오류 발생: {e}")
        return []
    finally:
        driver.quit()

if __name__ == "__main__":
    data = get_nextrade_data()
    
    if data:
        # Supabase 테이블 존재 여부를 확인하지 못하므로, 일단 kiwoom_realtime_stk와 유사한 형식으로 저장 시도
        # 만약 별도의 테이블을 원하신다면 테이블명을 변경해주세요.
        table_name = "nextrade_realtime_stk" # 임시 테이블명
        
        print(f"\n💾 데이터 저장 시도 중... (Table: {table_name})")
        try:
            # 먼저 테이블이 있는지 확인하거나, 그냥 insert 시도
            # (실제 환경에서는 테이블이 미리 생성되어 있어야 함)
            # supabase.table(table_name).insert(data).execute()
            # print(f"🎉 {len(data)}개 데이터 저장 성공!")
            
            # 테스트를 위해 상위 3개만 출력
            for item in data[:3]:
                print(item)
            print("\n⚠️ 실제 저장은 테이블 생성이 필요하여 주석 처리되어 있습니다.")
        except Exception as e:
            print(f"❌ 저장 중 오류: {e}")
            print("💡 'nextrade_realtime_stk' 테이블이 데이터베이스에 존재하는지 확인해주세요.")
