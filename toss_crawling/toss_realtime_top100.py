import os
import time
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def get_toss_ranking(ranking_type="buy"):
    # ranking_type: 'buy' (순매수) or 'sell' (순매도)
    ranking_name = "순매수" if ranking_type == "buy" else "순매도"
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,3000")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    url = f"https://www.tossinvest.com/?ranking-type=domestic_investor_trend&ranking={ranking_type}"
    
    all_data = []
    
    try:
        print(f"🚀 [{ranking_type}] Connecting to: {url}")
        driver.get(url)
        wait = WebDriverWait(driver, 15)
        # 리스트 아이템이 로딩될 때까지 대기 (기존 body -> 실제 아이템)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/stocks/']")))
        time.sleep(5) # 데이터 로딩 대기 시간 증가
        
        # 📜 스크롤 다운
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        print(f"📜 [{ranking_type}] Page scroll completed")
        
        # 🕒 기준 시간 추출 (투자자별)
        base_times = {}
        # 외국인, 기관만 추출
        time_xpaths = {
            "Foreigner": "/html/body/div[1]/div[2]/div/div[1]/main/div/div/div[2]/div[5]/section/div[3]/section[1]/hgroup/div/div/span",
            "Institution": "/html/body/div[1]/div[2]/div/div[1]/main/div/div/div[2]/div[5]/section/div[3]/section[2]/hgroup/div/div/span"
        }
        
        default_time = time.strftime('%Y-%m-%d %H:%M:%S')

        # XPath가 한글 사이트 기준이라 외국인/기관 텍스트 매칭이 안될 수 있음. 위치 기반이므로 그대로 둠.
        # 단, 키값을 영어로 변경하여 저장 로직과 통일
        
        # 원본 코드의 time_xpaths 키가 "외국인", "기관"이었음. 이를 영어로 매핑 필요.
        # 하지만 XPath 자체가 고정되어 있으므로, 키를 영어로 바꾸고 아래 로직도 수정해야 함.
        
        # 기존 로직 유지하되 디버깅을 위해 출력만 영어로
        
        mapping_inv = {"외국인": "Foreigner", "기관": "Institution"}
        
        # time_xpaths는 그대로 두고 순회
        original_time_xpaths = {
            "외국인": "/html/body/div[1]/div[2]/div/div[1]/main/div/div/div[2]/div[5]/section/div[3]/section[1]/hgroup/div/div/span",
            "기관": "/html/body/div[1]/div[2]/div/div[1]/main/div/div/div[2]/div[5]/section/div[3]/section[2]/hgroup/div/div/span"
        }

        for inv_type, xpath in original_time_xpaths.items():
            eng_type = mapping_inv.get(inv_type, inv_type)
            try:
                el = driver.find_element(By.XPATH, xpath)
                base_times[inv_type] = el.text.strip()
                print(f"🕒 [{ranking_type}] {eng_type} Base Time: {base_times[inv_type]}")
            except:
                base_times[inv_type] = default_time
                print(f"⚠️ [{ranking_type}] {eng_type} Base Time extraction failed")

        # 전체 종목 아이템 수집
        items = driver.find_elements(By.CSS_SELECTOR, "a[href*='/stocks/']")
        print(f"📦 [{ranking_type}] Found {len(items)} items")

        current_group_idx = 0
        groups = ["외국인", "기관", "개인", "기타"] # 한글 로직 유지

        
        def parse_amount(amount_str):
            # ... (기존 로직 동일)
            if not amount_str:
                return 0
            try:
                # "-" 제거 추가 (순매도 음수 표기 등 대비)
                amount_str = amount_str.replace("순매수", "").replace("순매도", "").replace(",", "").replace(" ", "").replace("-", "")
                total_amount = 0.0
                if "조" in amount_str:
                    parts = amount_str.split("조")
                    try:
                        jo_part = float(parts[0])
                        total_amount += jo_part * 10000
                    except: pass
                    amount_str = parts[1]
                if "억" in amount_str:
                    parts = amount_str.split("억")
                    try:
                        uk_part = float(parts[0]) if parts[0] else 0
                        total_amount += uk_part
                    except: pass
                    amount_str = parts[1]
                if "만" in amount_str:
                    parts = amount_str.split("만")
                    try:
                        man_part = float(parts[0]) if parts[0] else 0
                        total_amount += man_part / 10000
                    except: pass
                return round(total_amount, 4)
            except:
                return 0

        for idx, item in enumerate(items):
            try:
                raw_text = item.text
                text_lines = [line.strip() for line in raw_text.split('\n') if line.strip()]
                
                if len(text_lines) >= 2:
                    rank = text_lines[0]
                    name = text_lines[1]
                    
                    # 🔍 종목코드 추출 (href="/stocks/A178320/order" -> "178320")
                    try:
                        href = item.get_attribute("href")
                        if "/stocks/A" in href:
                            stock_code = href.split("/stocks/A")[1].split("/")[0]
                        else:
                            stock_code = ""
                    except:
                        stock_code = ""
                    
                    if rank == '1' and idx > 0:
                        current_group_idx += 1
                    
                    group_name = groups[current_group_idx] if current_group_idx < len(groups) else "Unknown"
                    
                    # 🚀 외국인, 기관만 수집 (개인 제외)
                    if group_name not in ["외국인", "기관"]:
                        continue

                    amount_str = ""
                    
                    for line in text_lines[2:]:
                        if "억" in line or "만" in line:
                            amount_str = line.strip()
                            break # 금액 정보 찾으면 중단
                    
                    if not amount_str and len(text_lines) > 2:
                         for line in text_lines[2:]:
                            if "원" not in line and "%" not in line:
                                amount_str = line
                                break
                    
                    amount_val = parse_amount(amount_str)
                    collected_time = base_times.get(group_name, default_time)
                    
                    # 🛑 날짜 체크 및 변환
                    today_date_str = datetime.now().strftime('%Y-%m-%d')
                    current_year = datetime.now().year
                    
                    # 날짜 문자열 정규화
                    if "어제" in collected_time:
                        yesterday = datetime.now() - timedelta(days=1)
                        yesterday_str = yesterday.strftime('%Y-%m-%d')
                        collected_time = collected_time.replace("어제", yesterday_str)
                    
                    if "오늘" in collected_time:
                        collected_time = collected_time.replace("오늘", today_date_str)
                    
                    # '1월 30일' 같은 포맷 처리
                    if "월" in collected_time and "일" in collected_time:
                        try:
                            # 1월 30일 -> 2026-01-30
                            # 시간 정보가 없으면 00:00:00으로 간주하거나 현재 시간 붙임? -> 보통 날짜만 있으면 됨.
                            # 정규식 등으로 숫자 추출
                            import re
                            match = re.search(r'(\d+)월\s*(\d+)일', collected_time)
                            if match:
                                month = int(match.group(1))
                                day = int(match.group(2))
                                collected_time = f"{current_year}-{month:02d}-{day:02d}"
                        except:
                            pass

                    # 날짜 형식이 YYYY-MM-DD 인지 확인 (대략적으로)
                    # 만약 여전히 한글 등이 남아있으면 DB 저장시 에러나므로, 파싱 실패시 현재 시간으로 대체하거나 스킵?
                    # 여기서는 최대한 파싱된 값 사용.
                    
                    # 기존 로직: 오늘 날짜가 아니면 continue 했었음.
                    # 변경: 날짜가 달라도 수집. (주말 등 고려)
                    # if not collected_time.startswith(today_date_str):
                    #     continue


                    # 컬럼명 동적 설정
                    amount_col_name = f"{ranking_name}금액(억원)"

                    all_data.append({
                        '투자자': group_name,
                        '종목명': name,
                        '종목코드': stock_code,
                        amount_col_name: amount_val,
                        '수집일시': collected_time
                    })
            except:
                continue

        # 결과 저장 (Supabase)
        if all_data:
            try:
                from toss_crawling.supabase_client import supabase
            except ImportError:
                from supabase_client import supabase

            data_to_insert = []
            amount_col_name = f"{ranking_name}금액(억원)"

            for item in all_data:
                # 데이터 매핑
                mapped_item = {
                    "investor": item['투자자'],
                    "stock_name": item['종목명'],
                    "stock_code": item['종목코드'],
                    "amount": item[amount_col_name],
                    "ranking_type": ranking_type,  # 'buy' or 'sell'
                    "collected_at": item['수집일시']
                }
                data_to_insert.append(mapped_item)
            
            try:
                # Supabase에 데이터 삽입
                response = supabase.table("toss_realtime_top100").insert(data_to_insert).execute()
                print(f"\n🎉 [{ranking_type}] Supabase Save Complete (Total {len(data_to_insert)} items)")
                
                # 디버깅용 출력 (일부 데이터 확인)
                print(f"[{ranking_type} Sample Data (First 1)]")
                print(data_to_insert[0])

            except Exception as e:
                print(f"❌ [{ranking_type}] Supabase Save Error: {e}")
                
        else:
            print(f"❌ [{ranking_type}] No collected data.")
        
    except Exception as e:
        print(f"❌ [{ranking_name}] 오류 발생: {e}")
    finally:
        driver.quit()

from datetime import datetime, timedelta

# ... (imports 유지)

if __name__ == "__main__":
    import sys
    
    # --once 플래그 확인
    run_once = "--once" in sys.argv

    while True:
        # 🕒 서버 시간(UTC)에 9시간을 더해 한국 시간(KST) 구하기
        now = datetime.utcnow() + timedelta(hours=9)
        
        # 15시 30분 이후 체크 (KST 기준)
        if not run_once and (now.hour > 15 or (now.hour == 15 and now.minute >= 30)):
            print(f"🕒 현재 시간(KST) {now.strftime('%H:%M:%S')} - 장 마감 시간(15:30)이 되어 수집을 종료합니다.")
            break

        start_time = time.time()
        print(f"=== 토스증권 수급 데이터 수집 시작 (시작 시각 KST: {now.strftime('%H:%M:%S')}) ===")
        
        try:
            get_toss_ranking("buy")  # 순매수
            print("\n" + "="*30 + "\n")
            get_toss_ranking("sell") # 순매도
        except Exception as e:
            print(f"❌ 메인 루프 실행 중 오류 발생: {e}")

        print("=== 이번 턴 수집 완료 ===")
        
        if run_once:
            print("🚀 1회 실행 모드 완료. 종료합니다.")
            break
        
        elapsed_time = time.time() - start_time
        wait_time = 60 - elapsed_time
        
        if wait_time > 0:
            print(f"⏳ 다음 수집까지 {wait_time:.1f}초 대기...")
            time.sleep(wait_time)
        else:
            print("⏳ 대기 없이 바로 다음 수집 시작")
