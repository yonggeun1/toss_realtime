import time
from datetime import datetime, timedelta
import re
import sys
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Supabase 클라이언트 임포트
try:
    from toss_crawling.supabase_client import supabase, delete_old_scores, load_etf_pdf_from_supabase
    from toss_crawling.toss_realtime_score import calculate_yg_score
except ImportError:
    # 로컬 실행 시 경로 문제 대비
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from supabase_client import supabase, delete_old_scores, load_etf_pdf_from_supabase
    from toss_realtime_score import calculate_yg_score

def parse_amount(amount_str):
    if not amount_str:
        return 0
    try:
        # "-", "원", "순매수/도" 등 불필요한 문자열 제거 및 정제
        amount_str = amount_str.replace("순매수", "").replace("순매도", "").replace(",", "").replace(" ", "").replace("-", "").replace("원", "")
        total_amount = 0.0
        
        # 조 단위 처리 (1조 = 10000억)
        if "조" in amount_str:
            parts = amount_str.split("조")
            try:
                if parts[0].strip():
                    jo_part = float(parts[0])
                    total_amount += jo_part * 10000
            except: pass
            amount_str = parts[1] if len(parts) > 1 else ""
            
        # 억 단위 처리
        if "억" in amount_str:
            parts = amount_str.split("억")
            try:
                if parts[0].strip():
                    uk_part = float(parts[0])
                    total_amount += uk_part
            except: pass
            amount_str = parts[1] if len(parts) > 1 else ""
            
        # 만 단위 처리 (1만 = 0.0001억)
        if "만" in amount_str:
            parts = amount_str.split("만")
            try:
                if parts[0].strip():
                    man_part = float(parts[0])
                    total_amount += man_part / 10000
            except: pass
            
        return round(total_amount, 4)
    except:
        return 0

def parse_date(date_str):
    """
    토스증권 날짜 형식(오늘, 어제, 1월 30일 등)을 YYYY-MM-DD 형식으로 변환
    """
    # KST 기준 시간 사용
    kst_now = datetime.utcnow() + timedelta(hours=9)
    today_str = kst_now.strftime('%Y-%m-%d')
    current_year = kst_now.year
    
    if not date_str:
        return today_str

    if "오늘" in date_str:
        return today_str
    
    if "어제" in date_str:
        yesterday = kst_now - timedelta(days=1)
        return yesterday.strftime('%Y-%m-%d')
    
    # 1월 30일 포맷
    match = re.search(r'(\d+)월\s*(\d+)일', date_str)
    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        return f"{current_year}-{month:02d}-{day:02d}"

    # 이미 YYYY-MM-DD 형식이면 그대로 반환
    if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
        return date_str
        
    return today_str

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
        
        # 리스트 아이템 로딩 대기
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/stocks/']")))
        time.sleep(5) 
        
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
        # 외국인, 기관 XPath 매핑
        time_xpaths = {
            "외국인": "/html/body/div[1]/div[2]/div/div[1]/main/div/div/div[2]/div[5]/section/div[3]/section[1]/hgroup/div/div/span",
            "기관": "/html/body/div[1]/div[2]/div/div[1]/main/div/div/div[2]/div[5]/section/div[3]/section[2]/hgroup/div/div/span"
        }
        
        default_time = time.strftime('%Y-%m-%d %H:%M:%S')

        for inv_type, xpath in time_xpaths.items():
            try:
                el = driver.find_element(By.XPATH, xpath)
                base_times[inv_type] = el.text.strip()
                print(f"🕒 [{ranking_type}] {inv_type} Base Time: {base_times[inv_type]}")
            except:
                base_times[inv_type] = default_time
                print(f"⚠️ [{ranking_type}] {inv_type} Base Time extraction failed")

        # 전체 종목 아이템 수집
        items = driver.find_elements(By.CSS_SELECTOR, "a[href*='/stocks/']")
        print(f"📦 [{ranking_type}] Found {len(items)} raw items")

        current_group_idx = 0
        groups = ["외국인", "기관", "개인", "기타"]
        group_counts = {"외국인": 0, "기관": 0}

        # KST 기준 오늘 날짜 계산 (루프 밖에서 한 번 계산)
        kst_now = datetime.utcnow() + timedelta(hours=9)
        today_str = kst_now.strftime('%Y-%m-%d')
        # [수정] 수집 시간(collected_at)을 KST 전체 타임스탬프로 설정 (이력 누적용)
        collected_at_kst = kst_now.isoformat()

        for idx, item in enumerate(items):
            try:
                raw_text = item.text
                if not raw_text: continue
                
                text_lines = [line.strip() for line in raw_text.split('\n') if line.strip()]
                
                if len(text_lines) >= 2:
                    rank = text_lines[0]
                    name = text_lines[1]
                    
                    # 그룹 인덱스 증가 로직 (Rank '1'을 만났을 때 다음 그룹으로 이동)
                    # 단, 너무 빨리 바뀌지 않도록 최소 개수(예: 90개) 이후에만 체크
                    if rank == '1' and idx > 10: 
                         if group_counts.get(groups[current_group_idx], 0) >= 90:
                            current_group_idx += 1
                            print(f"📌 [{ranking_type}] Switched to next group: {groups[current_group_idx]} at index {idx}")
                    
                    group_name = groups[current_group_idx] if current_group_idx < len(groups) else "Unknown"
                    
                    if group_name not in ["외국인", "기관"]:
                        continue

                    # 이미 해당 그룹 100개를 채웠다면 해당 아이템은 스킵
                    if group_counts[group_name] >= 100:
                        continue

                    # 🔍 종목코드 추출
                    try:
                        href = item.get_attribute("href")
                        if "/stocks/A" in href:
                            stock_code = href.split("/stocks/A")[1].split("/")[0]
                        else:
                            stock_code = ""
                    except:
                        stock_code = ""

                    # 이름 보정 로직
                    if re.match(r'^[0-9,.\-+\s%]+(원)?$', name):
                        if len(text_lines) > 2:
                            name = text_lines[2]
                    
                    # 금액 정보 파싱
                    amount_str = ""
                    is_yesterday = False
                    for line in text_lines:
                        if "어제" in line:
                            is_yesterday = True
                        if any(unit in line for unit in ["조", "억", "만"]):
                            amount_str = line.strip()
                    
                    # "어제" 데이터인 경우 금액을 0으로 강제 설정
                    if is_yesterday:
                        amount_val = 0.0
                        print(f"⚠️ [{ranking_type}] {group_name} - {name} ({stock_code}) 데이터가 '어제' 것이므로 0으로 처리합니다.")
                    else:
                        amount_val = parse_amount(amount_str)
                    
                    # 데이터 저장용 dict 생성
                    all_data.append({
                        "investor": group_name,
                        "stock_name": name,
                        "stock_code": stock_code,
                        "amount": amount_val,
                        "ranking_type": ranking_type,
                        "collected_at": collected_at_kst
                    })
                    group_counts[group_name] += 1
            except Exception as e:
                continue

        print(f"📊 [{ranking_type}] Final Counts -> 外: {group_counts.get('외국인', 0)}, 機: {group_counts.get('기관', 0)}")

        # 결과 저장 (Supabase)
        if all_data:
            # 중복 제거 (investor, stock_code, stock_name 조합 기준)
            unique_map = {}
            for item in all_data:
                # stock_code가 없는 경우 name을 사용하여 고유성 유지
                key = (item["investor"], item["stock_code"], item["stock_name"], item["ranking_type"])
                unique_map[key] = item
            all_data = list(unique_map.values())

            try:
                # Supabase에 데이터 삽입 (upsert 사용)
                response = supabase.table("toss_realtime_top100").upsert(
                    all_data, 
                    on_conflict="investor, stock_code, ranking_type, collected_at"
                ).execute()
                print(f"\n🎉 [{ranking_type}] Supabase Save Complete (Total {len(all_data)} items)")
                
            except Exception as e:
                print(f"❌ [{ranking_type}] Supabase Save Error: {e}")
                
        else:
            print(f"❌ [{ranking_type}] No collected data.")
        
    except Exception as e:
        print(f"❌ [{ranking_name}] 오류 발생: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    
    # 인자 확인
    run_once = "--once" in sys.argv
    is_morning = "--session morning" in sys.argv
    is_afternoon = "--session afternoon" in sys.argv

    # PDF 데이터 최초 1회 로드
    print("Loading ETF PDF data...")
    cached_pdf_data = load_etf_pdf_from_supabase()

    # 🧹 [변경] 오늘 이전 데이터 삭제는 시작 시 1회만 수행 (오전 세션 또는 단독 실행 시에만)
    if not is_afternoon:
        print("🧹 Cleaning up old data (older than today) before starting loop...")
        delete_old_scores()

    while True:
        # 🕒 서버 시간(UTC)에 9시간을 더해 한국 시간(KST) 구하기
        now = datetime.utcnow() + timedelta(hours=9)
        
        # 종료 시간 설정
        # 기본은 15:30 종료
        end_hour, end_minute = 15, 30
        
        # 오전 세션인 경우 12:00 종료
        if is_morning:
            end_hour, end_minute = 12, 0
            
        # 종료 조건 체크 (KST 기준)
        if not run_once and (now.hour > end_hour or (now.hour == end_hour and now.minute >= end_minute)):
            print(f"🕒 현재 시간(KST) {now.strftime('%H:%M:%S')} - 세션 종료 시간({end_hour:02d}:{end_minute:02d})이 되어 수집을 종료합니다.")
            break

        start_time = time.time()
        
        # [제거됨] delete_old_scores() 여기서는 호출하지 않음 (루프 진입 전 1회만 호출)
        
        print(f"=== 토스증권 수급 데이터 수집 시작 (시작 시각 KST: {now.strftime('%H:%M:%S')}) ===")
        
        try:
            get_toss_ranking("buy")  # 순매수
            print("\n" + "="*30 + "\n")
            get_toss_ranking("sell") # 순매도
            
            # 🚀 크롤링 완료 후 점수 계산 및 업데이트 실행
            print("\n📊 YG Score 계산 및 score 테이블 업데이트 시작...")
            try:
                calculate_yg_score(df_pdf=cached_pdf_data)
                print("✅ YG Score 업데이트 완료")
            except Exception as e:
                print(f"❌ YG Score 업데이트 중 오류 발생: {e}")
                
        except Exception as e:
            print(f"❌ 메인 루프 실행 중 오류 발생: {e}")

        print("=== 이번 턴 수집 완료 ===")
        
        if run_once:
            print("🚀 1회 실행 모드 완료. 종료합니다.")
        
        elapsed_time = time.time() - start_time
        wait_time = 60 - elapsed_time
        
        if wait_time > 0:
            print(f"⏳ 다음 수집까지 {wait_time:.1f}초 대기...")
            time.sleep(wait_time)
        else:
            print("⏳ 대기 없이 바로 다음 수집 시작")
