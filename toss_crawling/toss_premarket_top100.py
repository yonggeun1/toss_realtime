import time
from datetime import datetime, timedelta
import re
import sys
import signal # 종료 신호 처리를 위해 추가
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# 전역 변수로 종료 요청 상태 관리
stop_requested = False

def signal_handler(sig, frame):
    global stop_requested
    print(f"\n🛑 종료 신호({sig})를 수신했습니다. 현재 진행 중인 수집 및 계산을 마치고 안전하게 종료합니다...")
    stop_requested = True

# 종료 신호(Ctrl+C, GitHub Actions 취소 등) 연결
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Supabase 클라이언트 임포트
try:
    from toss_crawling.supabase_client import supabase, delete_old_scores, load_etf_pdf_from_supabase
except ImportError:
    # 로컬 실행 시 경로 문제 대비
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from supabase_client import supabase, delete_old_scores, load_etf_pdf_from_supabase

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

def get_toss_amount_ranking(collected_at=None):
    """
    거래대금 상위 100개 종목을 수집하는 함수
    URL: https://www.tossinvest.com/?market=kr&live-chart=biggest_total_amount&ranking-type=realtime_chart
    """
    if collected_at is None:
        kst_now = datetime.utcnow() + timedelta(hours=9)
        collected_at = kst_now.isoformat()

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        print(f"[거래대금] 연결 시도 {attempt}/{max_retries}: https://www.tossinvest.com/?market=kr&live-chart=biggest_total_amount&ranking-type=realtime_chart")
        
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,5000")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        url = "https://www.tossinvest.com/?market=kr&live-chart=biggest_total_amount&ranking-type=realtime_chart"
        
        all_data = []
        
        try:
            driver.get(url)
            wait = WebDriverWait(driver, 20)
            
            # [수정] 사용자가 제공한 테이블 컨테이너 XPath로 대기 조건 변경
            table_container_xpath = "/html/body/div[1]/div[2]/div/div[1]/main/div/div/div[2]/div[5]/div/div[3]"
            wait.until(EC.presence_of_element_located((By.XPATH, table_container_xpath)))
            time.sleep(5) 
            
            # 📜 충분한 스크롤 다운
            for _ in range(3):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
            
            # [수정] 테이블 내의 모든 행(tr)을 가져옴
            rows = driver.find_elements(By.XPATH, f"{table_container_xpath}//table/tbody/tr")
            
            print(f"🔍 [거래대금] 발견된 행(row) 수: {len(rows)}")

            count = 0

            for row in rows:
                try:
                    raw_text = row.text
                    if not raw_text: continue
                    text_lines = [line.strip() for line in raw_text.split('\n') if line.strip()]
                    
                    if len(text_lines) >= 3:
                        # 구조: [순위, 종목명, 현재가/변동률, 거래대금]
                        rank = text_lines[0]
                        name = text_lines[1]
                        
                        # [수정] 행 내부에서 종목코드 추출을 위한 링크(a) 찾기
                        try:
                            link_elem = row.find_element(By.TAG_NAME, "a")
                            href = link_elem.get_attribute("href")
                            code_match = re.search(r'/stocks/(?:A)?([0-9A-Z]{6,})', href)
                            stock_code = code_match.group(1) if code_match else ""
                        except: stock_code = ""
                        
                        if not stock_code: continue

                        # 거래대금 파싱 (보통 마지막 라인 또는 '억'/'조'가 포함된 라인)
                        amount_str = ""
                        change_rate_val = 0.0
                        
                        for line in text_lines:
                            # 등락율 추출 (예: +4.01%, -1.2% 등 %가 포함된 문자열)
                            if "%" in line:
                                try:
                                    # "+4.01%", "75,000원 +4.01%" 등에서 숫자 부분만 추출
                                    match = re.search(r'([+-]?\d+\.?\d*)%', line)
                                    if match:
                                        change_rate_val = float(match.group(1))
                                except: pass
                                
                            if any(unit in line for unit in ["조", "억", "만"]):
                                amount_str = line
                        
                        amount_val = parse_amount(amount_str)
                        
                        all_data.append({
                            "rank": int(rank) if rank.isdigit() else 0,
                            "stock_name": name,
                            "stock_code": stock_code,
                            "amount": amount_val,
                            "change_rate": change_rate_val, # 등락율 추가
                            "collected_at": collected_at
                        })
                        count += 1
                        if count >= 100: break
                except: continue

            print(f"📊 [거래대금] 수집 결과 -> {len(all_data)}개 종목")

            if len(all_data) >= 80: # 최소 80개 이상 수집 시 성공으로 간주
                try:
                    # 신규 데이터 저장 (삭제 로직 제거됨, 데이터 누적)
                    supabase.table("toss_premarket_top100").upsert(
                        all_data, on_conflict="stock_code, collected_at"
                    ).execute()
                    print(f"🎉 [거래대금] Supabase 저장 완료 (데이터 누적)")
                    driver.quit()
                    return
                except Exception as e:
                    print(f"❌ [거래대금] 저장 에러: {e}")
            else:
                print(f"⚠️ [거래대금] 수집 데이터 부족 ({len(all_data)}개). 재시도합니다.")
            
        except Exception as e:
            print(f"❌ [거래대금] 오류 발생: {e}")
        finally:
            driver.quit()
        
        time.sleep(5)
    
    print(f"🚨 [거래대금] {max_retries}회 시도 실패.")

if __name__ == "__main__":
    # 인자 확인
    is_morning = "--session morning" in sys.argv
    is_afternoon = "--session afternoon" in sys.argv

    # 종료 시간 설정
    # 기본은 13:20 종료
    end_hour, end_minute = 13, 20
    
    if is_morning:
        end_hour, end_minute = 12, 0
    elif is_afternoon:
        end_hour, end_minute = 13, 20
    
    print(f"=== 토스증권 거래대금 상위 100 루프 시작 (세션: {'오전' if is_morning else '오후' if is_afternoon else '기본'}, 종료 예정: {end_hour:02d}:{end_minute:02d}) ===")

    while True:
        now = datetime.utcnow() + timedelta(hours=9)
        
        # 종료 시간 체크
        if now.hour > end_hour or (now.hour == end_hour and now.minute >= end_minute):
            print(f"🕒 현재 시간(KST) {now.strftime('%H:%M:%S')} - 종료 시간({end_hour:02d}:{end_minute:02d})이 되어 종료합니다.")
            break

        start_time = time.time()
        turn_timestamp = now.replace(microsecond=0).isoformat()
        
        try:
            print(f"\n--- 수집 시작 시각: {turn_timestamp} ---")
            # 거래대금 상위 100개 종목 수집 및 DB 저장
            get_toss_amount_ranking(collected_at=turn_timestamp)
            
            # 서버 사이드 프리마켓 점수 계산 호출
            print(f"📊 [Server-Side] Premarket Score 계산 요청 중...")
            try:
                supabase.rpc('calculate_premarket_score_server', {'target_time': turn_timestamp}).execute()
                print("✅ [Server-Side] Premarket Score 업데이트 완료")
            except Exception as e:
                print(f"❌ [Server-Side] Premarket Score 업데이트 오류: {e}")

        except Exception as e:
            print(f"❌ 실행 중 오류 발생: {e}")

        # 60초 주기 대기 (실행 시간 제외)
        elapsed = time.time() - start_time
        wait_time = max(0, 60 - elapsed)
        
        if not stop_requested:
            print(f"⏳ 다음 수집까지 {wait_time:.1f}초 대기...")
            time.sleep(wait_time)
        else:
            break

    print("=== 모든 프로세스 종료 ===")
    sys.exit(0)
