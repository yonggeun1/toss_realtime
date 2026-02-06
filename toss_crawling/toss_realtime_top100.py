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

def get_toss_ranking(ranking_type="buy", collected_at=None):
    # ranking_type: 'buy' (순매수) or 'sell' (순매도)
    ranking_name = "순매수" if ranking_type == "buy" else "순매도"
    
    # [수정] 외부에서 받은 시간이 없으면 현재 시간 생성
    if collected_at is None:
        kst_now = datetime.utcnow() + timedelta(hours=9)
        collected_at = kst_now.isoformat()
    
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
        
        # [개선] 더 유연한 방식으로 섹션별 기준 시간 추출
        try:
            # 모든 섹션(section)을 돌며 내부의 '외국인', '기관' 텍스트와 시간(span)을 찾음
            sections = driver.find_elements(By.TAG_NAME, "section")
            for sec in sections:
                sec_text = sec.text
                if "외국인" in sec_text or "기관" in sec_text:
                    inv_type = "외국인" if "외국인" in sec_text else "기관"
                    # 해당 섹션 내에서 ':'가 포함된 span(시간) 찾기
                    spans = sec.find_elements(By.TAG_NAME, "span")
                    for s in spans:
                        t_text = s.text.strip()
                        if ":" in t_text and ("오늘" in t_text or "어제" in t_text or "기준" in t_text):
                            base_times[inv_type] = t_text
                            break
            print(f"🕒 [{ranking_type}] Detected Base Times: {base_times}")
        except Exception as e:
            print(f"⚠️ [{ranking_type}] Base Time extraction failed: {e}")

        default_time = time.strftime('%Y-%m-%d %H:%M:%S')
        if "외국인" not in base_times: base_times["외국인"] = default_time
        if "기관" not in base_times: base_times["기관"] = default_time

        # 전체 종목 아이템 수집
        items = driver.find_elements(By.CSS_SELECTOR, "a[href*='/stocks/']")
        print(f"📦 [{ranking_type}] Found {len(items)} raw items")

        current_group_idx = 0
        groups = ["외국인", "기관", "개인", "기타"]
        group_counts = {"외국인": 0, "기관": 0}

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

                    # 🔍 종목코드 추출 (더 유연한 방식)
                    try:
                        href = item.get_attribute("href")
                        # 국내 주식 코드는 보통 /stocks/A005930 또는 /stocks/005930 형태임
                        code_match = re.search(r'/stocks/(?:A)?([0-9A-Z]{6,})', href)
                        if code_match:
                            stock_code = code_match.group(1)
                        else:
                            stock_code = ""
                    except:
                        stock_code = ""

                    # 국내 주식(6자리 숫자 등)이 아니면 ETF 분석에 의미가 없으므로 스킵 시도할 수 있으나,
                    # 우선은 모든 코드를 수집하여 상태를 확인합니다.
                    if not stock_code:
                        # 코드가 없으면 중복 제거 시 이름으로 구분하기 위해 임시 처리
                        pass

                    # 이름 보정 로직
                    if re.match(r'^[0-9,.\-+\s%]+(원)?$', name):
                        if len(text_lines) > 2:
                            name = text_lines[2]
                    
                    # 금액 정보 파싱
                    amount_str = ""
                    # [수정] 해당 그룹(외국인/기관)의 헤더 시간이 '어제'인지 확인하여 금액 0 처리
                    group_base_time = base_times.get(group_name, "")
                    is_yesterday = "어제" in group_base_time
                    
                    for line in text_lines:
                        # 종목 텍스트 자체에 '어제'가 포함된 경우도 체크 (안전장치)
                        if "어제" in line:
                            is_yesterday = True
                        if any(unit in line for unit in ["조", "억", "만"]):
                            amount_str = line.strip()
                    
                    # "어제" 데이터인 경우 금액을 0으로 강제 설정
                    if is_yesterday:
                        amount_val = 0.0
                        print(f"⚠️ [{ranking_type}] {group_name} - {name} ({stock_code}) 데이터가 '{group_base_time}' 것이므로 0으로 처리합니다.")
                    else:
                        amount_val = parse_amount(amount_str)
                    
                    # 데이터 저장용 dict 생성
                    all_data.append({
                        "investor": group_name,
                        "stock_name": name,
                        "stock_code": stock_code,
                        "amount": amount_val,
                        "ranking_type": ranking_type,
                        "collected_at": collected_at
                    })
                    group_counts[group_name] += 1
            except Exception as e:
                continue

        print(f"📊 [{ranking_type}] Final Counts -> 外: {group_counts.get('외국인', 0)}, 機: {group_counts.get('기관', 0)}")

        # 결과 저장 (Supabase)
        if all_data:
            # [중요] 중복 제거 및 유효성 검사
            unique_map = {}
            no_code_count = 0
            for item in all_data:
                if not item["stock_code"]:
                    no_code_count += 1
                    # 코드가 없으면 (이름, 투자자) 조합으로 키 생성하여 뭉침 방지 (로그용)
                    key = (item["investor"], f"NO_CODE_{item['stock_name']}", item["ranking_type"], item["collected_at"])
                else:
                    key = (item["investor"], item["stock_code"], item["ranking_type"], item["collected_at"])
                unique_map[key] = item
            
            all_data = list(unique_map.values())
            
            # 실제 DB에 넣을 때는 코드가 있는 것만 넣는 것이 안전함 (제약조건 때문)
            valid_data = [d for d in all_data if d["stock_code"]]
            
            print(f"📦 [{ranking_type}] 총 수집: {len(all_data)}개 (코드 없음: {no_code_count}개, DB 저장 대상: {len(valid_data)}개)")

            if valid_data:
                try:
                    # Supabase에 데이터 삽입 (upsert 사용)
                    response = supabase.table("toss_realtime_top100").upsert(
                        valid_data, 
                        on_conflict="investor, stock_code, ranking_type, collected_at"
                    ).execute()
                    print(f"🎉 [{ranking_type}] Supabase Save Complete (Total {len(valid_data)} items)")
                except Exception as e:
                    print(f"❌ [{ranking_type}] Supabase Save Error: {e}")
            else:
                print(f"⚠️ [{ranking_type}] 유효한 종목코드가 있는 데이터가 없습니다.")
                
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
            
        # 시작 시간 체크 (09:00 이전이면 대기)
        if not run_once and now.hour < 9:
            print(f"🕒 현재 시간(KST) {now.strftime('%H:%M:%S')} - 장 시작 전(09:00)입니다. 대기 중...")
            time.sleep(60)
            continue

        start_time = time.time()
        
        # [제거됨] delete_old_scores() 여기서는 호출하지 않음 (루프 진입 전 1회만 호출)
        
        print(f"=== 토스증권 수급 데이터 수집 시작 (시작 시각 KST: {now.strftime('%H:%M:%S')}) ===")
        
        # [수정] 한 턴(Buy/Sell) 동안 동일한 타임스탬프를 공유하도록 고정
        turn_timestamp = now.isoformat()
        
        try:
            get_toss_ranking("buy", collected_at=turn_timestamp)  # 순매수
            print("\n" + "="*30 + "\n")
            get_toss_ranking("sell", collected_at=turn_timestamp) # 순매도
            
            # 🚀 [변경] 로컬 계산 대신 Supabase 서버 사이드 함수(RPC) 호출
            print("\n📊 [Server-Side] YG Score 계산 및 업데이트 요청 중...")
            try:
                # RPC 호출: calculate_yg_score_server(target_time)
                supabase.rpc('calculate_yg_score_server', {'target_time': turn_timestamp}).execute()
                print("✅ [Server-Side] YG Score 업데이트 완료")
            except Exception as e:
                print(f"❌ [Server-Side] YG Score 업데이트 중 오류 발생: {e}")
                
        except Exception as e:
            print(f"❌ 메인 루프 실행 중 오류 발생: {e}")

        print("=== 이번 턴 수집 완료 ===")
        
        if run_once:
            print("🚀 1회 실행 모드 완료. 종료합니다.")
            break

        # [수정] 세션 종료 조건 또는 외부 종료 요청 체크
        now_check = datetime.utcnow() + timedelta(hours=9)
        if stop_requested:
            print("🛑 외부 요청에 의해 안전하게 프로세스를 종료합니다.")
            break
            
        if now_check.hour > end_hour or (now_check.hour == end_hour and now_check.minute >= end_minute):
            print(f"🕒 현재 시간(KST) {now_check.strftime('%H:%M:%S')} - 세션 종료 시간({end_hour:02d}:{end_minute:02d})이 되어 안전하게 종료합니다.")
            break
        
        elapsed_time = time.time() - start_time
        wait_time = 60 - elapsed_time
        
        if wait_time > 0:
            print(f"⏳ 다음 수집까지 {wait_time:.1f}초 대기...")
            # 대기 중에도 1초마다 종료 신호를 체크하기 위해 sleep을 쪼개서 수행
            for _ in range(int(wait_time)):
                if stop_requested: break
                time.sleep(1)
            # 남은 소수점 시간만큼 대기
            if not stop_requested:
                time.sleep(wait_time - int(wait_time))
        else:
            print("⏳ 대기 없이 바로 다음 수집 시작")
