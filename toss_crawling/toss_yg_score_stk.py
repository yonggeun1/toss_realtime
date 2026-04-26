import time
from datetime import datetime, timedelta
import re
import sys
import signal
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

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Supabase 클라이언트 임포트
try:
    from toss_crawling.supabase_client import supabase, delete_old_scores, load_etf_pdf_from_supabase, get_kst_now, check_market_open
except ImportError:
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from supabase_client import supabase, delete_old_scores, load_etf_pdf_from_supabase, get_kst_now, check_market_open


def parse_amount(amount_str):
    if not amount_str:
        return 0
    try:
        amount_str = amount_str.replace("순매수", "").replace("순매도", "").replace(",", "").replace(" ", "").replace("-", "").replace("원", "")
        total_amount = 0.0

        if "조" in amount_str:
            parts = amount_str.split("조")
            try:
                if parts[0].strip():
                    total_amount += float(parts[0]) * 10000
            except (ValueError, TypeError):
                pass
            amount_str = parts[1] if len(parts) > 1 else ""

        if "억" in amount_str:
            parts = amount_str.split("억")
            try:
                if parts[0].strip():
                    total_amount += float(parts[0])
            except (ValueError, TypeError):
                pass
            amount_str = parts[1] if len(parts) > 1 else ""

        if "만" in amount_str:
            parts = amount_str.split("만")
            try:
                if parts[0].strip():
                    total_amount += float(parts[0]) / 10000
            except (ValueError, TypeError):
                pass

        return round(total_amount, 4)
    except Exception:
        return 0


def parse_date(date_str):
    """토스증권 날짜 형식(오늘, 어제, 1월 30일 등)을 YYYY-MM-DD 형식으로 변환"""
    kst_now = get_kst_now()
    today_str = kst_now.strftime('%Y-%m-%d')
    current_year = kst_now.year

    if not date_str:
        return today_str
    if "오늘" in date_str:
        return today_str
    if "어제" in date_str:
        return (kst_now - timedelta(days=1)).strftime('%Y-%m-%d')

    match = re.search(r'(\d+)월\s*(\d+)일', date_str)
    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        return f"{current_year}-{month:02d}-{day:02d}"

    if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
        return date_str

    return today_str


def get_toss_ranking(ranking_type="buy", collected_at=None):
    ranking_name = "순매수" if ranking_type == "buy" else "순매도"

    if collected_at is None:
        collected_at = get_kst_now().isoformat()

    # 09:00~10:00 장 초반 보호 로직 여부 판단
    is_opening_period = False
    try:
        dt_collected = datetime.fromisoformat(collected_at)
        if dt_collected.hour == 9:
            is_opening_period = True
    except (ValueError, AttributeError):
        pass

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        print(f"🚀 [{ranking_type}] 연결 시도 {attempt}/{max_retries}: https://www.tossinvest.com/?ranking-type=domestic_investor_trend&ranking={ranking_type}")

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,5000")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        url = f"https://www.tossinvest.com/?ranking-type=domestic_investor_trend&ranking={ranking_type}"
        all_data = []

        try:
            driver.get(url)
            wait = WebDriverWait(driver, 20)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/stocks/']")))
            time.sleep(5)

            for _ in range(5):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

            # 기준 시간 추출 (투자자별)
            base_times = {"외국인": "", "기관": ""}
            try:
                spans = driver.find_elements(By.XPATH, "//span[contains(text(), '기준') and (contains(text(), '오늘') or contains(text(), '어제'))]")
                for s in spans:
                    t_text = s.text.strip()
                    try:
                        curr = s
                        parent_text = ""
                        for _ in range(4):
                            try:
                                curr = curr.find_element(By.XPATH, "..")
                                parent_text += curr.text
                            except Exception:
                                break

                        if "외국인" in parent_text and not base_times["외국인"]:
                            base_times["외국인"] = t_text
                        if "기관" in parent_text and not base_times["기관"]:
                            base_times["기관"] = t_text
                    except Exception:
                        pass

                if not base_times["기관"] or not base_times["외국인"]:
                    sections = driver.find_elements(By.TAG_NAME, "section")
                    for sec in sections:
                        sec_text = sec.text
                        if "외국인" in sec_text and not base_times["외국인"]:
                            for s in sec.find_elements(By.TAG_NAME, "span"):
                                if ":" in s.text and ("오늘" in s.text or "어제" in s.text):
                                    base_times["외국인"] = s.text.strip()
                                    break
                        if "기관" in sec_text and not base_times["기관"]:
                            for s in sec.find_elements(By.TAG_NAME, "span"):
                                if ":" in s.text and ("오늘" in s.text or "어제" in s.text):
                                    base_times["기관"] = s.text.strip()
                                    break

                print(f"🕒 [{ranking_type}] 검출된 기준 시각: {base_times}")
            except Exception as e:
                print(f"⚠️ 기준 시각 검출 중 오류: {e}")

            default_time = time.strftime('%Y-%m-%d %H:%M:%S')
            if not base_times.get("외국인"):
                base_times["외국인"] = default_time
            if not base_times.get("기관"):
                base_times["기관"] = default_time

            items = driver.find_elements(By.CSS_SELECTOR, "a[href*='/stocks/']")
            current_group_idx = 0
            groups = ["외국인", "기관", "개인", "기타"]
            group_counts = {"외국인": 0, "기관": 0}

            for idx, item in enumerate(items):
                try:
                    raw_text = item.text
                    if not raw_text:
                        continue
                    text_lines = [line.strip() for line in raw_text.split('\n') if line.strip()]

                    if len(text_lines) >= 2:
                        rank = text_lines[0]
                        name = text_lines[1]

                        if rank == '1' and idx > 10:
                            if group_counts.get(groups[current_group_idx], 0) >= 80:
                                current_group_idx += 1

                        group_name = groups[current_group_idx] if current_group_idx < len(groups) else "Unknown"

                        if group_name not in ["외국인", "기관"]:
                            continue
                        if group_counts[group_name] >= 100:
                            continue

                        try:
                            href = item.get_attribute("href")
                            code_match = re.search(r'/stocks/(?:A)?([0-9A-Z]{6,})', href)
                            stock_code = code_match.group(1) if code_match else ""
                        except (AttributeError, TypeError):
                            stock_code = ""

                        group_base_time = base_times.get(group_name, "")
                        is_yesterday = "어제" in group_base_time

                        if group_name == "기관" and is_opening_period:
                            is_yesterday = True
                            if group_counts[group_name] == 0:
                                print("🛡️ [기관] 장 초반(09:00~10:00) 보호 로직 작동: 금액을 0으로 고정합니다.")

                        if group_name == "기관" and is_yesterday and group_counts[group_name] == 0 and not is_opening_period:
                            print(f"ℹ️ [기관] 섹션이 '어제'로 감지되었습니다. 모든 금액을 0으로 처리합니다. (기준: {group_base_time})")

                        amount_str = ""
                        for line in text_lines:
                            if "어제" in line:
                                is_yesterday = True
                            if any(unit in line for unit in ["조", "억", "만"]):
                                amount_str = line.strip()

                        amount_val = 0.0 if is_yesterday else parse_amount(amount_str)

                        all_data.append({
                            "investor": group_name,
                            "stock_name": name,
                            "stock_code": stock_code,
                            "amount": amount_val,
                            "ranking_type": ranking_type,
                            "collected_at": collected_at,
                        })
                        group_counts[group_name] += 1
                except Exception:
                    continue

            print(f"📊 [{ranking_type}] 수집 결과 -> 外: {group_counts.get('외국인', 0)}, 機: {group_counts.get('기관', 0)}")

            if group_counts.get("외국인", 0) >= 100 and group_counts.get("기관", 0) >= 100:
                print(f"✅ [{ranking_type}] 목표치(200개) 달성! 저장을 시작합니다.")

                unique_map = {}
                no_code_count = 0
                for item in all_data:
                    if not item["stock_code"]:
                        no_code_count += 1
                        key = (item["investor"], f"NO_CODE_{item['stock_name']}", item["ranking_type"], item["collected_at"])
                    else:
                        key = (item["investor"], item["stock_code"], item["ranking_type"], item["collected_at"])
                    unique_map[key] = item

                valid_data = [d for d in unique_map.values() if d["stock_code"]]
                print(f"📦 [{ranking_type}] 최종 유효 데이터: {len(valid_data)}개 (코드 없음 {no_code_count}개 제외)")

                if valid_data:
                    try:
                        supabase.table("toss_yg_score_stk").upsert(
                            valid_data, on_conflict="investor, stock_code, ranking_type, collected_at"
                        ).execute()
                        print(f"🎉 [{ranking_type}] Supabase 저장 완료")
                        driver.quit()
                        return
                    except Exception as e:
                        print(f"❌ [{ranking_type}] 저장 에러: {e}")
            else:
                print(f"⚠️ [{ranking_type}] 수집 데이터 부족 (外:{group_counts.get('외국인')}, 機:{group_counts.get('기관')}). 재시도합니다.")

        except Exception as e:
            print(f"❌ [{ranking_type}] 오류 발생: {e}")
        finally:
            driver.quit()

        time.sleep(5)

    print(f"🚨 [{ranking_type}] {max_retries}회 시도에도 불구하고 목표 데이터를 모두 수집하지 못했습니다.")


if __name__ == "__main__":

    run_once = "--once" in sys.argv
    is_morning = "morning" in sys.argv
    is_afternoon = "afternoon" in sys.argv

    print("Loading ETF PDF data...")
    cached_pdf_data = load_etf_pdf_from_supabase()

    now = get_kst_now()
    end_hour, end_minute = 15, 20

    if is_morning:
        end_hour, end_minute = 12, 0
    elif is_afternoon:
        end_hour, end_minute = 15, 20

    print(f"🕒 현재 시각(KST): {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"=== 토스증권 수급 데이터 수집 시작 (세션: {'오전' if is_morning else '오후' if is_afternoon else '기본'}, 종료 예정: {end_hour:02d}:{end_minute:02d}) ===")

    if not run_once and (now.hour > end_hour or (now.hour == end_hour and now.minute >= end_minute)):
        print(f"⚠️ 현재 시간({now.strftime('%H:%M')})이 이미 종료 시간({end_hour:02d}:{end_minute:02d})을 지났습니다. 프로그램을 종료합니다.")
        sys.exit(0)

    is_market_open_confirmed = False

    while True:
        now = get_kst_now()
        current_time_str = now.strftime("%H%M")

        # 시작 시간 체크 (08:50 이전이면 대기)
        if not run_once and current_time_str < "0850":
            print(f"🕒 현재 시간(KST) {now.strftime('%H:%M:%S')} - 시작 전(08:50)입니다. 대기 중...", end='\r')
            time.sleep(30)
            continue

        # 시장 개장 여부 확인 (08:58 이후 프리마켓 데이터 기준)
        if not run_once and not is_market_open_confirmed:
            if current_time_str < "0858":
                print(f"🕒 시장 개장 여부 확인을 위해 08:58까지 대기합니다... (현재: {now.strftime('%H:%M:%S')})", end='\r')
                time.sleep(10)
                continue

            today_str = now.strftime("%Y-%m-%d")
            print(f"\n🔍 [{today_str}] 시장 개장 여부 확인 중 (프리마켓 데이터 기준)...")
            try:
                if not check_market_open(today_str):
                    print(f"ℹ️ [{today_str}] 프리마켓 데이터가 없습니다. 장이 열리지 않은 날로 판단하여 종료합니다. (기존 데이터 보존)")
                    sys.exit(0)

                print(f"✅ [{today_str}] 개장일 확인됨. 기존 데이터를 정리하고 수집을 시작합니다.")
                if not is_afternoon:
                    print("🧹 Cleaning up old data (older than today) before starting loop...")
                    delete_old_scores()
                is_market_open_confirmed = True
            except Exception as e:
                print(f"⚠️ 개장 확인 중 오류 발생: {e}. 안전을 위해 1분 후 재시도합니다.")
                time.sleep(60)
                continue

        start_time = time.time()

        print(f"=== 토스증권 수급 데이터 수집 시작 (시작 시각 KST: {now.strftime('%H:%M:%S')}) ===")
        turn_timestamp = now.isoformat()

        try:
            get_toss_ranking("buy", collected_at=turn_timestamp)
            print("\n" + "=" * 30 + "\n")
            get_toss_ranking("sell", collected_at=turn_timestamp)

            print("\n📊 [Server-Side] YG Score 계산 및 업데이트 요청 중...")
            try:
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

        now_check = get_kst_now()
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
            for _ in range(int(wait_time)):
                if stop_requested:
                    break
                time.sleep(1)
            if not stop_requested:
                time.sleep(wait_time - int(wait_time))
        else:
            print("⏳ 대기 없이 바로 다음 수집 시작")
