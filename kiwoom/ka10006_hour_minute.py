import sys
import os
import time
import re
from datetime import datetime

# 프로젝트 루트를 path에 추가하여 toss_crawling.supabase_client 임포트 가능하게 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.append(project_root)

# 수파베이스 클라이언트 임포트
from toss_crawling.supabase_client import supabase
# 키움 공통 모듈 임포트
from kiwoom_login_common import get_token, fn_call_mrkcond, DEFAULT_HOST

def clean_value(val):
    """키움 API 특유의 +, - 기호 및 콤마 제거 후 숫자로 변환"""
    if val is None or val == "":
        return 0
    try:
        # 기호 및 숫자가 아닌 문자 제거
        cleaned = re.sub(r'[^\d.]', '', str(val))
        if not cleaned:
            return 0
        return float(cleaned)
    except:
        return 0

def fn_ka10006(token, data, host=DEFAULT_HOST):
    """주식시분요청 (ka10006) - 종목별 분 단위 시세 정보"""
    return fn_call_mrkcond(
        token=token,
        api_id='ka10006',
        data=data,
        host=host
    )

def main():
    # 인자 확인 (GitHub Actions 세션 분할용)
    is_morning = "morning" in sys.argv
    is_afternoon = "afternoon" in sys.argv
    
    # 종료 시간 설정
    if is_morning:
        end_hour, end_minute = 12, 0
        print("🕒 오전 세션 모드: 12:00에 종료됩니다.")
    else:
        end_hour, end_minute = 15, 20
        print("🕒 일반/오후 세션 모드: 15:20에 종료됩니다.")

    print("🚀 [ka10006] 실시간 시세 수집 엔진 시작")
    
    while True:
        now = datetime.now()
        current_time = now.strftime("%H%M")

        # 1. 세션 종료 체크
        if now.hour > end_hour or (now.hour == end_hour and now.minute >= end_minute):
            print(f"🏁 세션 종료 시간({end_hour:02d}:{end_minute:02d})이 되어 프로그램을 종료합니다.")
            break

        # 2. 장 시작 전 대기 (08:55 이전 실행 시)
        if current_time < "0855":
            print(f"💤 장 시작 전입니다. 08:55까지 대기합니다... (현재: {now.strftime('%H:%M:%S')})", end="\r")
            time.sleep(30)
            continue

        print(f"\n🔔 실시간 수집 사이클 시작: {now.strftime('%H:%M:%S')}")
        
        # [수집 로직 시작]
        try:
            token = get_token()
            res_holdings = supabase.table("holding_name").select("holding_code, holding_name").execute()
            holdings = res_holdings.data
            
            if not holdings:
                print("🚨 조회할 종목이 없습니다. 1분 후 재시도합니다.")
                time.sleep(60)
                continue

            collected_data = []
            cycle_start = time.time()
            
            for idx, item in enumerate(holdings):
                # 다시 한 번 종료 시간 체크 (루프 도중 15:20이 지날 수 있음)
                if datetime.now().strftime("%H%M") > "1520": break

                code = item['holding_code']
                name = item['holding_name']
                if not re.match(r'^\d{6}$', code): continue

                res = fn_ka10006(token, {'stk_cd': code})
                if res and res.get('return_code') == 5:
                    time.sleep(1.0)
                    res = fn_ka10006(token, {'stk_cd': code})

                if res and res.get('return_code') == 0:
                    record = {
                        "stk_cd": code, "stk_nm": name, "date": res.get("date"),
                        "close_pric": clean_value(res.get("close_pric")),
                        "pre": clean_value(res.get("pre")),
                        "flu_rt": clean_value(res.get("flu_rt")),
                        "open_pric": clean_value(res.get("open_pric")),
                        "high_pric": clean_value(res.get("high_pric")),
                        "low_pric": clean_value(res.get("low_pric")),
                        "trde_qty": int(clean_value(res.get("trde_qty"))),
                        "trde_prica": int(clean_value(res.get("trde_prica"))),
                        "cntr_str": clean_value(res.get("cntr_str")),
                        "collected_at": datetime.now().astimezone().isoformat()
                    }
                    collected_data.append(record)
                time.sleep(0.4)

            # 데이터 적재 및 계산 RPC 호출
            if collected_data:
                batch_size = 100
                for i in range(0, len(collected_data), batch_size):
                    batch = collected_data[i:i+batch_size]
                    supabase.table("kiwoom_realtime_stk").insert(batch).execute()
                    supabase.rpc("calculate_kiwoom_etf_score", {}).execute()
                
                print(f"✅ 사이클 완료: {len(collected_data)}개 종목 ({time.time() - cycle_start:.1f}초 소요)")
            
            print("⏳ 다음 사이클 대기 (10초)...")
            time.sleep(10)

        except Exception as e:
            print(f"🚨 오류 발생: {e}")
            time.sleep(60)



if __name__ == '__main__':
    # 인코딩 설정 (윈도우 CMD/PowerShell 유니코드 출력 대응)
    if sys.stdout.encoding != 'utf-8':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    main()
