import os
import sys
import requests
import re
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv, find_dotenv

# 기본 호스트 (모의투자)
DEFAULT_HOST = "https://mockapi.kiwoom.com"
DEFAULT_SOCKET_URL = 'wss://mockapi.kiwoom.com:10000/api/dostk/websocket'

# 기본 타겟 시간 (장중 스코어 계산용)
DEFAULT_TARGET_TIMES = ["09:30", "10:00", "11:30", "13:20", "14:30", "15:30", "18:00"]

def fn_au10001(data, host=DEFAULT_HOST):
    """토큰 발급 함수"""
    url = host + '/oauth2/token'
    headers = {'Content-Type': 'application/json;charset=UTF-8'}
    try:
        response = requests.post(url, headers=headers, json=data)
    except requests.exceptions.RequestException as e:
        print(f"HTTP 요청 오류: {e}")
        return None
    if response.status_code != 200:
        print("토큰 요청 실패:", response.status_code, response.text)
        return None
    
    res_json = response.json()
    token = res_json.get('token')
    if not token:
        print("⚠️ 토큰 발급 응답에 'token' 필드가 없습니다:", res_json)
    return token

def get_token():
    """
    환경 변수(API_KEY, API_SECRET_KEY)를 로드하고
    Kiwoom API 토큰을 발급받아 반환합니다.
    실패 시 프로그램을 종료합니다.
    """
    load_dotenv(find_dotenv(), override=True)

    api_key = os.getenv('API_KEY')
    api_secret_key = os.getenv('API_SECRET_KEY')

    if not api_key or not api_secret_key:
        print("❌ [오류] API_KEY 또는 API_SECRET_KEY가 환경 변수에 설정되지 않았습니다.")
        sys.exit(1)

    print("=== Kiwoom API 토큰 발급 시도 ===")
    token = fn_au10001({
        'grant_type': 'client_credentials',
        'appkey': api_key.strip(),
        'secretkey': api_secret_key.strip()
    })

    if not token:
        print("❌ [오류] 토큰 발급 실패. API 키를 확인해주세요.")
        sys.exit(1)
        
    print(f"✅ 토큰 발급 성공")
    return token

def get_korea_timestamp(target_times=None):
    """
    한국 시간(KST) 기준 현재 날짜와 가장 가까운 타겟 시간을 반환합니다.
    주말(토/일)인 경우 직전 금요일 15:30으로 처리합니다.
    """
    if target_times is None:
        target_times = DEFAULT_TARGET_TIMES

    KST = timezone(timedelta(hours=9))
    now = datetime.now(KST)
    
    # 주말(토=5, 일=6) 처리 -> 금요일 15:30
    if now.weekday() >= 5:
        days_to_subtract = 1 if now.weekday() == 5 else 2
        friday = now - timedelta(days=days_to_subtract)
        return friday.strftime("%Y-%m-%d"), "15:30"

    today_str = now.strftime("%Y-%m-%d")
    
    time_diffs = []
    for t_str in target_times:
        h, m = map(int, t_str.split(':'))
        target_dt = now.replace(hour=h, minute=m, second=0, microsecond=0)
        diff = abs((target_dt - now).total_seconds())
        time_diffs.append((diff, t_str))
    
    if not time_diffs:
        return today_str, "09:00"

    nearest_time_str = min(time_diffs, key=lambda x: x[0])[1]
    
    return today_str, nearest_time_str

def clean_amount(val):
    if pd.isna(val): return 0
    try:
        val = str(val).strip().replace(',', '')
        val = val.replace('▼', '-').replace('▲', '').replace('--', '-')
        if val.startswith('(') and val.endswith(')'):
            val = '-' + val[1:-1]
        val = re.sub(r'[^\d\.\-]', '', val)
        if not val or val == '-':
            return 0
        return int(float(val))
    except:
        return 0

def clean_rate(val):
    if pd.isna(val): return 0.0
    try:
        val = str(val).strip().replace(',', '')
        val = val.replace('▼', '-').replace('▲', '').replace('--', '-')
        if val.startswith('(') and val.endswith(')'):
            val = '-' + val[1:-1]
        val = re.sub(r'[^\d\.\-]', '', val)
        if not val or val == '-':
            return 0.0
        return float(val)
    except:
        return 0.0

# --- 공통 데이터 처리 및 체크 유틸리티 ---

def handle_empty_data(df, label="데이터", wait_sec=60):
    if df is None or df.empty:
        print(f"❌ {label}를 가져오지 못했습니다. {wait_sec}초 후 재시도합니다.")
        time.sleep(wait_sec)
        return True
    return False

def check_is_holiday_by_data(df_current, file_path, date_col='거래일', time_col='거래시간', compare_cols=None):
    """
    기존 파일의 마지막 종가(15:30) 데이터와 현재 수집된 데이터를 비교하여 휴장일 여부 판단
    """
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return False
        
    try:
        existing_df = pd.read_csv(file_path, dtype={date_col: str, '종목코드': str})
        df_close = existing_df[existing_df[time_col] == '15:30']
        if df_close.empty:
            return False
            
        last_close_date = df_close[date_col].iloc[-1]
        today_date = df_current[date_col].iloc[0]
        
        if today_date != last_close_date:
            last_close_snapshot = existing_df[(existing_df[date_col] == last_close_date) & (existing_df[time_col] == '15:30')]
            
            if compare_cols is None:
                compare_cols = ['종목코드', '현재가', '거래량'] 
            
            valid_cols = [c for c in compare_cols if c in df_current.columns and c in last_close_snapshot.columns]
            
            new_check = df_current[valid_cols].sort_values('종목코드').reset_index(drop=True)
            old_check = last_close_snapshot[valid_cols].sort_values('종목코드').reset_index(drop=True)
            
            if new_check.equals(old_check):
                KST = timezone(timedelta(hours=9))
                now_kst = datetime.now(KST)
                # 장중 시간(10:30 이후)임에도 데이터가 이전 종가와 같으면 휴장일로 판단
                if now_kst.weekday() < 5 and (now_kst.hour > 10 or (now_kst.hour == 10 and now_kst.minute >= 30)):
                    return True
    except Exception as e:
        print(f"⚠️ 휴장일 체크 중 오류: {e}")
        
    return False

def is_duplicate_snapshot(df_new, file_path, date_val, time_val, date_col='거래일', time_col='거래시간', compare_cols=None):
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return False
        
    try:
        existing_df = pd.read_csv(file_path, dtype={date_col: str, '종목코드': str})
        target_snapshot = existing_df[(existing_df[date_col] == date_val) & (existing_df[time_col] == time_val)]
        
        if target_snapshot.empty:
            return False
            
        if compare_cols is None:
            compare_cols = ['종목코드', '현재가', '거래량']
            
        valid_cols = [c for c in compare_cols if c in df_new.columns and c in target_snapshot.columns]
        
        new_data = df_new[valid_cols].sort_values('종목코드').reset_index(drop=True)
        old_data = target_snapshot[valid_cols].sort_values('종목코드').reset_index(drop=True)
        
        return new_data.equals(old_data)
    except:
        return False

def save_data_to_csv_safe(df_new, file_path, subset_keys=['거래일', '거래시간', '종목코드']):
    """
    중복을 제거하며 CSV 파일에 데이터를 안전하게 누적 저장
    """
    try:
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            # 📌 종목코드 및 날짜 관련 컬럼을 문자열로 고정하여 로드 (0 누락 방지)
            dtype_dict = {subset_keys[0]: str, subset_keys[2] if len(subset_keys)>2 else '종목코드': str}
            existing_df = pd.read_csv(file_path, dtype=dtype_dict)
            
            combined_df = pd.concat([existing_df, df_new], ignore_index=True)
            before_len = len(combined_df)
            combined_df.drop_duplicates(subset=subset_keys, keep='last', inplace=True)
            after_len = len(combined_df)
            
            combined_df.to_csv(file_path, mode='w', header=True, index=False, encoding='utf-8-sig')
            
            added_count = after_len - len(existing_df)
            print(f"💾 저장 완료: 통합 {before_len}건 -> 중복제거 후 {after_len}건 (추가/갱신: {added_count}건)")
            return True
        else:
            df_new.to_csv(file_path, mode='w', header=True, index=False, encoding='utf-8-sig')
            print(f"💾 신규 파일 생성 및 {len(df_new)}건 저장 완료.")
            return True
    except Exception as e:
        print(f"❌ 데이터 저장 중 오류 발생: {e}")
        return False

# --- Kiwoom Data API Functions ---

def fn_call_etf(token, api_id, data, cont_yn='N', next_key='', host=DEFAULT_HOST):
    """ETF 관련 API 공통 호출 함수 (URL: /api/dostk/etf)"""
    url = host + '/api/dostk/etf'
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'authorization': f'Bearer {token}',
        'cont-yn': cont_yn,
        'next-key': next_key,
        'api-id': api_id,
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code != 200:
            print(f"[{api_id}] API 호출 오류: {response.status_code} {response.text}")
            return {}
        result = response.json()
        result['server_next_key'] = response.headers.get('next-key', '')
        result['server_cont_yn'] = response.headers.get('cont-yn', '')
        return result
    except Exception as e:
        print(f"[{api_id}] Exception occurred: {e}")
        return {}

def fn_call_mrkcond(token, api_id, data, cont_yn='N', next_key='', host=DEFAULT_HOST):
    """장중투자자별매매요청 관련 API 호출 함수 (URL: /api/dostk/mrkcond)"""
    url = host + '/api/dostk/mrkcond'
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'authorization': f'Bearer {token}',
        'cont-yn': cont_yn,
        'next-key': next_key,
        'api-id': api_id,
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code != 200:
            print(f"[{api_id}] API 호출 오류: {response.status_code} {response.text}")
            return {}
        result = response.json()
        result['server_next_key'] = response.headers.get('next-key', '')
        result['server_cont_yn'] = response.headers.get('cont-yn', '')
        return result
    except Exception as e:
        print(f"[{api_id}] Exception occurred: {e}")
        return {}

def fn_call_stkinfo(token, api_id, data, cont_yn='N', next_key='', host=DEFAULT_HOST):
    """종목정보 관련 API 호출 함수 (URL: /api/dostk/stkinfo)"""
    url = host + '/api/dostk/stkinfo'
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'authorization': f'Bearer {token}',
        'cont-yn': cont_yn,
        'next-key': next_key,
        'api-id': api_id,
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code != 200:
            print(f"[{api_id}] API 호출 오류: {response.status_code} {response.text}")
            return {}
        result = response.json()
        result['server_next_key'] = response.headers.get('next-key', '')
        result['server_cont_yn'] = response.headers.get('cont-yn', '')
        return result
    except Exception as e:
        print(f"[{api_id}] Exception occurred: {e}")
        return {}

def fn_call_chart(token, api_id, data, cont_yn='N', next_key='', host=DEFAULT_HOST):
    """차트 관련 API 호출 함수 (URL: /api/dostk/chart)"""
    url = host + '/api/dostk/chart'
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'authorization': f'Bearer {token}',
        'cont-yn': cont_yn,
        'next-key': next_key,
        'api-id': api_id,
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code != 200:
            print(f"[{api_id}] API 호출 오류: {response.status_code} {response.text}")
            return {}
        result = response.json()
        result['server_next_key'] = response.headers.get('next-key', '')
        result['server_cont_yn'] = response.headers.get('cont-yn', '')
        return result
    except Exception as e:
        print(f"[{api_id}] Exception occurred: {e}")
        return {}
