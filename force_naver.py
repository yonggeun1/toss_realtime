import requests
from bs4 import BeautifulSoup
import re
import sys
import os
import time
from datetime import datetime, timedelta

try:
    from toss_crawling.supabase_client import supabase
except ImportError:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.append(project_root)
    from toss_crawling.supabase_client import supabase

def get_naver_sise(url, market_name, type_name, now_kst):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'euc-kr'
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', class_='type_2')
        if not table: return []
        rows = table.find_all('tr')
        collected_data = []
        for row in rows:
            tds = row.find_all('td')
            if len(tds) < 10: continue
            a_tag = tds[1].find('a')
            if not a_tag: continue
            stk_nm = a_tag.text.strip()
            code_match = re.search(r'code=(\d{6})', a_tag.get('href', ''))
            stk_cd = code_match.group(1) if code_match else ""
            close_pric = float(tds[2].text.strip().replace(',', ''))
            flu_rt = float(tds[4].text.strip().replace('%', '').replace(',', '').replace('+', ''))
            if stk_cd:
                if "하락" in type_name and flu_rt > 0: flu_rt = -flu_rt
                collected_data.append({
                    "stk_cd": stk_cd, "stk_nm": stk_nm, "close_pric": close_pric,
                    "flu_rt": flu_rt, "market": market_name, "type": type_name, "collected_at": now_kst
                })
        return collected_data
    except: return []

def run_once():
    now = datetime.utcnow() + timedelta(hours=9)
    turn_timestamp = now.replace(microsecond=0).isoformat()
    urls = [
        ("https://finance.naver.com/sise/sise_rise.naver?sosok=0", "KOSPI", "상승"),
        ("https://finance.naver.com/sise/sise_rise.naver?sosok=1", "KOSDAQ", "상승"),
        ("https://finance.naver.com/sise/sise_fall.naver?sosok=0", "KOSPI", "하락"),
        ("https://finance.naver.com/sise/sise_fall.naver?sosok=1", "KOSDAQ", "하락")
    ]
    all_collected = []
    for url, m, t in urls:
        all_collected.extend(get_naver_sise(url, m, t, turn_timestamp))
        time.sleep(0.2)
    
    if all_collected:
        print(f"Collected {len(all_collected)} items. Updating DB...")
        supabase.table("naver_realtime_stk").delete().neq("stk_cd", "").execute()
        batch_size = 1000
        for i in range(0, len(all_collected), batch_size):
            supabase.table("naver_realtime_stk").upsert(all_collected[i:i+batch_size]).execute()
        supabase.rpc('calculate_naver_etf_score', {}).execute()
        print("Done.")

if __name__ == '__main__':
    run_once()
