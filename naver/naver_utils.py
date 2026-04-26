import re
import requests
from bs4 import BeautifulSoup


def get_naver_sise(url, market_name, type_name, now_kst):
    """네이버 증권 시세 페이지(상승/하락 테이블)를 크롤링하여 종목 리스트를 반환합니다."""
    print(f"🚀 [{market_name} {type_name}] 크롤링 중: {url}")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = 'euc-kr'

        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', class_='type_2')
        if not table:
            print(f"❌ 테이블을 찾을 수 없습니다: {market_name} {type_name}")
            return []

        rows = table.find_all('tr')
        collected_data = []

        for row in rows:
            tds = row.find_all('td')
            if len(tds) < 10:
                continue

            a_tag = tds[1].find('a')
            if not a_tag:
                continue

            stk_nm = a_tag.text.strip()
            href = a_tag.get('href', '')
            code_match = re.search(r'code=(\d{6})', href)
            stk_cd = code_match.group(1) if code_match else ""

            close_pric_str = tds[2].text.strip().replace(',', '')
            close_pric = float(close_pric_str) if close_pric_str else 0.0

            pre_str = re.sub(r'[^0-9.]', '', tds[3].text.strip().replace(',', ''))
            pre = float(pre_str) if pre_str else 0.0

            flu_rt_str = tds[4].text.strip().replace('%', '').replace(',', '').replace('+', '')
            flu_rt = float(flu_rt_str) if flu_rt_str else 0.0

            trde_qty_str = tds[5].text.strip().replace(',', '')
            trde_qty = int(trde_qty_str) if trde_qty_str else 0

            if stk_cd:
                if "하락" in type_name and flu_rt > 0:
                    flu_rt = -flu_rt
                    pre = -pre

                collected_data.append({
                    "stk_cd": stk_cd,
                    "stk_nm": stk_nm,
                    "close_pric": close_pric,
                    "pre": pre,
                    "flu_rt": flu_rt,
                    "trde_qty": trde_qty,
                    "market": market_name,
                    "type": type_name,
                    "collected_at": now_kst,
                })

        return collected_data

    except Exception as e:
        print(f"❌ 오류 발생 ({market_name} {type_name}): {e}")
        return []
