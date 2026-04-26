import sys
import os
import time

# Supabase 연동
try:
    from toss_crawling.supabase_client import supabase, get_kst_now
except ImportError:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.append(project_root)
    from toss_crawling.supabase_client import supabase, get_kst_now

try:
    from naver.naver_utils import get_naver_sise
except ImportError:
    from naver_utils import get_naver_sise


def delete_old_premarket_data():
    """오늘(KST 기준) 이전의 프리마켓 관련 데이터를 모두 삭제합니다."""
    try:
        now_kst = get_kst_now()
        today_start_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
        threshold_str = today_start_kst.isoformat()

        print(f"🧹 [프리마켓] 오늘({today_start_kst.strftime('%Y-%m-%d')}) 이전 데이터 삭제 중...")
        supabase.table("naver_premarket_stk").delete().lt("collected_at", threshold_str).execute()
        supabase.table("naver_premarket_etf").delete().lt("updated_at", threshold_str).execute()
        print("✅ [프리마켓] 지난 데이터 삭제 프로세스 완료")
    except Exception as e:
        print(f"🚨 [프리마켓] 지난 데이터 삭제 오류: {e}")


def main():
    now = get_kst_now()
    current_time_str = now.strftime("%H%M")
    target_time = "0851"

    print(f"=== 네이버 프리마켓(Nextrade) 최종 집계 시작 (현재: {now.strftime('%H:%M:%S')}) ===")

    # 08:51분 이전이라면 대기
    if current_time_str < target_time:
        print(f"🕒 아직 {target_time[:2]}:{target_time[2:]} 전입니다. {target_time}까지 대기합니다.")
        while True:
            now = get_kst_now()
            current_time_str = now.strftime("%H%M")
            if current_time_str >= target_time:
                break
            print(f"🕒 대기 중... ({now.strftime('%H:%M:%S')})", end='\r')
            time.sleep(30)
        print("\n🚀 지정된 시간이 되어 수집을 시작합니다.")

    turn_timestamp = now.replace(microsecond=0).isoformat()

    urls = [
        ("https://finance.naver.com/sise/nxt_sise_rise.naver?sosok=0", "KOSPI", "상승"),
        ("https://finance.naver.com/sise/nxt_sise_rise.naver?sosok=1", "KOSDAQ", "상승"),
        ("https://finance.naver.com/sise/nxt_sise_fall.naver?sosok=0", "KOSPI", "하락"),
        ("https://finance.naver.com/sise/nxt_sise_fall.naver?sosok=1", "KOSDAQ", "하락"),
    ]

    all_collected = []
    for url, market, type_name in urls:
        data = get_naver_sise(url, market, type_name, turn_timestamp)
        all_collected.extend(data)
        time.sleep(0.5)

    if all_collected:
        print(f"✨ 총 {len(all_collected)}개 프리마켓 데이터 수집 완료")
        try:
            print("🔍 기존 데이터와 비교 중...")
            existing_response = supabase.table("naver_premarket_stk").select("stk_cd, close_pric, flu_rt, trde_qty").execute()
            existing_data = {row['stk_cd']: row for row in existing_response.data}

            is_changed = len(all_collected) != len(existing_data)
            if not is_changed:
                for record in all_collected:
                    cd = record['stk_cd']
                    ext = existing_data.get(cd)
                    if ext is None or (
                        float(record['close_pric']) != float(ext['close_pric']) or
                        float(record['flu_rt']) != float(ext['flu_rt']) or
                        int(record['trde_qty']) != int(ext['trde_qty'])
                    ):
                        is_changed = True
                        break

            if not is_changed:
                print("ℹ️ 기존 데이터와 값이 동일합니다. (장이 열리지 않았거나 업데이트가 없는 상태)")
                print("⏩ 업데이트를 스킵하고 종료합니다.")
                sys.exit(0)

            print("🔄 데이터 변경이 감지되었습니다. 업데이트를 진행합니다.")
            supabase.table("naver_premarket_stk").delete().gte("stk_cd", "0").execute()

            batch_size = 1000
            for i in range(0, len(all_collected), batch_size):
                supabase.table("naver_premarket_stk").upsert(all_collected[i:i + batch_size]).execute()
            print(f"🎉 Supabase 저장 완료 ({len(all_collected)}개)")

            print("📊 [Server-Side] 네이버 프리마켓 점수 계산 요청 중...")
            supabase.rpc('calculate_naver_premarket_score', {}).execute()
            print("✅ [Server-Side] 네이버 프리마켓 점수 업데이트 완료")
        except Exception as e:
            print(f"❌ 저장 및 계산 중 오류: {e}")

    print("=== 프리마켓 수집 및 집계 완료. 프로세스를 종료합니다. ===")
    sys.exit(0)


if __name__ == "__main__":
    main()
