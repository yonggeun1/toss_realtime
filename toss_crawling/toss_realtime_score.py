import pandas as pd
import os
import sys

# 콘솔 출력 인코딩 설정 (Windows 환경 대응)
sys.stdout.reconfigure(encoding='utf-8')

# 모듈 경로 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

# Supabase 클라이언트 함수 임포트
from toss_crawling.supabase_client import (
    load_toss_data_from_supabase, 
    load_etf_pdf_from_supabase, 
    save_score_to_supabase,
    delete_old_scores
)

# 전역 변수로 캐시 관리
_cached_pdf = None

def calculate_yg_score(df_pdf=None, force_load=False):
    global _cached_pdf
    
    # 1. 실시간 수급 데이터 로드 (Supabase) - 이건 매번 가져와야 함
    df_toss = load_toss_data_from_supabase()
    if df_toss is None:
        return

    # 2. ETF PDF 로드 (캐싱 로직 적용)
    if df_pdf is not None:
        # 외부에서 명시적으로 넘겨준 경우 사용
        pdf_to_use = df_pdf
    elif _cached_pdf is not None and not force_load:
        # 메모리에 캐시가 있고 강제 로드가 아니면 캐시 사용
        pdf_to_use = _cached_pdf
    else:
        # 캐시가 없거나 강제 로드인 경우 Supabase에서 가져옴
        print("🔄 [Cache] Loading ETF PDF from Supabase...")
        _cached_pdf = load_etf_pdf_from_supabase()
        pdf_to_use = _cached_pdf

    if pdf_to_use is None:
        print("⚠️ [Error] ETF PDF 데이터를 불러올 수 없습니다.")
        return

    # 3. 데이터 피벗 (투자자별 컬럼 분리)
    # df_toss: 투자자, 종목명, 종목코드, 금액
    df_pivot = df_toss.pivot_table(index='종목코드', columns='투자자', values='금액', aggfunc='sum').fillna(0).reset_index()

    # 컬럼 매핑 (Supabase 저장 값 -> 로직 내부 변수명)
    col_mapping = {
        'Foreigner': '외국인_순매수',
        'Institution': '기관_순매수',
        '외국인': '외국인_순매수',
        '기관': '기관_순매수'
    }
    df_pivot.rename(columns=col_mapping, inplace=True)

    # 필수 컬럼 확인 및 생성
    if '외국인_순매수' not in df_pivot.columns: df_pivot['외국인_순매수'] = 0
    if '기관_순매수' not in df_pivot.columns: df_pivot['기관_순매수'] = 0

    # 4. ETF 구성종목과 수급 데이터 병합
    df_merged = pd.merge(pdf_to_use, df_pivot, left_on='구성종목코드', right_on='종목코드', how='left')

    df_merged['외국인_순매수'] = df_merged['외국인_순매수'].fillna(0)
    df_merged['기관_순매수'] = df_merged['기관_순매수'].fillna(0)

    # 5. YG Score 계산
    df_merged['YG_SCORE_외국인'] = df_merged['구성비중(%)'] * (df_merged['외국인_순매수'] * 100) / 100
    df_merged['YG_SCORE_기관'] = df_merged['구성비중(%)'] * (df_merged['기관_순매수'] * 100) / 100
    df_merged['YG_SCORE_합계'] = df_merged['YG_SCORE_외국인'] + df_merged['YG_SCORE_기관']

    # 6. ETF별 점수 집계
    result = df_merged.groupby(['ETF종목코드', 'ETF종목명'], as_index=False).agg({
        'YG_SCORE_외국인': 'sum',
        'YG_SCORE_기관': 'sum',
        'YG_SCORE_합계': 'sum',
        '구성종목명': 'count'
    })
    result.rename(columns={'구성종목명': '종목수'}, inplace=True)

    # 소수점 정리 -> 정수형 변환
    for col in ['YG_SCORE_외국인', 'YG_SCORE_기관', 'YG_SCORE_합계']:
        result[col] = result[col].fillna(0).astype(int)

    # 정렬: YG_SCORE_합계 내림차순
    result.sort_values(by='YG_SCORE_합계', ascending=False, inplace=True)

    # 7. 결과 출력
    cols = ['ETF종목코드', 'ETF종목명', 'YG_SCORE_합계', 'YG_SCORE_외국인', 'YG_SCORE_기관', '종목수']

    print("\n" + "="*80)
    print(f"📊 [실시간/Supabase] 수급 기반 ETF YG Score (Top 20)")
    print("="*80)
    print(result[cols].head(20).to_string(index=False))

    print("\n" + "="*80)
    print(f"📊 [실시간/Supabase] 외국인/기관 동시 순매수 ETF (Top 20)")
    print("="*80)
    cond_both = (result['YG_SCORE_외국인'] > 0) & (result['YG_SCORE_기관'] > 0)
    print(result[cond_both][cols].head(20).to_string(index=False))

    # 8. Supabase 저장
    save_score_to_supabase(result)

    return result

if __name__ == "__main__":
    # 단독 실행 시에는 오늘 이전 데이터를 정리
    print("🧹 [Standalone] Cleaning up old scores...")
    delete_old_scores()
    calculate_yg_score()
