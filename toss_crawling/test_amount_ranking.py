import sys
import os
import traceback
from datetime import datetime, timedelta

# 상위 디렉토리를 path에 추가하여 임포트 가능하게 설정
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from toss_premarket_top100 import get_toss_amount_ranking
    import traceback
    
    print("Starting test for get_toss_amount_ranking...")
    
    kst_now = datetime.utcnow() + timedelta(hours=9)
    test_timestamp = kst_now.isoformat()
    
    # 디버깅을 위해 함수 내부의 동작을 확인하기 위해 직접 호출하거나 수정된 함수를 사용해야 하지만,
    # 여기서는 기존 함수를 호출하되 출력을 더 자세히 확인합니다.
    get_toss_amount_ranking(collected_at=test_timestamp)
    
    print("Test completed successfully.")

except Exception as e:
    print("Error during test execution:")
    traceback.print_exc()
