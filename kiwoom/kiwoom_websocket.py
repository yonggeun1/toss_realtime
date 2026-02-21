# 1. 필요한 라이브러리 가져오기
import asyncio     # 1-2. 비동기 작업을 쉽게 처리할 수 있게 해줍니다.
import websockets  # 1-2. 웹소켓을 사용할 수 있도록 도와주는 도구입니다.
import json        # 1-2. 데이터를 JSON 형식(문자열로 변환)으로 다룰 수 있도록 합니다.
import os
import sys
import re
from datetime import datetime

# 프로젝트 내 공통 로그인 및 DB 모듈에서 설정을 가져옵니다.
from kiwoom_login_common import get_token, DEFAULT_SOCKET_URL

# 프로젝트 루트 디렉토리를 path에 추가하여 toss_crawling 모듈을 불러올 수 있게 합니다.
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.append(project_root)

from toss_crawling.supabase_client import supabase

def clean_value(val):
    """키움 API 특유의 +, - 기호 및 콤마 제거 후 숫자로 변환. 마이너스 기호(-)는 음수 처리를 위해 반드시 보존함."""
    if val is None or val == "":
        return 0
    try:
        val_str = str(val).replace(',', '').strip()
        # 숫자가 아닌 문자 제거하되, 마이너스 기호(-)와 소수점(.)은 유지
        # regex [^\d.\-] 는 숫자, 점, 마이너스가 아닌 모든 문자를 제거함 -> 마이너스는 살아남음
        cleaned = re.sub(r'[^\d.\-]', '', val_str)
        if not cleaned or cleaned == '-':
            return 0
        return float(cleaned)
    except:
        return 0

# 2. 웹소켓 서버 정보 설정
# SOCKET_URL: 접속할 주소 (모의투자용 DEFAULT_SOCKET_URL 사용)
SOCKET_URL = DEFAULT_SOCKET_URL 

# 3. 실시간 데이터 필드 매핑 (0B: 주식체결)
REALTIME_FIELD_MAP = {
    '0B': {
        '20': '체결시간', '10': '현재가', '11': '전일대비', '12': '등락율',
        '27': '매도호가', '28': '매수호가', '15': '거래량', '13': '누적거래량',
        '14': '누적거래대금', '16': '시가', '17': '고가', '18': '저가',
        '25': '전일대비기호', '26': '전일거래량대비', '29': '거래대금증감',
        '30': '전일거래량대비비율', '31': '거래회전율', '32': '거래비용',
        '228': '체결강도', '311': '시가총액(억)', '290': '장구분',
        '1313': '순간거래대금', '1314': '순매수체결량', '9081': '거래소구분'
    }
}

# 4. 웹소켓 클라이언트 만들기
class WebSocketClient:
    def __init__(self, uri, token):
        self.uri = uri                # self.uri: 연결할 서버의 주소
        self.token = token            # 인증을 확인하는 키 (ACCESS_TOKEN)
        self.websocket = None         # self.websocket: 실제 웹소켓 연결을 관리하는 변수
        self.connected = False        # self.connected: 연결 상태 (True면 연결됨, False면 끊김)
        self.keep_running = True      # 루프 유지 여부
        self.current_batch_data = {}  # 현재 배치의 수집 데이터 저장

    # 5. 서버에 연결하기
    async def connect(self):
        try:
            # websockets.connect(self.uri): 웹소켓 서버에 연결을 시도합니다.
            self.websocket = await websockets.connect(self.uri)
            self.connected = True
            print("서버와 연결을 시도 중입니다.")

            # 로그인 패킷: 인증을 위해 LOGIN 서비스와 토큰을 보냅니다.
            param = {
                'trnm': 'LOGIN',
                'token': self.token
            }

            print('실시간 시세 서버로 로그인 패킷을 전송합니다.')
            # 웹소켓 연결 시 로그인 정보 전달
            await self.send_message(message=param)

        except Exception as e:
            print(f'Connection error: {e}')
            self.connected = False

    # 6. 메시지 보내기
    async def send_message(self, message):
        if not self.connected:
            await self.connect()  # 연결이 끊어졌다면 재연결
            
        if self.connected:
            # message가 문자열이 아니면 JSON으로 직렬화 (전송을 위해 문자열 변환)
            if not isinstance(message, str):
                message = json.dumps(message)

            # 서버에 데이터를 전송합니다.
            await self.websocket.send(message)
            # print(f'Message sent: {message}')

    # 7. 서버에서 메시지 받기
    async def receive_messages(self):
        while self.keep_running:
            try:
                # 서버로부터 수신한 메시지를 JSON 형식으로 파싱
                response = json.loads(await self.websocket.recv())
                trnm = response.get('trnm')

                # 1. 로그인 처리
                if trnm == 'LOGIN':
                    if response.get('return_code') != 0:
                        print(f'로그인 실패: {response.get("return_msg")}')
                        await self.disconnect()
                    else:
                        print('로그인 성공하였습니다.')

                # 2. 실시간 등록/해지 결과 처리
                elif trnm in ['REG', 'REMOVE']:
                    if response.get('return_code') == 0:
                        # print(f'실시간 {trnm} 요청 성공')
                        pass
                    else:
                        print(f'실시간 {trnm} 요청 실패: {response.get("return_msg")}')

                # 3. 실시간 데이터 수신 (REAL)
                elif trnm == 'REAL':
                    for entry in response.get('data', []):
                        tr_type = entry.get('type')
                        item_code = entry.get('item')
                        values = entry.get('values', {})
                        
                        if tr_type == '0B':  # 주식체결
                            # 데이터 파싱 및 저장 (clean_value 사용하여 부호 및 기호 처리)
                            self.current_batch_data[item_code] = {
                                "stk_cd": item_code,
                                "close_pric": abs(clean_value(values.get('10', 0))),
                                "pre": clean_value(values.get('11', 0)),
                                "flu_rt": clean_value(values.get('12', 0)),
                                "open_pric": abs(clean_value(values.get('16', 0))),
                                "high_pric": abs(clean_value(values.get('17', 0))),
                                "low_pric": abs(clean_value(values.get('18', 0))),
                                "trde_qty": int(abs(clean_value(values.get('13', 0)))),
                                "trde_prica": int(abs(clean_value(values.get('14', 0)))),
                                "cntr_str": clean_value(values.get('228', 0)),
                                "date": values.get('20', ''),
                                "collected_at": datetime.now().astimezone().isoformat()
                            }

                # 4. PING 처리 (세션 유지)
                elif trnm == 'PING':
                    await self.send_message(response)

            except websockets.ConnectionClosed:
                print('Connection closed by the server (서버에 의해 연결이 종료되었습니다.)')
                self.connected = False
                await self.websocket.close()
                break
            except Exception as e:
                print(f"Error in receive_messages: {e}")
                await asyncio.sleep(1)

    # 8. 웹소켓 실행하기
    async def run(self):
        # 서버에 연결하고, 메시지를 계속 받을 준비를 합니다.
        await self.connect()
        await self.receive_messages()

    # 8. 실시간 서비스 등록/해지 (규격 최적화)
    async def request_realtime(self, trnm, items, types, grp_no='1', refresh='1'):
        """
        trnm: REG(등록), REMOVE(해지)
        items: 종목코드 리스트 ['039490']
        types: 실시간 타입 리스트 ['0B']
        grp_no: 그룹번호
        """
        param = {
            'trnm': trnm,
            'grp_no': grp_no,
            'refresh': refresh,
            'data': [{
                'item': items,  # 공백 제거
                'type': types   # 공백 제거
            }]
        }
        await self.send_message(param)

    # 9. 웹소켓 종료하기
    async def disconnect(self):
        self.keep_running = False
        if self.connected and self.websocket:
            await self.websocket.close()
            self.connected = False
            print('Disconnected from WebSocket server (웹소켓 연결 종료)')

# 10. 프로그램 실행하기
async def main():
    # 10-1. Supabase에서 종목 코드 가져오기
    print("DB에서 종목 리스트를 조회 중입니다...")
    try:
        response = supabase.table('holding_name_websocket').select('holding_code, holding_name').execute()
        holdings = response.data
        code_to_name = {h['holding_code']: h['holding_name'] for h in holdings}
        all_codes = list(code_to_name.keys())
        print(f"총 {len(all_codes)}개의 종목을 조회했습니다.")
    except Exception as e:
        print(f"DB 조회 중 오류 발생: {e}")
        return

    # 액세스 토큰 가져오기
    access_token = get_token()
    
    # WebSocketClient 객체 생성
    websocket_client = WebSocketClient(SOCKET_URL, access_token)

    # 웹소켓 클라이언트를 백그라운드 태스크로 실행
    receive_task = asyncio.create_task(websocket_client.run())

    # 로그인 처리 대기
    await asyncio.sleep(3)

    if not websocket_client.connected:
        print("서버 연결에 실패했습니다.")
        return

    print("🚀 실시간 배치 수집 시작")

    try:
        while True:
            now = datetime.now()
            # 장 마감 후 종료 (15:30)
            if now.hour > 15 or (now.hour == 15 and now.minute > 30):
                print("🏁 장 마감 시간이 되어 프로그램을 종료합니다.")
                break
            
            # 장 시작 전 대기 (08:55 이전)
            current_time_str = now.strftime("%H%M")
            if current_time_str < "0855":
                print(f"💤 장 시작 전입니다. 대기 중... ({now.strftime('%H:%M:%S')})", end="\r")
                await asyncio.sleep(30)
                continue

            # 100개씩 배치 처리
            for i in range(0, len(all_codes), 100):
                batch_codes = all_codes[i:i+100]
                print(f"\n📦 배치 {i//100 + 1} 처리 중... ({len(batch_codes)} 종목)")

                # 데이터 초기화 및 등록
                websocket_client.current_batch_data = {}
                await websocket_client.request_realtime(
                    trnm='REG',
                    items=batch_codes,
                    types=['0B'],
                    grp_no='1',
                    refresh='0' # 이전 등록 초기화
                )

                # 데이터 응답 대기 (최대 10초)
                await asyncio.sleep(10)

                # 수집된 데이터 Supabase로 전송
                collected_records = []
                for code in batch_codes:
                    if code in websocket_client.current_batch_data:
                        record = websocket_client.current_batch_data[code]
                        record['stk_nm'] = code_to_name.get(code, '')
                        collected_records.append(record)

                if collected_records:
                    try:
                        supabase.table('kiwoom_websocket_stk').insert(collected_records).execute()
                        supabase.rpc('calculate_kiwoom_websocket_etf_score', {}).execute()
                        print(f"✅ {len(collected_records)}개 종목 저장 및 ETF 스코어 계산 완료")
                    except Exception as e:
                        print(f"❌ DB 저장 중 오류 발생: {e}")

                # 실시간 해지
                await websocket_client.request_realtime(
                    trnm='REMOVE',
                    items=batch_codes,
                    types=['0B'],
                    grp_no='1'
                )
                
                # 다음 배치를 위한 짧은 대기
                await asyncio.sleep(1)

            print("\n🔄 한 사이클 완료. 10초 후 재시작합니다.")
            await asyncio.sleep(10)

    except KeyboardInterrupt:
        print("\n사용자에 의해 종료되었습니다.")
    finally:
        await websocket_client.disconnect()
        receive_task.cancel()

# asyncio.run(main())을 사용하여 프로그램을 실행합니다.
if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

