import psycopg2 as db
import requests
from datetime import datetime, timedelta
import json
import kis_api_resp as resp
import pandas as pd
import streamlit as st
import altair as alt
import plotly.graph_objects as go
from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import JsCode
import sys
import websockets
import math
import asyncio
from dateutil.relativedelta import relativedelta

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스
KW_URL_BASE = "https://api.kiwoom.com"    
SOCKET_URL = "wss://api.kiwoom.com:10000/api/dostk/websocket"  # 접속 URL

# PostgreSQL 연결 설정
# conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
# DB 연결
conn = db.connect(conn_string)

# 인증처리
def auth(APP_KEY, APP_SECRET):

    # 인증처리
    headers = {"content-type":"application/json"}
    body = {"grant_type":"client_credentials",
            "appkey":APP_KEY,
            "appsecret":APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body), verify=False)
    ACCESS_TOKEN = res.json()["access_token"]

    return ACCESS_TOKEN

# 계정정보 조회
def account(nickname):

    cur01 = conn.cursor()
    cur01.execute("select acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day from \"stockAccount_stock_account\" where nick_name = '" + nickname + "'")
    result_two = cur01.fetchone()
    cur01.close()

    acct_no = result_two[0]
    access_token = result_two[1]
    app_key = result_two[2]
    app_secret = result_two[3]
    today = datetime.now().strftime("%Y%m%d")

    YmdHMS = datetime.now()
    validTokenDate = datetime.strptime(result_two[4], '%Y%m%d%H%M%S')
    diff = YmdHMS - validTokenDate
    # print("diff : " + str(diff.days))
    if diff.days >= 1 or result_two[5] != today:  # 토큰 유효기간(1일) 만료 재발급
        access_token = auth(app_key, app_secret)
        token_publ_date = datetime.now().strftime("%Y%m%d%H%M%S")
        print("new access_token1 : " + access_token)
        # 계정정보 토큰값 변경
        cur02 = conn.cursor()
        update_query = "update \"stockAccount_stock_account\" set access_token = %s, token_publ_date = %s, last_chg_date = %s where acct_no = %s"
        # update 인자값 설정
        record_to_update = ([access_token, token_publ_date, datetime.now(), acct_no])
        # DB 연결된 커서의 쿼리 수행
        cur02.execute(update_query, record_to_update)
        conn.commit()
        cur02.close()

    account_rtn = {'acct_no':acct_no, 'access_token':access_token, 'app_key':app_key, 'app_secret':app_secret}

    return account_rtn

# 계좌잔고 조회
def stock_balance(access_token, app_key, app_secret, acct_no):
    
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8434R"} 
    params = {
                "CANO": acct_no,                # 종합계좌번호 계좌번호 체계(8-2)의 앞 8자리
                'ACNT_PRDT_CD': '01',           # 계좌상품코드 계좌번호 체계(8-2)의 뒤 2자리
                'AFHR_FLPR_YN': 'N',            # 시간외단일가, 거래소여부 N : 기본값, Y : 시간외단일가, X : NXT 정규장 (프리마켓, 메인, 애프터마켓)
                'OFL_YN': '',                   # 오프라인여부 공란(Default)
                'INQR_DVSN': '02',              # 조회구분 01 : 대출일별, 02 : 종목별
                'UNPR_DVSN': '01',              # 단가구분 01 : 기본값 
                'FUND_STTL_ICLD_YN': 'N',       # 펀드결제분포함여부 N : 포함하지 않음, Y : 포함
                'FNCG_AMT_AUTO_RDPT_YN': 'N',   # 융자금액자동상환여부 N : 기본값
                'PRCS_DVSN': '01',              # 처리구분 00 : 전일매매포함, 01 : 전일매매미포함
                'CTX_AREA_FK100': '',
                'CTX_AREA_NK100': ''
            }
    PATH = "uapi/domestic-stock/v1/trading/inquire-balance"
    URL = f"{URL_BASE}/{PATH}"
    
    try:
        res = requests.get(URL, headers=headers, params=params, verify=False)
        ar = resp.APIResp(res)
        
        body = ar.getBody()

        output1 = body.output1 if hasattr(body, 'output1') else []
        output2 = body.output2 if hasattr(body, 'output2') else {}

        return output1, output2
    
    except Exception as e:
        print("계좌잔고조회 중 오류 발생:", e)
        return []

# 기간별매매손익현황조회
def inquire_period_trade_profit(access_token, app_key, app_secret, code, strt_dt, end_dt):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8715R",
               "custtype": "P"}
    params = {
            'CANO': acct_no,            # 종합계좌번호
            'SORT_DVSN': "01",          # 00: 최근 순, 01: 과거 순, 02: 최근 순
            'ACNT_PRDT_CD': "01",
            'CBLC_DVSN': "00",
            'PDNO': code,               # ""공란입력 시, 전체
            'INQR_STRT_DT': strt_dt,    # 조회시작일(8자리) 
            'INQR_END_DT': end_dt,      # 조회종료일(8자리)
            'CTX_AREA_NK100': "",
            'CTX_AREA_FK100': "" 
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-period-trade-profit"
    URL = f"{URL_BASE}/{PATH}"

    try:
        res = requests.get(URL, headers=headers, params=params, verify=False)
        ar = resp.APIResp(res)

        # 응답에 output1이 있는지 확인
        body = ar.getBody()
        if hasattr(body, 'output1'):
            return body.output1
        else:
            print("기간별매매손익현황조회 응답이 없습니다.")
            return []  # 혹은 None

    except Exception as e:
        print("기간별매매손익현황조회 중 오류 발생:", e)
        return []

# 기간별매매손익현황 합산조회
def inquire_period_trade_profit_sum(access_token, app_key, app_secret, strt_dt, end_dt):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8715R",
               "custtype": "P"}
    params = {
            'CANO': acct_no,            # 종합계좌번호
            'SORT_DVSN': "01",          # 00: 최근 순, 01: 과거 순, 02: 최근 순
            'ACNT_PRDT_CD': "01",
            'CBLC_DVSN': "00",
            'PDNO': "",                 # ""공란입력 시, 전체
            'INQR_STRT_DT': strt_dt,    # 조회시작일(8자리) 
            'INQR_END_DT': end_dt,      # 조회종료일(8자리)
            'CTX_AREA_NK100': "",
            'CTX_AREA_FK100': "" 
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-period-trade-profit"
    URL = f"{URL_BASE}/{PATH}"

    try:
        res = requests.get(URL, headers=headers, params=params, verify=False)
        ar = resp.APIResp(res)

        # 응답에 output2이 있는지 확인
        body = ar.getBody()
        if hasattr(body, 'output2'):
            return body.output2
        else:
            print("기간별매매손익현황 합산조회 응답이 없습니다.")
            return []  # 혹은 None

    except Exception as e:
        print("기간별매매손익현황 합산조회 중 오류 발생:", e)
        return []

# 기간별손익일별합산조회
def inquire_period_profit(access_token, app_key, app_secret, code, strt_dt, end_dt):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8708R",
               "custtype": "P"}
    params = {
            'CANO': acct_no,            # 종합계좌번호
            'SORT_DVSN': "01",          # 00: 최근 순, 01: 과거 순, 02: 최근 순
            'INQR_DVSN': "00",
            'ACNT_PRDT_CD':"01",
            'CBLC_DVSN': "00",
            'PDNO': code,               # ""공란입력 시, 전체
            'INQR_STRT_DT': strt_dt,    # 조회시작일(8자리) 
            'INQR_END_DT': end_dt,      # 조회종료일(8자리)
            'CTX_AREA_NK100': "",
            'CTX_AREA_FK100': "" 
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-period-profit"
    URL = f"{URL_BASE}/{PATH}"

    try:
        res = requests.get(URL, headers=headers, params=params, verify=False)
        ar = resp.APIResp(res)

        # 응답에 output1이 있는지 확인
        body = ar.getBody()
        if hasattr(body, 'output1'):
            return body.output1
        else:
            print("기간별손익일별합산조회 응답이 없습니다.")
            return []  # 혹은 None

    except Exception as e:
        print("기간별손익일별합산조회 중 오류 발생:", e)
        return []
    
# 일별주문체결조회
def get_my_complete(access_token, app_key, app_secret, acct_no, strt_dt, end_dt):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC0081R",
               "custtype": "P"}
    params = {
            'CANO': acct_no,            # 종합계좌번호 계좌번호 체계(8-2)의 앞 8자리
            'ACNT_PRDT_CD':"01",        # 계좌상품코드 계좌번호 체계(8-2)의 뒤 2자리
            'SORT_DVSN': "01",          # 00: 최근 순, 01: 과거 순, 02: 최근 순
            'INQR_STRT_DT': strt_dt,    # 조회시작일(8자리) 
            'INQR_END_DT': end_dt,      # 조회종료일(8자리)
            'SLL_BUY_DVSN_CD': "00",    # 매도매수구분코드 00 : 전체 / 01 : 매도 / 02 : 매수
            'PDNO': "",                 # 종목번호(6자리) ""공란입력 시, 전체
            'ORD_GNO_BRNO': "",         # 주문채번지점번호 ""공란입력 시, 전체
            'ODNO': "",                 # 주문번호 ""공란입력 시, 전체
            'CCLD_DVSN': "00",          # 체결구분 00 전체, 01 체결, 02 미체결
            'INQR_DVSN': "00",          # 조회구분 00 역순, 01 정순
            'INQR_DVSN_1': "",          # 조회구분1 없음: 전체, 1: ELW, 2: 프리보드
            'INQR_DVSN_3': "00",        # 조회구분3 00 전체, 01 현금, 02 신용, 03 담보, 04 대주, 05 대여, 06 자기융자신규/상환, 07 유통융자신규/상환
            'EXCG_ID_DVSN_CD': "ALL",   # 거래소ID구분코드 KRX : KRX, NXT : NXT, SOR (Smart Order Routing) : SOR, ALL : 전체
            'CTX_AREA_NK100': "",
            'CTX_AREA_FK100': "" 
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-daily-ccld"
    URL = f"{URL_BASE}/{PATH}"

    try:
        res = requests.get(URL, headers=headers, params=params, verify=False)
        ar = resp.APIResp(res)

        # 응답에 output1이 있는지 확인
        body = ar.getBody()
        if hasattr(body, 'output1'):
            return body.output1
        else:
            print("일별주문체결조회 응답이 없습니다.")
            return []  # 혹은 None

    except Exception as e:
        print("일별주문체결조회 중 오류 발생:", e)
        return []
    
# 주식예약주문조회 : 15시 40분 ~ 다음 영업일 07시 30분까지 가능(23시 40분 ~ 0시 10분까지 서버초기화 작업시간 불가)
def order_reserve_complete(access_token, app_key, app_secret, reserce_strt_dt, reserve_end_dt, acct_no, code):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "CTSC0004R",                                # tr_id : CTSC0004R
    }  
    params = {
                "RSVN_ORD_ORD_DT": reserce_strt_dt,                 # 예약주문시작일자
                "RSVN_ORD_END_DT": reserve_end_dt,                  # 예약주문종료일자
                "RSVN_ORD_SEQ": "",                                 # 예약주문순번
                "TMNL_MDIA_KIND_CD": "00",
                "CANO": acct_no,
                "ACNT_PRDT_CD": '01',
                "PRCS_DVSN_CD": "0",                                # 처리구분코드 : 전체 0, 처리내역 1, 미처리내역 2
                "CNCL_YN": "Y",                                     # 취소여부 : 'Y'
                "PDNO": code if code != "" else "",                 # 종목코드 : 공백 입력 시 전체 조회
                "SLL_BUY_DVSN_CD": "",                              # 매도매수구분코드 : 01 매도, 02 매수        
                "CTX_AREA_FK200": "",                               
                "CTX_AREA_NK200": "",                               
    }
    PATH = "uapi/domestic-stock/v1/trading/order-resv-ccnl"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)
    #ar.printAll()
    return ar.getBody().output    

nickname = ['phills2', 'chichipa', 'phills75', 'yh480825', 'phills13', 'phills15', 'mamalong', 'honeylong']
# nickname = ['yh480825']
my_choice = st.selectbox('닉네임을 선택하세요', nickname)   

ac = account(my_choice)
acct_no = ac['acct_no']
access_token = ac['access_token']
app_key = ac['app_key']
app_secret = ac['app_secret']

# 계좌잔고 조회
result0 = stock_balance(access_token, app_key, app_secret, acct_no)

if not result0:
    print("계좌잔고 조회 결과가 없습니다.")
else:
    output1, output2 = result0
    data0 = []
    
    # output1: 종목별 잔고
    if output1:
        for item in output1:

            data0.append({
                '종목명': item['prdt_name'],
                '매입단가': float(item['pchs_avg_pric']),
                '매입수량': float(item['hldg_qty']),
                '매입금액': float(item['pchs_amt']),
                '현재가': float(item['prpr']),
                '평가금액': float(item['evlu_amt']),
                '손익률(%)': float(item['evlu_pfls_rt']),
                '손익금액': float(item['evlu_pfls_amt']),
            })

    # output2: 예수금 정보 → '현금' 항목으로 추가
    if output2[0] and 'prvs_rcdl_excc_amt' in output2[0]:
        data0.append({
            '종목명': '현금',
            '매입단가': 0,
            '매입수량': 0,
            '매입금액': 0,
            '현재가': 0,
            '평가금액': float(output2[0]['prvs_rcdl_excc_amt']),
            '손익률(%)': 0,
            '손익금액': 0,
        })       

    df0 = pd.DataFrame(data0)

    if df0.empty:
        st.warning("조회된 데이터가 없습니다. 조건을 확인해주세요.")
    else:
        # Streamlit 앱 구성
        st.title("잔고정보 조회")
        
        total_amt = df0['평가금액'].sum()
        cash_amt = df0[df0['종목명'] == '현금']['평가금액'].sum()
        df_filtered = df0[~df0['종목명'].isin(['현금'])]
        total_hold_amt = df_filtered['매입금액'].sum()
        total_eval_amt = df_filtered['평가금액'].sum()
        total_profit_amt = df_filtered['손익금액'].sum()
        profit_rate = (total_profit_amt / total_hold_amt * 100) if total_hold_amt != 0 else 0.0
        
        st.subheader("📊 총 집계 정보")
        col1, col2 = st.columns(2)
        col1.metric("총 금액", f"{total_amt:,.0f}원")
        col2.metric("현금", f"{cash_amt:,.0f}원")
        col3, col4, col5 = st.columns(3)
        col3.metric("총 매입금액", f"{total_hold_amt:,.0f}원")
        col4.metric("총 평가금액", f"{total_eval_amt:,.0f}원")
        col5.metric("총 손익금액", f"{total_profit_amt:,.0f}원", delta=f"{profit_rate:+.2f}%")

        # 전체 평가금액 기준 비중 계산
        df0['비중(%)'] = df0['평가금액'] / df0['평가금액'].sum() * 100

        # 비중 순으로 정렬
        df0.sort_values(by='비중(%)', ascending=False, inplace=True)

        # 순서 컬럼 추가 (1부터 시작)
        df0.insert(0, '순서', range(1, len(df0) + 1))

        df_display = df0.copy().reset_index(drop=True)

        # Grid 옵션 생성
        gb = GridOptionsBuilder.from_dataframe(df_display)
        # 페이지당 20개 표시
        gb.configure_pagination(enabled=True, paginationPageSize=20)
        gb.configure_grid_options(domLayout='normal')
        # Excel 다운로드를 위한 옵션 추가
        gb.configure_grid_options(enableRangeSelection=True)
        gb.configure_grid_options(enableExcelExport=True)

        # JS 코드: 첫 렌더링 시 모든 컬럼 자동 크기 맞춤 (컬럼명 포함)
        auto_size_js = JsCode("""
        function onFirstDataRendered(params) {
            const allColumnIds = [];
            params.columnApi.getAllColumns().forEach(function(column) {
                allColumnIds.push(column.getId());
            });
            params.columnApi.autoSizeColumns(allColumnIds, false);
        }
        """)
        gb.configure_grid_options(onFirstDataRendered=auto_size_js)
        
        column_widths = {
            '순서': 40,
            '종목명': 140,
            '매입단가': 80,
            '매입수량': 70,
            '매입금액': 100,
            '현재가': 80,
            '평가금액': 100,
            '손익률(%)': 70,
            '손익금액': 100,
            '비중(%)': 70
        }

        # 숫자 포맷을 JS 코드로 적용 (정렬 문제 방지)
        number_format_js = JsCode("""
            function(params) {
                if (params.value === null || params.value === undefined) {
                    return '';
                }
                return params.value.toLocaleString();
            }
        """)

        percent_format_js = JsCode("""
            function(params) {
                if (params.value === null || params.value === undefined) {
                    return '';
                }
                return params.value.toFixed(2) + '%';
            }
        """)

        for col, width in column_widths.items():
            if col in ['손익률(%)', '비중(%)']:
                gb.configure_column(col, type=['numericColumn'], cellRenderer=percent_format_js, width=width)
            elif col in ['매입단가', '매입수량', '매입금액', '현재가', '평가금액', '손익금액']:
                gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js, width=width)
            else:
                gb.configure_column(col, width=width)
        
        grid_options = gb.build()

        # AgGrid를 통해 데이터 출력
        AgGrid(
            df_display,
            gridOptions=grid_options,
            fit_columns_on_grid_load=False,  # 화면 로드시 자동 폭 맞춤
            allow_unsafe_jscode=True,
            use_container_width=True,
            update_mode=GridUpdateMode.NO_UPDATE,
            enable_enterprise_modules=True,  # 엑셀 다운로드 위해 필요
            excel_export_mode='xlsx'         # 엑셀(xlsx)로 다운로드
        )

        df_pie = df0[df0['평가금액'] > 0].copy()

        # 레이블 생성: 종목명 (매입단가) or 종목명 (평가금액)
        def format_label(row):
            if row['종목명'] == '현금':
                return f"{row['비중(%)']:.1f}% {row['종목명']} ({row['평가금액']:,.0f}원)"
            else:
                profit_rate = f"{row['손익률(%)']:+.2f}%"
                return f"{row['비중(%)']:.1f}% {row['종목명']} (매입가 {row['매입단가']:,.0f}원, 손익률 {profit_rate})"

        df_pie['종목명'] = df_pie.apply(format_label, axis=1)
        df_pie['custom_평가금액'] = df_pie['평가금액'].apply(lambda x: f"{x:,.0f}원")

        df_pie.sort_values(by='비중(%)', ascending=False, inplace=True)

        # 도넛 차트 생성
        fig = go.Figure(
            data=[go.Pie(
                labels=df_pie['종목명'],
                values=df_pie['평가금액'],
                hole=0.4,
                customdata=df_pie[['custom_평가금액']],
                hovertemplate='<b>%{label}</b><br><span style="color:red">평가금액: %{customdata[0]}</span><extra></extra>'
            )]
        )

        fig.update_layout(title='종목별 평가금액 비율')

        # Streamlit에 출력
        st.plotly_chart(fig)

code = ""
# selected_date = st.slider(
#     "날짜 범위 선택",
#     min_value=datetime.today() - timedelta(days=365),
#     max_value=datetime.today(),
#     value=(datetime.today() - timedelta(days=30), datetime.today()),
#     step=timedelta(days=1),
# )

# strt_dt = selected_date[0].strftime("%Y%m%d")
# end_dt = selected_date[1].strftime("%Y%m%d")

strt_dt = (st.date_input("시작일", datetime.today() - timedelta(days=30))).strftime("%Y%m%d")
end_dt = (st.date_input("종료일", datetime.today())).strftime("%Y%m%d")

cur03 = conn.cursor()
cur03.execute("select prvs_excc_amt, pchs_amt, evlu_amt, evlu_pfls_amt, dt from \"dly_acct_balance\" where acct = '" + str(acct_no) + "' and dt between '" + strt_dt + "' and '" + end_dt + "'")
result_three = cur03.fetchall()
cur03.close() 

data01 = []
for item in result_three:

    전체금액 = float(item[0]) + float(item[2])  # 예수금 + 평가금액
    예수금 = float(item[0])

    data01.append({
        '일자': item[4],
        '전체금액': 전체금액,
        '총구매금액': float(item[1]),
        '평가금액': float(item[2]),
        '수익금액': float(item[3]),
        '예수금': 예수금,
        '예수금비율(%)': (예수금 / 전체금액 * 100) if 전체금액 > 0 else 0,
    })

df01 = pd.DataFrame(data01)

if df01.empty:
    st.warning("조회된 데이터가 없습니다. 조건을 확인해주세요.")
else:
    # Streamlit 앱 구성
    st.title("기간별 잔고현황 조회")

    df01['일자'] = pd.to_datetime(df01['일자']).dt.strftime('%Y-%m-%d')

    # 버튼을 클릭하면, 데이터프레임이 보이도록 만들기.
    if st.button('기간별 잔고현황 상세 데이터'):

        df_display = df01.sort_values(by='일자', ascending=False).copy().reset_index(drop=True)

        # Grid 옵션 생성
        gb = GridOptionsBuilder.from_dataframe(df_display)
        # 페이지당 20개 표시
        gb.configure_pagination(enabled=True, paginationPageSize=20)
        gb.configure_grid_options(domLayout='normal')
        # Excel 다운로드를 위한 옵션 추가
        gb.configure_grid_options(enableRangeSelection=True)
        gb.configure_grid_options(enableExcelExport=True)

        # JS 코드: 첫 렌더링 시 모든 컬럼 자동 크기 맞춤 (컬럼명 포함)
        auto_size_js = JsCode("""
        function onFirstDataRendered(params) {
            const allColumnIds = [];
            params.columnApi.getAllColumns().forEach(function(column) {
                allColumnIds.push(column.getId());
            });
            params.columnApi.autoSizeColumns(allColumnIds, false);
        }
        """)
        gb.configure_grid_options(onFirstDataRendered=auto_size_js)

        column_widths = {
            '일자': 80,
            '전체금액': 100,
            '총구매금액': 100,
            '평가금액': 100,
            '수익금액': 80,
            '예수금': 100,
            '예수금비율(%)': 70
        }

        # 숫자 포맷을 JS 코드로 적용 (정렬 문제 방지)
        number_format_js = JsCode("""
            function(params) {
                if (params.value === null || params.value === undefined) {
                    return '';
                }
                return params.value.toLocaleString();
            }
        """)

        percent_format_js = JsCode("""
            function(params) {
                if (params.value === null || params.value === undefined) {
                    return '';
                }
                return params.value.toFixed(2) + '%';
            }
        """)

        for col, width in column_widths.items():
            if col in ['예수금비율(%)',]:
                gb.configure_column(col, type=['numericColumn'], cellRenderer=percent_format_js, width=width)
            elif col in ['전체금액', '총구매금액', '평가금액', '수익금액', '예수금']:
                gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js, width=width)
            else:
                gb.configure_column(col, width=width)

        grid_options = gb.build()

        # AgGrid를 통해 데이터 출력
        AgGrid(
            df_display,
            gridOptions=grid_options,
            fit_columns_on_grid_load=False,   # 화면 로드시 자동 폭 맞춤
            allow_unsafe_jscode=True,
            use_container_width=True,
            update_mode=GridUpdateMode.NO_UPDATE,
            enable_enterprise_modules=True,  # 엑셀 다운로드 위해 필요
            excel_export_mode='xlsx'         # 엑셀(xlsx)로 다운로드
        )

    df01['일자'] = pd.to_datetime(df01['일자'])
    df01 = df01.dropna(subset=['일자'])               
    df01 = df01.sort_values(by='일자')
    df01 = df01[df01['전체금액'] != 0]
    # 인덱스를 'YYYY-MM-DD' 문자열로 포맷
    df01['일자_str'] = df01['일자'].dt.strftime('%Y-%m-%d')
    df01.set_index('일자_str', inplace=True)                
    
    st.line_chart(df01[['전체금액']])       

# 기간별매매손익현황조회
result1 = inquire_period_trade_profit(access_token, app_key, app_secret, code, strt_dt, end_dt)   

if not result1:
    print("기간별매매손익현황조회 결과가 없습니다.")
else:

    data1 = []
    for item in result1:

        data1.append({
            '거래일자': item['trad_dt'],
            '종목명': item['prdt_name'],
            '매입단가': float(item['pchs_unpr']),
            '보유수량': float(item['hldg_qty']),
            '매도단가': float(item['sll_pric']),
            '매수수량': float(item['buy_qty']),
            '매도수량': float(item['sll_qty']),
            '손익률(%)': float(item['pfls_rt']),
            '손익금액': float(item['rlzt_pfls']),
            '거래세': float(item['tl_tax']),
            '수수료': float(item['fee']),
        })

    df1 = pd.DataFrame(data1)

    if df1.empty:
        st.warning("기간별매매손익현황조회된 데이터가 없습니다. 조건을 확인해주세요.")
    else:
        # Streamlit 앱 구성
        st.title("기간별 매매 손익현황 조회")

        df1['거래일자'] = pd.to_datetime(df1['거래일자']).dt.strftime('%Y-%m-%d')

        # 버튼을 클릭하면, 데이터프레임이 보이도록 만들기.
        if st.button('기간별 매매 손익현황 상세 데이터'):

            df_display = df1.sort_values(by='거래일자', ascending=False).copy().reset_index(drop=True)

            # Grid 옵션 생성
            gb = GridOptionsBuilder.from_dataframe(df_display)
            # 페이지당 20개 표시
            gb.configure_pagination(enabled=True, paginationPageSize=20)
            gb.configure_grid_options(domLayout='normal')
            # Excel 다운로드를 위한 옵션 추가
            gb.configure_grid_options(enableRangeSelection=True)
            gb.configure_grid_options(enableExcelExport=True)

            # JS 코드: 첫 렌더링 시 모든 컬럼 자동 크기 맞춤 (컬럼명 포함)
            auto_size_js = JsCode("""
            function onFirstDataRendered(params) {
                const allColumnIds = [];
                params.columnApi.getAllColumns().forEach(function(column) {
                    allColumnIds.push(column.getId());
                });
                params.columnApi.autoSizeColumns(allColumnIds, false);
            }
            """)
            gb.configure_grid_options(onFirstDataRendered=auto_size_js)

            column_widths = {
                '거래일자': 80,
                '종목명': 140,
                '매입단가': 80,
                '보유수량': 70,
                '매도단가': 80,
                '매수수량': 70,
                '매도수량': 70,
                '손익률(%)': 70,
                '손익금액': 100,
                '거래세': 60,
                '수수료': 60
            }

            # 숫자 포맷을 JS 코드로 적용 (정렬 문제 방지)
            number_format_js = JsCode("""
                function(params) {
                    if (params.value === null || params.value === undefined) {
                        return '';
                    }
                    return params.value.toLocaleString();
                }
            """)

            percent_format_js = JsCode("""
                function(params) {
                    if (params.value === null || params.value === undefined) {
                        return '';
                    }
                    return params.value.toFixed(2) + '%';
                }
            """)

            for col, width in column_widths.items():
                if col in ['손익률(%)',]:
                    gb.configure_column(col, type=['numericColumn'], cellRenderer=percent_format_js, width=width)
                elif col in ['매입단가', '보유수량', '매도단가', '매수수량', '매도수량', '손익금액', '거래세', '수수료']:
                    gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js, width=width)
                else:
                    gb.configure_column(col, width=width)

            grid_options = gb.build()

            # AgGrid를 통해 데이터 출력
            AgGrid(
                df_display,
                gridOptions=grid_options,
                fit_columns_on_grid_load=False,   # 화면 로드시 자동 폭 맞춤
                allow_unsafe_jscode=True,
                use_container_width=True,
                update_mode=GridUpdateMode.NO_UPDATE,
                enable_enterprise_modules=True,  # 엑셀 다운로드 위해 필요
                excel_export_mode='xlsx'         # 엑셀(xlsx)로 다운로드
            )

        df1['거래일자'] = pd.to_datetime(df1['거래일자'], errors='coerce')
        df1 = df1.dropna(subset=['거래일자'])
        df1 = df1.sort_values(by='거래일자')

        종목리스트 = df1['종목명'].unique()
        선택종목 = st.selectbox("종목을 선택하세요", 종목리스트)

        선택_df = df1[df1['종목명'] == 선택종목].copy()
        선택_df = 선택_df.sort_values(by='거래일자')
        선택_df['누적손익금액'] = 선택_df['손익금액'].cumsum()

        # 누적손익금액 - 왼쪽 Y축 (라인)
        profit_line = alt.Chart(선택_df).mark_line(color='red', strokeWidth=2).encode(
            x=alt.X('거래일자:T', title='거래일자', axis=alt.Axis(format='%Y-%m-%d')),
            y=alt.Y('누적손익금액:Q', title='누적손익금액', axis=alt.Axis(titleColor='red')),
            tooltip=[
                alt.Tooltip('거래일자:T', format='%Y-%m-%d'),
                alt.Tooltip('누적손익금액:Q', format=',')
            ]
        )

        # 보유수량 - 오른쪽 Y축 (바 차트 + 오른쪽 axis)
        qty_bar = alt.Chart(선택_df).mark_bar(color='gray', opacity=0.5).encode(
            x=alt.X('거래일자:T', axis=alt.Axis(format='%Y-%m-%d')),
            y=alt.Y('보유수량:Q', axis=alt.Axis(title='보유수량', titleColor='gray', orient='right')),
            tooltip=[
                alt.Tooltip('거래일자:T', format='%Y-%m-%d'),
                alt.Tooltip('보유수량:Q', format=',')
            ]
        )

        # 결합 차트
        combined_chart = alt.layer(
            profit_line,
            qty_bar
        ).resolve_scale(
            y='independent'
        ).properties(
            width=800,
            height=400,
            title=f"{선택종목} - 누적손익금액(좌) & 보유수량(우)"
        )

        st.altair_chart(combined_chart, use_container_width=True)

# 기간별손익일별합산조회
result2 = inquire_period_profit(access_token, app_key, app_secret, code, strt_dt, end_dt)    

if not result2:
    print("기간별손익일별합산조회 결과가 없습니다.")
else:

    data2 = []
    for item in result2:

        data2.append({
            '거래일자': item['trad_dt'],
            '매수금액': float(item['buy_amt']),
            '매도금액': float(item['sll_amt']),
            '손익률(%)': float(item['pfls_rt']),
            '손익금액': float(item['rlzt_pfls']),
            '거래세': float(item['tl_tax']),
            '수수료': float(item['fee']),
        })

    df2 = pd.DataFrame(data2)

    if df2.empty:
        st.warning("기간별손익일별합산조회된 데이터가 없습니다. 조건을 확인해주세요.")
    else:
        # Streamlit 앱 구성
        st.title("기간별 손익 일별합산 조회")

        df2['거래일자'] = pd.to_datetime(df2['거래일자']).dt.strftime('%Y-%m-%d')

        # 버튼을 클릭하면, 데이터프레임이 보이도록 만들기.
        if st.button('기간별 손익 일별합산 상세 데이터'):
            
            df_display = df2.sort_values(by='거래일자', ascending=False).copy().reset_index(drop=True)

            # Grid 옵션 생성
            gb = GridOptionsBuilder.from_dataframe(df_display)
            # 페이지당 20개 표시
            gb.configure_pagination(enabled=True, paginationPageSize=20)
            gb.configure_grid_options(domLayout='normal')
            # Excel 다운로드를 위한 옵션 추가
            gb.configure_grid_options(enableRangeSelection=True)
            gb.configure_grid_options(enableExcelExport=True)

            # JS 코드: 첫 렌더링 시 모든 컬럼 자동 크기 맞춤 (컬럼명 포함)
            auto_size_js = JsCode("""
            function onFirstDataRendered(params) {
                const allColumnIds = [];
                params.columnApi.getAllColumns().forEach(function(column) {
                    allColumnIds.push(column.getId());
                });
                params.columnApi.autoSizeColumns(allColumnIds, false);
            }
            """)
            gb.configure_grid_options(onFirstDataRendered=auto_size_js)

            column_widths = {
                '거래일자': 80,
                '매수금액': 100,
                '매도금액': 100,
                '손익률(%)': 70,
                '손익금액': 100,
                '거래세': 60,
                '수수료': 60
            }

            # 숫자 포맷을 JS 코드로 적용 (정렬 문제 방지)
            number_format_js = JsCode("""
                function(params) {
                    if (params.value === null || params.value === undefined) {
                        return '';
                    }
                    return params.value.toLocaleString();
                }
            """)

            percent_format_js = JsCode("""
                function(params) {
                    if (params.value === null || params.value === undefined) {
                        return '';
                    }
                    return params.value.toFixed(2) + '%';
                }
            """)

            for col, width in column_widths.items():
                if col in ['손익률(%)',]:
                    gb.configure_column(col, type=['numericColumn'], cellRenderer=percent_format_js, width=width)
                elif col in ['매수금액', '매도금액', '손익금액', '거래세', '수수료']:
                    gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js, width=width)
                else:
                    gb.configure_column(col, width=width)
            
            grid_options = gb.build()

            # AgGrid를 통해 데이터 출력
            AgGrid(
                df_display,
                gridOptions=grid_options,
                fit_columns_on_grid_load=False,   # 화면 로드시 자동 폭 맞춤
                allow_unsafe_jscode=True,
                use_container_width=True,
                update_mode=GridUpdateMode.NO_UPDATE,
                enable_enterprise_modules=True,  # 엑셀 다운로드 위해 필요
                excel_export_mode='xlsx'         # 엑셀(xlsx)로 다운로드
            )

        # 라디오버튼 선택
        # status = st.radio('정렬을 선택하세요', ['오름차순정렬', '내림차순정렬'])

        # if status == '오름차순정렬':
        # 	# df의 petal_length 컬럼을 기준으로 오름차순으로 정렬해서 보여주세요
        # 	st.dataframe(df.sort_values('petal_length',ascending=True))
        # elif status == '내림차순정렬':
        # 	st.dataframe(df.sort_values('petal_length',ascending=False))

        df2['거래일자'] = pd.to_datetime(df2['거래일자'])
        df2 = df2.dropna(subset=['거래일자'])
        df2 = df2.sort_values(by='거래일자')
        df2 = df2[df2['손익금액'] != 0]

        # 누적 손익금액 계산
        df2['누적손익금액'] = df2['손익금액'].cumsum()

        # Altair 바 차트 생성 - 누적손익금액 기준
        bar_chart = alt.Chart(df2).mark_bar().encode(
            x=alt.X('거래일자:T', title='거래일자', axis=alt.Axis(format='%Y-%m-%d')),
            y=alt.Y('누적손익금액:Q', axis=alt.Axis(title='누적 손익금액 (₩)')),
            color=alt.condition(
                alt.datum['누적손익금액'] > 0,
                alt.value('steelblue'),  # 이익
                alt.value('tomato')      # 손실
            ),
            tooltip=[
                alt.Tooltip('거래일자:T', format='%Y-%m-%d'),
                alt.Tooltip('누적손익금액:Q', format=',')
            ]
        ).properties(
            width=800,
            height=400,
            title='거래일자별 누적 손익금액 바 차트'
        )

        # Streamlit에 표시
        st.altair_chart(bar_chart, use_container_width=True)

# 기간별손익 합산조회
result3 = inquire_period_trade_profit_sum(access_token, app_key, app_secret, strt_dt, end_dt)        

if not result3:
    print("손익합산조회 결과가 없습니다.")
else:

    data3 = []
    
    data3.append({
        '매수정산금액 합계': float(result3['buy_excc_amt_smtl']),    # 매수정산금액 합계
        '매도정산금액 합계': float(result3['sll_excc_amt_smtl']),    # 매도정산금액 합계
        '총정산금액': float(result3['tot_excc_amt']),                # 총정산금액
        '총실현손익': float(result3['tot_rlzt_pfls']),        # 총실현손익
        '총수수료': float(result3['tot_fee']),                       # 총수수료
        '총제세금': float(result3['tot_tltx']),                      # 총제세금
    })

    df3 = pd.DataFrame(data3)

    if df3.empty:
        st.warning("손익합산조회된 데이터가 없습니다. 조건을 확인해주세요.")
    else:
        # Streamlit 앱 구성
        st.title("손익 합산 조회")

        df_display = df3.copy().reset_index(drop=True)

        # Grid 옵션 생성
        gb = GridOptionsBuilder.from_dataframe(df_display)
        gb.configure_pagination(enabled=False) 
        gb.configure_grid_options(domLayout='autoHeight')

        column_widths = {
            '매수정산금액 합계': 100,
            '매도정산금액 합계': 100,
            '총정산금액': 120,
            '총실현손익': 80,
            '총수수료': 60,
            '총제세금': 60,
        }

        # 숫자 포맷을 JS 코드로 적용 (정렬 문제 방지)
        number_format_js = JsCode("""
            function(params) {
                if (params.value === null || params.value === undefined) {
                    return '';
                }
                return params.value.toLocaleString();
            }
        """)

        # 숫자 포맷을 적용할 컬럼들 설정
        for col, width in column_widths.items():
            gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js, width=width)

        grid_options = gb.build()

        # AgGrid를 통해 데이터 출력
        AgGrid(
            df_display,
            gridOptions=grid_options,
            fit_columns_on_grid_load=False, 
            allow_unsafe_jscode=True,
            update_mode=GridUpdateMode.NO_UPDATE,
            enable_enterprise_modules=True,  # 엑셀 다운로드 위해 필요
            excel_export_mode='xlsx'         # 엑셀(xlsx)로 다운로드
        )

# 일별주문체결조회
result4 = get_my_complete(access_token, app_key, app_secret, acct_no, strt_dt, end_dt)

if not result4:
    print("일별주문체결조회 결과가 없습니다.")
else:

    # 모든 orgn_odno 수집
    orig_odnos = {item['orgn_odno'] for item in result4 if item['orgn_odno'] != ""}
    data4 = []
    for item in result4:

        odno = item['odno']
        orgn_odno = item['orgn_odno']

        # 주문취소 제외한 주문정보 대상(원주문번호와 동일한 주문번호 대상건 제외)
        if odno not in orig_odnos:            

            data4.append({
                '주문일자': item['ord_dt'],
                '주문시각': item['ord_tmd'],
                '종목명': item['prdt_name'],
                '주문번호': float(odno) if odno != "" else "",
                '원주문번호': float(orgn_odno) if orgn_odno != "" else "",
                '체결금액': float(item['tot_ccld_amt']),
                '주문유형': item['sll_buy_dvsn_cd_name'],
                '주문단가': float(item['ord_unpr']),
                '주문수량': float(item['ord_qty']),
                '체결단가': float(item['avg_prvs']),
                '체결수량': float(item['tot_ccld_qty']),
                '잔여수량': float(item['rmn_qty']),
            })

    df4 = pd.DataFrame(data4)

    if df4.empty:
        st.warning("일별주문체결조회된 데이터가 없습니다. 조건을 확인해주세요.")
    else:
        # Streamlit 앱 구성
        st.title("일별 주문체결 조회")

        all_types = df4['주문유형'].unique()
        주문유형리스트 = [t for t in all_types if t in ('현금매수', '현금매도', '매수정정*', '매도정정*')]
        선택주문유형 = st.selectbox("주문유형을 선택하세요", 주문유형리스트)

        선택주문유형_df = df4[df4['주문유형'] == 선택주문유형].copy()

        선택주문유형_df['주문일자'] = pd.to_datetime(선택주문유형_df['주문일자'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
        선택주문유형_df['주문시각'] = pd.to_datetime(선택주문유형_df['주문시각'], format='%H%M%S').dt.strftime('%H:%M:%S')

        df_display = 선택주문유형_df.sort_values(by=['주문일자', '주문시각'], ascending=False).copy().reset_index(drop=True)

        # Grid 옵션 생성
        gb = GridOptionsBuilder.from_dataframe(df_display)
        # 주문유형 컬럼 숨기기
        gb.configure_column('주문유형', hide=True)
        # 원주문번호 컬럼 숨기기
        gb.configure_column('원주문번호', hide=True)
        # 페이지당 20개 표시
        gb.configure_pagination(enabled=True, paginationPageSize=20)
        gb.configure_grid_options(domLayout='normal')
        # Excel 다운로드를 위한 옵션 추가
        gb.configure_grid_options(enableRangeSelection=True)
        gb.configure_grid_options(enableExcelExport=True)

        # JS 코드: 첫 렌더링 시 모든 컬럼 자동 크기 맞춤 (컬럼명 포함)
        auto_size_js = JsCode("""
        function onFirstDataRendered(params) {
            const allColumnIds = [];
            params.columnApi.getAllColumns().forEach(function(column) {
                allColumnIds.push(column.getId());
            });
            params.columnApi.autoSizeColumns(allColumnIds, false);
        }
        """)
        gb.configure_grid_options(onFirstDataRendered=auto_size_js)

        column_widths = {
            '주문일자': 60,
            '주문시각': 60,
            '종목명': 140,
            '주문번호': 70,
            '체결금액': 100,
            '주문단가': 80,
            '주문수량': 70,
            '체결단가': 80,
            '체결수량': 70,
            '잔여수량': 70,
        }

        # 숫자 포맷을 JS 코드로 적용 (정렬 문제 방지)
        number_format_js = JsCode("""
            function(params) {
                if (params.value === null || params.value === undefined) {
                    return '';
                }
                return params.value.toLocaleString();
            }
        """)

        # 숫자 포맷을 적용할 컬럼들 설정
        for col, width in column_widths.items():
            if col in ['주문단가', '주문수량', '체결단가', '체결수량', '잔여수량', '취소수량', '체결금액']:
                gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js, width=width)
            else:
                gb.configure_column(col, width=width)

        grid_options = gb.build()

        # AgGrid를 통해 데이터 출력
        AgGrid(
            df_display,
            gridOptions=grid_options,
            fit_columns_on_grid_load=False,   # 화면 로드시 자동 폭 맞춤
            allow_unsafe_jscode=True,
            use_container_width=True,
            update_mode=GridUpdateMode.NO_UPDATE,
            enable_enterprise_modules=True,  # 엑셀 다운로드 위해 필요
            excel_export_mode='xlsx'         # 엑셀(xlsx)로 다운로드
        )

reserce_strt_dt = datetime.now().strftime("%Y%m%d")
reserve_end_dt = (datetime.now() + relativedelta(months=1)).strftime("%Y%m%d")
# 전체예약 조회
output = order_reserve_complete(access_token, app_key, app_secret, reserce_strt_dt, reserve_end_dt, str(acct_no), "")

if len(output) > 0:
    tdf = pd.DataFrame(output)
    tdf.set_index('rsvn_ord_seq')
    d = tdf[['rsvn_ord_seq', 'rsvn_ord_ord_dt', 'rsvn_ord_rcit_dt', 'pdno', 'ord_dvsn_cd', 'ord_rsvn_qty', 'tot_ccld_qty', 'cncl_ord_dt', 'ord_tmd', 'odno', 'rsvn_ord_rcit_tmd', 'kor_item_shtn_name', 'sll_buy_dvsn_cd', 'ord_rsvn_unpr', 'tot_ccld_amt', 'cncl_rcit_tmd', 'prcs_rslt', 'ord_dvsn_name', 'rsvn_end_dt']]
    reserve_data = []

    for i, name in enumerate(d.index):
        d_rsvn_ord_seq = int(d['rsvn_ord_seq'][i])          # 예약주문 순번
        d_rsvn_ord_ord_dt = d['rsvn_ord_ord_dt'][i]         # 예약주문주문일자
        d_rsvn_ord_rcit_dt = d['rsvn_ord_rcit_dt'][i]       # 예약주문접수일자
        d_code = d['pdno'][i]
        d_ord_dvsn_cd = d['ord_dvsn_cd'][i]                 # 주문구분코드
        d_ord_rsvn_qty = int(d['ord_rsvn_qty'][i])          # 주문예약수량
        d_tot_ccld_qty = int(d['tot_ccld_qty'][i])          # 총체결수량
        d_cncl_ord_dt = d['cncl_ord_dt'][i]                 # 취소주문일자
        d_ord_tmd = d['ord_tmd'][i]                         # 주문시각
        d_order_no = d['odno'][i]                           # 주문번호
        d_rsvn_ord_rcit_tmd = d['rsvn_ord_rcit_tmd'][i]     # 예약주문접수시각
        d_name = d['kor_item_shtn_name'][i]                 # 종목명
        d_sll_buy_dvsn_cd = d['sll_buy_dvsn_cd'][i]         # 매도매수구분코드
        d_ord_rsvn_unpr = int(d['ord_rsvn_unpr'][i])        # 주문예약단가
        d_tot_ccld_amt = int(d['tot_ccld_amt'][i])          # 총체결금액
        d_cncl_rcit_tmd = d['cncl_rcit_tmd'][i]             # 취소접수시각
        d_prcs_rslt = d['prcs_rslt'][i]                     # 처리결과
        d_ord_dvsn_name = d['ord_dvsn_name'][i]             # 주문구분명
        d_rsvn_end_dt = d['rsvn_end_dt'][i]                 # 예약종료일자

        reserve_data.append({
            '종목명': d_name,
            '예약시작일': d_rsvn_ord_ord_dt,
            '예약종료일': d_rsvn_end_dt,
            '예약구분': d_ord_dvsn_name,
            '예약번호': str(d_rsvn_ord_seq),
            '예약단가': d_ord_rsvn_unpr,
            '예약수량': d_ord_rsvn_qty,
            '처리여부': d_prcs_rslt,
        })
    
    reserve_df = pd.DataFrame(reserve_data)

    if reserve_df.empty:
        st.warning("조회된 데이터가 없습니다. 조건을 확인해주세요.")
    else:
        # Streamlit 앱 구성
        st.title("예약정보 조회")
        
        all_types = reserve_df['예약구분'].unique()
        예약구분리스트 = [t for t in all_types if t in ('현금매수', '현금매도')]
        선택예약구분 = st.selectbox("예약구분을 선택하세요", 예약구분리스트)

        선택예약구분_df = reserve_df[reserve_df['예약구분'] == 선택예약구분].copy()

        선택예약구분_df['예약시작일'] = pd.to_datetime(선택예약구분_df['예약시작일'], format='%Y%m%d').dt.strftime('%Y/%m/%d')
        선택예약구분_df['예약종료일'] = pd.to_datetime(선택예약구분_df['예약종료일'], format='%Y%m%d').dt.strftime('%Y/%m/%d')

        df_display = 선택예약구분_df.sort_values(by=['예약시작일', '예약종료일'], ascending=False).copy().reset_index(drop=True)

        # Grid 옵션 생성
        gb = GridOptionsBuilder.from_dataframe(df_display)
        # 예약구분 컬럼 숨기기
        gb.configure_column('예약구분', hide=True)
        # 페이지당 20개 표시
        gb.configure_pagination(enabled=True, paginationPageSize=20)
        gb.configure_grid_options(domLayout='normal')
        # Excel 다운로드를 위한 옵션 추가
        gb.configure_grid_options(enableRangeSelection=True)
        gb.configure_grid_options(enableExcelExport=True)

        # JS 코드: 첫 렌더링 시 모든 컬럼 자동 크기 맞춤 (컬럼명 포함)
        auto_size_js = JsCode("""
        function onFirstDataRendered(params) {
            const allColumnIds = [];
            params.columnApi.getAllColumns().forEach(function(column) {
                allColumnIds.push(column.getId());
            });
            params.columnApi.autoSizeColumns(allColumnIds, false);
        }
        """)
        gb.configure_grid_options(onFirstDataRendered=auto_size_js)

        column_widths = {
            '예약시작일': 60,
            '예약종료일': 60,
            '종목명': 140,
            '예약번호': 70,
            '예약단가': 80,
            '예약수량': 70,
            '처리여부': 70,
        }

        # 숫자 포맷을 JS 코드로 적용 (정렬 문제 방지)
        number_format_js = JsCode("""
            function(params) {
                if (params.value === null || params.value === undefined) {
                    return '';
                }
                return params.value.toLocaleString();
            }
        """)

        # 숫자 포맷을 적용할 컬럼들 설정
        for col, width in column_widths.items():
            if col in ['예약단가', '예약수량']:
                gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js, width=width)
            else:
                gb.configure_column(col, width=width)

        grid_options = gb.build()

        # AgGrid를 통해 데이터 출력
        AgGrid(
            df_display,
            gridOptions=grid_options,
            fit_columns_on_grid_load=False,   # 화면 로드시 자동 폭 맞춤
            allow_unsafe_jscode=True,
            use_container_width=True,
            update_mode=GridUpdateMode.NO_UPDATE,
            enable_enterprise_modules=True,  # 엑셀 다운로드 위해 필요
            excel_export_mode='xlsx'         # 엑셀(xlsx)로 다운로드
        )

else:
    print("전체예약 조회 결과가 없습니다.")

def kw_auth(APP_KEY, APP_SECRET):

    params = {
		'grant_type': 'client_credentials',  # grant_type
		'appkey': APP_KEY,  # 앱키
		'secretkey': APP_SECRET,  # 시크릿키
	}

    # 인증처리
    PATH = 'oauth2/token'
    url = f"{KW_URL_BASE}/{PATH}"

    headers = {
		'Content-Type': 'application/json;charset=UTF-8', # 컨텐츠타입
	}

	# 3. http POST 요청
    response  = requests.post(url, headers=headers, json=params)

    return response.json()["token"]

async def kw_account(nickname):

    cur01 = conn.cursor()
    cur01.execute("select acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day from \"stockAccount_stock_account\" where nick_name = '" + nickname + "'")
    result_two = cur01.fetchone()
    cur01.close()

    acct_no = result_two[0]
    access_token = result_two[1]
    app_key = result_two[2]
    app_secret = result_two[3]
    today = datetime.now().strftime("%Y%m%d")

    YmdHMS = datetime.now()
    validTokenDate = datetime.strptime(result_two[4], '%Y%m%d%H%M%S')
    diff = YmdHMS - validTokenDate
    # print("diff : " + str(diff.days))
    if diff.days >= 1 or result_two[5] != today:  # 토큰 유효기간(1일) 만료 재발급
        access_token = kw_auth(app_key, app_secret)
        token_publ_date = datetime.now().strftime("%Y%m%d%H%M%S")
        print("new access_token2 : " + access_token)
        # 계정정보 토큰값 변경
        cur02 = conn.cursor()
        update_query = "update \"stockAccount_stock_account\" set access_token = %s, token_publ_date = %s, last_chg_date = %s where acct_no = %s"
        # update 인자값 설정
        record_to_update = ([access_token, token_publ_date, datetime.now(), acct_no])
        # DB 연결된 커서의 쿼리 수행
        cur02.execute(update_query, record_to_update)
        conn.commit()
        cur02.close()

    # WebSocketClient 전역 변수 선언
    websocket_client = WebSocketClient(SOCKET_URL, access_token)
    await websocket_client.run()   

class WebSocketClient:
    def __init__(self, uri, access_token):
        self.uri = uri
        self.access_token = access_token
        self.websocket = None
        self.connected = False
        self.keep_running = True
        self.condition_list = []  # 조건검색 목록 저장
        self.search_results = []  # 조건검색 결과 저장

    # WebSocket 서버에 연결합니다.
    async def connect(self):
        try:
            self.websocket = await websockets.connect(self.uri)
            self.connected = True
            print("서버와 연결을 시도 중입니다.")

            # 로그인 패킷
            param = {
                'trnm': 'LOGIN',
                'token': self.access_token
            }

            print('실시간 시세 서버로 로그인 패킷을 전송합니다.')
            # 웹소켓 연결 시 로그인 정보 전달
            await self.send_message(message=param)

        except Exception as e:
            print(f'Connection error: {e}')
            self.connected = False

    # 서버에 메시지를 보냅니다. 연결이 없다면 자동으로 연결합니다.
    async def send_message(self, message):
        if not self.connected:
            await self.connect()  # 연결이 끊어졌다면 재연결
        if self.connected:
            # message가 문자열이 아니면 JSON으로 직렬화
            if not isinstance(message, str):
                message = json.dumps(message)

            await self.websocket.send(message)
            print(f'Message sent: {message}')

    # 서버에서 오는 메시지를 수신하여 출력합니다.
    async def receive_messages(self):
        while self.keep_running:
            try:
                message = await self.websocket.recv()
                if not message:
                    print('수신된 메시지가 없습니다. 연결이 종료되었을 수 있습니다.')
                    self.connected = False
                    await self.websocket.close()
                    break

                try:
                    response = json.loads(message)
                except json.JSONDecodeError:
                    print(f'JSON 디코딩 오류: {message}')
                    continue

                trnm = response.get('trnm')

                # 메시지 유형이 LOGIN일 경우 로그인 시도 결과 체크
                if trnm == 'LOGIN':
                    if response.get('return_code') != 0:
                        print('로그인 실패하였습니다. : ', response.get('return_msg'))
                        await self.disconnect()
                    else:
                        print('로그인 성공하였습니다.')
                        await self.send_message({'trnm': 'CNSRLST'})

                elif trnm == 'CNSRLST':
                    self.condition_list = response.get('data', [])
                    # print(f'조건검색 목록 수신: {self.condition_list}')
                    if self.condition_list:
                        # 다섯번째 조건검색식: 파워급등주
                        seq5 = self.condition_list[5][0]
                        self.power_rapid_name = self.condition_list[5][1]  # 파워급등주 이름 저장
                        await self.send_message({
                            'trnm': 'CNSRREQ',
                            'seq': seq5,
                            'search_type': '0',
                            'stex_tp': 'K',
                            'cont_yn': 'N',
                            'next_key': '',
                        })
                        # 여섯번째 조건검색식: 파워종목
                        seq6 = self.condition_list[6][0]
                        self.power_item_name = self.condition_list[6][1]  # 파워종목 이름 저장
                        await self.send_message({
                            'trnm': 'CNSRREQ',
                            'seq': seq6,
                            'search_type': '0',
                            'stex_tp': 'K',
                            'cont_yn': 'N',
                            'next_key': '',
                        })

                elif trnm == 'CNSRREQ':
                    self.search_results = response.get('data', [])
                    # print(f'조건검색 결과 수신: {self.search_results}')
                    seq = response.get('seq', '').strip()  # 시퀀스 번호로 구분

                    if seq == self.condition_list[5][0]:  # 파워급등주 결과
                        # print(f'{self.power_rapid_name}')
                        # print(f'{self.power_rapid_name}-{self.search_results}')
                        search_data = []
                        for i in self.search_results:
                            code = i['9001'][1:] if i['9001'].startswith('A') else i['9001']
                            name = i['302']
                            current_price = math.ceil(float(i['10']))
                            rate = float(i['12']) / 1000
                            vol = math.ceil(float(i['13']))
                            high_price = math.ceil(float(i['17']))
                            low_price = math.ceil(float(i['18']))
                            search_data.append({
                                '종목명': name,
                                '종목코드': code,
                                '현재가': current_price,
                                '거래량': vol,
                                '고가': high_price,
                                '저가': low_price,
                                '등락율': rate,
                            })
                            # print(f"{name} [{code}] 현재가: {format(current_price, ',d')}원, 거래량: {format(vol, ',d')}주, 고가: {format(high_price, ',d')}원, 저가: {format(low_price, ',d')}원, 등락율: {rate:.2f}%")
                            
                        search_df = pd.DataFrame(search_data)

                        if search_df.empty:
                            st.warning("조회된 데이터가 없습니다. 조건을 확인해주세요.")
                        else:
                            # Streamlit 앱 구성
                            st.title(self.power_rapid_name)

                            df_display = search_df.copy().reset_index(drop=True)

                            # Grid 옵션 생성
                            gb = GridOptionsBuilder.from_dataframe(df_display)
                            # 페이지당 20개 표시
                            gb.configure_pagination(enabled=True, paginationPageSize=20)
                            gb.configure_grid_options(domLayout='normal')
                            # Excel 다운로드를 위한 옵션 추가
                            gb.configure_grid_options(enableRangeSelection=True)
                            gb.configure_grid_options(enableExcelExport=True)

                            # JS 코드: 첫 렌더링 시 모든 컬럼 자동 크기 맞춤 (컬럼명 포함)
                            auto_size_js = JsCode("""
                            function onFirstDataRendered(params) {
                                const allColumnIds = [];
                                params.columnApi.getAllColumns().forEach(function(column) {
                                    allColumnIds.push(column.getId());
                                });
                                params.columnApi.autoSizeColumns(allColumnIds, false);
                            }
                            """)
                            gb.configure_grid_options(onFirstDataRendered=auto_size_js)

                            column_widths = {
                                '종목명': 140,
                                '종목코드': 80,
                                '현재가': 100,
                                '거래량': 120,
                                '고가': 100,
                                '저가': 100,
                                '등락율': 80,
                            }

                            # 숫자 포맷을 JS 코드로 적용 (정렬 문제 방지)
                            number_format_js = JsCode("""
                                function(params) {
                                    if (params.value === null || params.value === undefined) {
                                        return '';
                                    }
                                    return params.value.toLocaleString();
                                }
                            """)

                            # 숫자 포맷을 적용할 컬럼들 설정
                            for col, width in column_widths.items():
                                gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js, width=width)

                            grid_options = gb.build()

                            # AgGrid를 통해 데이터 출력
                            AgGrid(
                                df_display,
                                gridOptions=grid_options,
                                fit_columns_on_grid_load=False, 
                                allow_unsafe_jscode=True,
                                update_mode=GridUpdateMode.NO_UPDATE,
                                enable_enterprise_modules=True,  # 엑셀 다운로드 위해 필요
                                excel_export_mode='xlsx'         # 엑셀(xlsx)로 다운로드
                            )

                    elif seq == self.condition_list[6][0]:  # 파워종목 결과
                        # print(f'{self.power_item_name}')
                        # print(f'{self.power_item_name}-{self.search_results}')
                        search_data = []
                        for i in self.search_results:
                            code = i['9001'][1:] if i['9001'].startswith('A') else i['9001']
                            name = i['302']
                            current_price = math.ceil(float(i['10']))
                            rate = float(i['12']) / 1000
                            vol = math.ceil(float(i['13']))
                            high_price = math.ceil(float(i['17']))
                            low_price = math.ceil(float(i['18']))
                            search_data.append({
                                '종목명': name,
                                '종목코드': code,
                                '현재가': current_price,
                                '거래량': vol,
                                '고가': high_price,
                                '저가': low_price,
                                '등락율': rate,
                            })
                            # print(f"{name} [{code}] 현재가: {format(current_price, ',d')}원, 거래량: {format(vol, ',d')}주, 고가: {format(high_price, ',d')}원, 저가: {format(low_price, ',d')}원, 등락율: {rate:.2f}%")

                        search_df = pd.DataFrame(search_data)

                        if search_df.empty:
                            st.warning("조회된 데이터가 없습니다. 조건을 확인해주세요.")
                        else:
                            # Streamlit 앱 구성
                            st.title(self.power_item_name)

                            df_display = search_df.copy().reset_index(drop=True)

                            # Grid 옵션 생성
                            gb = GridOptionsBuilder.from_dataframe(df_display)
                            # 페이지당 20개 표시
                            gb.configure_pagination(enabled=True, paginationPageSize=20)
                            gb.configure_grid_options(domLayout='normal')
                            # Excel 다운로드를 위한 옵션 추가
                            gb.configure_grid_options(enableRangeSelection=True)
                            gb.configure_grid_options(enableExcelExport=True)

                            # JS 코드: 첫 렌더링 시 모든 컬럼 자동 크기 맞춤 (컬럼명 포함)
                            auto_size_js = JsCode("""
                            function onFirstDataRendered(params) {
                                const allColumnIds = [];
                                params.columnApi.getAllColumns().forEach(function(column) {
                                    allColumnIds.push(column.getId());
                                });
                                params.columnApi.autoSizeColumns(allColumnIds, false);
                            }
                            """)
                            gb.configure_grid_options(onFirstDataRendered=auto_size_js)                            

                            column_widths = {
                                '종목명': 140,
                                '종목코드': 80,
                                '현재가': 100,
                                '거래량': 120,
                                '고가': 100,
                                '저가': 100,
                                '등락율': 80,
                            }

                            # 숫자 포맷을 JS 코드로 적용 (정렬 문제 방지)
                            number_format_js = JsCode("""
                                function(params) {
                                    if (params.value === null || params.value === undefined) {
                                        return '';
                                    }
                                    return params.value.toLocaleString();
                                }
                            """)

                            # 숫자 포맷을 적용할 컬럼들 설정
                            for col, width in column_widths.items():
                                gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js, width=width)

                            grid_options = gb.build()

                            # AgGrid를 통해 데이터 출력
                            AgGrid(
                                df_display,
                                gridOptions=grid_options,
                                fit_columns_on_grid_load=False, 
                                allow_unsafe_jscode=True,
                                update_mode=GridUpdateMode.NO_UPDATE,
                                enable_enterprise_modules=True,  # 엑셀 다운로드 위해 필요
                                excel_export_mode='xlsx'         # 엑셀(xlsx)로 다운로드
                            )

                # 메시지 유형이 PING일 경우 수신값 그대로 송신
                elif response.get('trnm') == 'PING':
                    # await self.send_message(response)
                    await self.websocket.close()
                    self.keep_running = False
                    self.connected = False
                    sys.exit(0)

                else:
                    print(f'실시간 시세 서버 응답 수신: {response}')

            except websockets.ConnectionClosed:
                print('서버에 의해 연결이 종료되었습니다.')
                self.connected = False
                break
            except Exception as e:
                print(f'예외 발생: {e}')
                self.connected = False
                break        

    # WebSocket 실행
    async def run(self):
        await self.connect()
        await self.receive_messages()

    # WebSocket 연결 종료
    async def disconnect(self):
        self.keep_running = False
        if self.connected and self.websocket:
            await self.websocket.close()
            self.connected = False
            print('Disconnected from WebSocket server')            

asyncio.run(kw_account('kwphills75'))
