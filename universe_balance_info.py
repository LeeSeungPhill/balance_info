import psycopg2 as db
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import JsCode
import requests
import kis_api_resp as resp
import json
import altair as alt
import sys
import websockets
import math
import asyncio
from dateutil.relativedelta import relativedelta
import time

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스
KW_URL_BASE = "https://api.kiwoom.com"    
SOCKET_URL = "wss://api.kiwoom.com:10000/api/dostk/websocket"  # 접속 URL

# PostgreSQL 연결 설정
kis_conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='asdf1234'"
# kis_conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"
# DB 연결
kis_conn = db.connect(kis_conn_string)

kis_nickname = ['phills2', 'chichipa', 'phills75', 'yh480825', 'phills13', 'phills15', 'mamalong', 'honeylong', 'worry106']
# kis_nickname = ['yh480825']  

# PostgreSQL 연결 설정
conn_string = "dbname='universe' host='localhost' port='5432' user='postgres' password='asdf1234'"
# conn_string = "dbname='universe' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"
# DB 연결
conn = db.connect(conn_string)

market = ['UPBIT', 'BITHUMB']
my_choice1 = st.selectbox('시장을 선택하세요', market)   
nickname = ['phills2', 'mama', 'honey']
my_choice2 = st.selectbox('닉네임을 선택하세요', nickname) 

cur1 = conn.cursor()
result_1 = []

# 잔고정보 조회
select1 = """
    select 
        A.acct_no, 
        A.cust_num,
        A.market_name,
        A.prd_nm,
        A.hold_price,
        A.hold_volume,
        A.hold_amt,
        A.loss_profit_rate,
        A.last_order_no,
        A.last_buy_count,
        A.last_sell_count,
        A.current_price, 
        A.current_amt,
        A.loss_price,
        A.target_price, 
        A.proc_yn, 
        A.regr_id,
        A.reg_date, 
        A.chgr_id, 
        A.chg_date
    from balance_info A, cust_mng B
    where A.cust_num = B.cust_num
    and B.market_name = %s
    and B.cust_nm = %s
    and A.prd_nm not in ('KRW-P')
"""

param1 = (my_choice1, my_choice2,)
cur1.execute(select1, param1)  
result0 = cur1.fetchall()
cur1.close()

if not result0:
    print("잔고 조회 결과가 없습니다.")
else:
    data0 = []
    
    for item in result0:

        data0.append({
            '시장명': item[2],
            '종목명': item[3],
            '매입단가': float(item[4]),
            '매입수량': float(item[5]),
            '매입금액': float(item[6]),
            '현재가': float(item[11]),
            '평가금액': float(item[12]),
            '손익률(%)': float(item[7]) if item[7] is not None else 0.0,
            '손익금액': float(item[12]) - float(item[6]),
        })     

    df0 = pd.DataFrame(data0)
    df0['손익률(%)'] = df0['손익률(%)'].fillna(0.0)

    if df0.empty:
        st.warning("조회된 데이터가 없습니다. 조건을 확인해주세요.")
    else:
        # Streamlit 앱 구성
        st.title("잔고정보 조회")
        
        total_amt = df0['평가금액'].sum()
        cash_amt = df0[df0['종목명'] == 'KRW-KRW']['평가금액'].sum()
        df_filtered = df0[~df0['종목명'].isin(['KRW-KRW'])]
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
            '매입단가': 100,
            '매입수량': 100,
            '매입금액': 100,
            '현재가': 100,
            '평가금액': 100,
            '손익률(%)': 80,
            '손익금액': 100,
            '비중(%)': 80
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

# strt_dt = (st.date_input("시작일", datetime.today() - timedelta(days=30))).strftime("%Y%m%d")
strt_dt = (st.date_input("시작일", datetime(2026, 1, 1), key="strt_dt_balance")).strftime("%Y%m%d")
end_dt = (st.date_input("종료일", datetime.today(), key="end_dt_balance")).strftime("%Y%m%d")

cur2 = conn.cursor()
# 기간별 잔고정보 조회
select2 = """
    SELECT  
        A.sday,
        SUM(CASE WHEN A.prd_nm NOT IN ('KRW-P', 'KRW-KRW') THEN A.hold_amt END) AS hold_amt_sum,
        SUM(CASE WHEN A.prd_nm NOT IN ('KRW-P', 'KRW-KRW') THEN A.current_amt END) AS current_amt_sum,
        SUM(CASE WHEN A.prd_nm NOT IN ('KRW-P', 'KRW-KRW') THEN A.current_amt END) - SUM(CASE WHEN A.prd_nm NOT IN ('KRW-P', 'KRW-KRW') THEN A.hold_amt END) AS loss_profit_amt_sum,
        SUM(CASE WHEN A.prd_nm IN ('KRW-P', 'KRW-KRW') THEN A.current_amt ELSE 0 END) AS krw_amt
    FROM dly_balance_info A, cust_mng B
    WHERE A.cust_num = B.cust_num
    AND B.market_name = %s
    AND B.cust_nm = %s
    AND A.sday between %s AND %s
	GROUP BY A.sday 
"""

param2 = (my_choice1, my_choice2, strt_dt, end_dt)
cur2.execute(select2, param2)  
result1 = cur2.fetchall()
cur2.close()

data01 = []
for item in result1:

    전체금액 = float(item[4]) + float(item[2])  # 예수금 + 평가금액
    예수금 = float(item[4])

    data01.append({
        '일자': item[0],
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
            '예수금비율(%)': 80
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
    
cur3 = conn.cursor()
# 일별주문체결조회
select3 = """
    SELECT  
		substr(ord_dtm, 0, 9) AS ord_dt,
		substr(ord_dtm, 9, 6) AS ord_tmd,
		prd_nm,
		ord_no,
		orgn_ord_no,
		ord_amt,
		ord_tp,
		ord_price,
		ord_vol,
		ord_type,
		executed_vol,
		remaining_vol,
        ord_state,
        hold_price,
        hold_vol,
        paid_fee
	FROM trade_mng A, cust_mng B
    WHERE A.cust_num = B.cust_num
    AND B.market_name = %s
    AND B.cust_nm = %s
    AND substr(ord_dtm, 0, 9) between %s AND %s
    
    UNION ALL
    
    SELECT  
		substr(ord_dtm, 0, 9) AS ord_dt,
		substr(ord_dtm, 9, 6) AS ord_tmd,
		prd_nm,
		ord_no,
		orgn_ord_no,
		ord_amt,
		ord_tp,
		ord_price,
		ord_vol,
		ord_type,
		executed_vol,
		remaining_vol,
        ord_state,
        hold_price,
        hold_vol,
        paid_fee
	FROM trade_mng_hist A, cust_mng B
    WHERE A.cust_num = B.cust_num
    AND B.market_name = %s
    AND B.cust_nm = %s
    AND substr(ord_dtm, 0, 9) between %s AND %s
"""

param3 = (my_choice1, my_choice2, strt_dt, end_dt, my_choice1, my_choice2, strt_dt, end_dt)
cur3.execute(select3, param3)  
result2 = cur3.fetchall()
cur3.close()

if not result2:
    print("일별주문체결조회 결과가 없습니다.")
else:

    # 모든 orgn_odno 수집
    orig_odnos = {item[4] for item in result2 if item[4] != ""}
    data4 = []
    for item in result2:

        odno = item[3]
        orgn_odno = item[4]

        # 주문취소 제외한 주문정보 대상(원주문번호와 동일한 주문번호 대상건 제외)
        if odno not in orig_odnos:            

            data4.append({
                '주문일자': item[0],
                '주문시각': item[1],
                '종목명': item[2],
                '주문번호': odno if odno != "" else "",
                '원주문번호': orgn_odno if orgn_odno != "" else "",
                '주문금액': int(float(item[7])*float(item[8])),
                '체결금액': int(float(item[7])*float(item[10])),
                '주문유형': '매수' if item[6] == '01' else '매도',
                '주문상태': item[12],
                '주문단가': float(item[7]),
                '주문수량': float(item[8]),
                # '체결단가': float(item['avg_prvs']),
                '체결수량': float(item[10]),
                '잔여수량': float(item[11]),
                '보유단가': float(item[13]) if item[13] != None else 0,
                '보유수량': float(item[14]) if item[14] != None else 0,
                '수수료': float(item[15]) if item[15] != None else 0,
                '손실수익금': int((float(item[7])-float(item[13]))*float(item[10])) if item[13] != None else 0,
            })

    df4 = pd.DataFrame(data4)

    if df4.empty:
        st.warning("일별주문체결조회된 데이터가 없습니다. 조건을 확인해주세요.")
    else:
        # Streamlit 앱 구성
        st.title("일별 주문체결 조회")

        # 주문유형 필터
        all_types = df4['주문유형'].unique()
        주문유형리스트 = [t for t in all_types if t in ('매수', '매도')]
        선택주문유형 = st.selectbox("주문유형을 선택하세요", 주문유형리스트)

        # 주문상태 필터
        all_states = df4['주문상태'].unique()
        주문상태리스트 = [t for t in all_states if t in ('done', 'cancel', 'wait')]
        선택주문상태 = st.selectbox("주문상태를 선택하세요", 주문상태리스트)

        # 선택한 주문유형 + 주문상태로 필터링
        선택필터_df = df4[
            (df4['주문유형'] == 선택주문유형) &
            (df4['주문상태'] == 선택주문상태)
        ].copy()

        # 날짜/시간 포맷 변경
        선택필터_df['주문일자'] = pd.to_datetime(선택필터_df['주문일자'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
        선택필터_df['주문시각'] = pd.to_datetime(선택필터_df['주문시각'], format='%H%M%S').dt.strftime('%H:%M:%S')

        # 최신 순 정렬
        df_display = 선택필터_df.sort_values(by=['주문일자', '주문시각'], ascending=False).reset_index(drop=True)

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
            '주문일자': 80,
            '주문시각': 80,
            '종목명': 140,
            '주문상태': 80,
            '주문번호': 100,
            '주문금액': 100,
            '체결금액': 100,
            '보유단가': 100,
            '보유수량': 100,
            '주문단가': 100,
            '주문수량': 100,
            # '체결단가': 80,
            '체결수량': 100,
            '잔여수량': 100,
            '수수료': 70,
            '손실수익금': 100
        }

        # 숫자 포맷 JS
        number_format_js = JsCode("""
            function(params) {
                if (params.value === null || params.value === undefined) {
                    return '';
                }
                return params.value.toLocaleString();
            }
        """)

        # 숫자 포맷 적용
        for col, width in column_widths.items():
            if col in ['보유단가', '보유수량', '주문단가', '주문수량', '체결수량', '잔여수량', '취소수량', '주문금액', '체결금액', '수수료', '손실수익금']:
                gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js, width=width)
            else:
                gb.configure_column(col, width=width)

        grid_options = gb.build()

        # AgGrid 출력
        AgGrid(
            df_display,
            gridOptions=grid_options,
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,
            use_container_width=True,
            update_mode=GridUpdateMode.NO_UPDATE,
            enable_enterprise_modules=True,  # 엑셀 다운로드 위해 필요
            excel_export_mode='xlsx'         # 엑셀(xlsx)로 다운로드
        )   

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

# 시장비율 계산 (Batch/kis_interest_item_total.py의 compute_market_ratio 참조)
def compute_market_ratio(kospi_short, kospi_mid, kospi_long, kosdak_short, kosdak_mid, kosdak_long):

    rules = [
        (kospi_short,  '01', '02', 5),
        (kospi_mid,    '03', '04', 8),
        (kospi_long,   '05', '06', 12),
        (kosdak_short, '01', '02', 5),
        (kosdak_mid,   '03', '04', 8),
        (kosdak_long,  '05', '06', 12),
    ]
    score = 0
    for signal, bull, bear, weight in rules:
        if signal == bull:
            score += weight
        elif signal == bear:
            score -= weight
    return max(0, min(100, 50 + score))

# 시장신호 조회 (코스피/코스닥 단기, 중기, 장기)
def fetch_market_state(conn, acct_no):

    cur = conn.cursor()
    cur.execute("""
        SELECT kospi_short, kospi_mid, kospi_long,
               kosdak_short, kosdak_mid, kosdak_long
          FROM "stockFundMng_stock_fund_mng"
         WHERE acct_no = %s
         ORDER BY id DESC
         LIMIT 1
    """, (str(acct_no),))
    row = cur.fetchone()
    cur.close()
    return dict(zip(
        ('kospi_short', 'kospi_mid', 'kospi_long',
         'kosdak_short', 'kosdak_mid', 'kosdak_long'),
        row if row else (None,) * 6
    ))

# ────────────────────────────────────────────────────────────────────────────
# 시장비율 리밸런싱 매도대상 선정 로직 (simul/kis_market_ratio_rebalance.py 참조)
# ────────────────────────────────────────────────────────────────────────────
W_SUPPLY     = 0.5    # strength = W_SUPPLY*수급
W_CHART      = 0.5    # strength = W_CHART*차트
TOP_CUT      = 70     # sell_priority(100 - strength) 이 값 이상이면 종목당 전량 매도 허용, 아니면 일 최대 50%
PER_NAME_CAP = 0.5    # 종목당 1일 최대 매도 비중 (평가금액 대비)

def _reb_headers(access_token, app_key, app_secret, tr_id):
    return {"Content-Type": "application/json",
            "authorization": f"Bearer {access_token}",
            "appKey": app_key, "appSecret": app_secret,
            "tr_id": tr_id, "custtype": "P"}

def _fetch_daily_ohlcv(at, ak, asec, code):
    try:
        r = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-price",
                         headers=_reb_headers(at, ak, asec, "FHKST01010400"),
                         params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code,
                                 "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "1"},
                         verify=False, timeout=10)
        rows = r.json().get("output") or []
        return rows if isinstance(rows, list) else []
    except Exception:
        return []

def _fetch_short_selling(at, ak, asec, code):
    today  = datetime.now().strftime("%Y%m%d")
    d60ago = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
    try:
        r = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-short-over",
                         headers=_reb_headers(at, ak, asec, "FHPST04830000"),
                         params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code,
                                 "FID_INPUT_DATE_1": d60ago, "FID_INPUT_DATE_2": today},
                         verify=False, timeout=10)
        d = r.json()
        return (d.get("output2") or []) if d.get("rt_cd") == "0" else []
    except Exception:
        return []

def _fetch_investor(at, ak, asec, code):
    try:
        r = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-investor",
                         headers=_reb_headers(at, ak, asec, "FHKST01010900"),
                         params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
                         verify=False, timeout=10)
        d = r.json()
        return d["output"] if d.get("rt_cd") == "0" and isinstance(d.get("output"), list) else []
    except Exception:
        return []

def _fetch_cur_price_out(at, ak, asec, code):
    try:
        r = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
                         headers=_reb_headers(at, ak, asec, "FHKST01010100"),
                         params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
                         verify=False, timeout=10)
        d = r.json()
        return d["output"] if d.get("rt_cd") == "0" and d.get("output") else None
    except Exception:
        return None

def _adx(highs, lows, closes, period=14):
    n = len(closes)
    if n < period * 2:
        return None, None, None
    trs, plus_dm, minus_dm = [], [], []
    for i in range(1, n):
        up, dn = highs[i] - highs[i-1], lows[i-1] - lows[i]
        plus_dm.append(up if (up > dn and up > 0) else 0.0)
        minus_dm.append(dn if (dn > up and dn > 0) else 0.0)
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))

    def _smooth(x):
        s = [sum(x[:period])]
        for v in x[period:]:
            s.append(s[-1] - s[-1]/period + v)
        return s
    tr_s, pdm_s, mdm_s = _smooth(trs), _smooth(plus_dm), _smooth(minus_dm)
    dxs = []
    for tr, pdm, mdm in zip(tr_s, pdm_s, mdm_s):
        if tr == 0:
            continue
        pdi, mdi = 100*pdm/tr, 100*mdm/tr
        if pdi + mdi == 0:
            continue
        dxs.append(100*abs(pdi-mdi)/(pdi+mdi))
    if len(dxs) < period:
        return None, None, None
    adx = sum(dxs[-period:]) / period
    pdi = 100*pdm_s[-1]/tr_s[-1] if tr_s[-1] else 0.0
    mdi = 100*mdm_s[-1]/tr_s[-1] if tr_s[-1] else 0.0
    return adx, pdi, mdi

def _obv_trend(closes, volumes, n=5):
    if len(closes) < n + 1:
        return 0.0
    obv = [0.0]
    for i in range(1, len(closes)):
        if   closes[i] > closes[i-1]: obv.append(obv[-1] + volumes[i])
        elif closes[i] < closes[i-1]: obv.append(obv[-1] - volumes[i])
        else:                         obv.append(obv[-1])
    recent, past = obv[0], obv[min(n, len(obv)-1)]
    denom = abs(past) if past else 1.0
    return (recent - past) / denom * 100

def _calc_chart_score(rows):
    if not rows or len(rows) < 25:
        return None
    def _f(v):
        try: return float(v)
        except: return 0.0
    closes  = [_f(r.get("stck_clpr", 0)) for r in rows]
    highs   = [_f(r.get("stck_hgpr", 0)) for r in rows]
    lows    = [_f(r.get("stck_lwpr", 0)) for r in rows]
    volumes = [_f(r.get("acml_vol",  0)) for r in rows]
    cur = closes[0]
    ma5  = sum(closes[:5]) / 5
    ma20 = sum(closes[:20]) / 20
    ma60 = sum(closes[:60]) / 60 if len(closes) >= 60 else None

    if ma60:
        if   ma5 > ma20 > ma60:               trend_sc = 30
        elif ma5 > ma20 and ma20 < ma60:      trend_sc = 22
        elif ma5 > ma60 and ma5 <= ma20:      trend_sc = 16
        elif abs(ma5-ma20)/ma20 < 0.01:       trend_sc = 10
        elif ma5 < ma20 and ma20 > ma60:      trend_sc = 5
        else:                                 trend_sc = 0
    else:
        if   ma5 > ma20*1.02: trend_sc = 22
        elif ma5 > ma20:      trend_sc = 16
        elif ma5 > ma20*0.99: trend_sc = 10
        else:                 trend_sc = 0

    adx, pdi, mdi = _adx(list(reversed(highs)), list(reversed(lows)), list(reversed(closes)))
    if adx is None:                            adx_sc = 8
    elif adx >= 40 and pdi > mdi:              adx_sc = 25
    elif adx >= 25 and pdi > mdi:              adx_sc = 20
    elif adx >= 25 and pdi <= mdi:             adx_sc = 5
    elif adx >= 20:                            adx_sc = 12
    else:                                      adx_sc = 8

    d = (cur - ma20)/ma20*100 if ma20 else 0.0
    if   -3 <= d <= 5:    dev_sc = 20
    elif  5 <  d <= 10:   dev_sc = 15
    elif -8 <= d < -3:    dev_sc = 15
    elif -15 <= d < -8:   dev_sc = 12
    elif 10 <  d <= 15:   dev_sc = 10
    elif d < -15:         dev_sc = 8
    else:                 dev_sc = 5

    va5, va20 = sum(volumes[:5])/5, sum(volumes[:20])/20
    v = va5/va20*100 if va20 else 100
    vol_sc = 15 if v > 150 else 12 if v > 120 else 9 if v > 90 else 6 if v > 70 else 3

    vod = volumes[0]/volumes[1]*100 if len(volumes) >= 2 and volumes[1] > 0 else 100
    vod_sc = 10 if vod > 150 else 8 if vod > 120 else 6 if vod > 80 else 4 if vod > 50 else 2

    return trend_sc + adx_sc + dev_sc + vol_sc + vod_sc

def _calc_supply_score(ohlcv, inv, price, ssts=None):
    if not inv:
        return None
    def _si(v):
        try: return int(v)
        except: return 0
    def _sf(v):
        try: return float(v)
        except: return 0.0
    n5 = min(5, len(inv))
    frgn = sum(_si(r.get("frgn_ntby_tr_pbmn", 0)) for r in inv[:n5]) / 100
    orgn = sum(_si(r.get("orgn_ntby_tr_pbmn", 0)) for r in inv[:n5]) / 100
    if ssts:
        nd5 = min(5, len(ssts))
        ssts_avg = sum(_sf(r.get("ssts_vol_rlim", 0)) for r in ssts[:nd5]) / nd5
    else:
        ssts_avg = 0.0
    loan = _sf((price or {}).get("whol_loan_rmnd_rate", 0))
    obv = 0.0
    if ohlcv and len(ohlcv) >= 6:
        closes  = [_sf(r.get("stck_clpr", 0)) for r in ohlcv]
        volumes = [_sf(r.get("acml_vol",  0)) for r in ohlcv]
        obv = _obv_trend(closes, volumes, n=5)

    frgn_sc = 30 if frgn > 200 else 24 if frgn > 50 else 18 if frgn > 10 else 14 if frgn > 0 else 8 if frgn > -10 else 3 if frgn > -50 else 0
    orgn_sc = 25 if orgn > 200 else 20 if orgn > 50 else 15 if orgn > 10 else 11 if orgn > 0 else 6 if orgn > -10 else 2 if orgn > -50 else 0
    ssts_sc = 20 if ssts_avg < 1 else 16 if ssts_avg < 2 else 12 if ssts_avg < 3 else 8 if ssts_avg < 5 else 4 if ssts_avg < 10 else 0
    loan_sc = 15 if loan < 0.5 else 12 if loan < 1 else 9 if loan < 2 else 5 if loan < 5 else 2 if loan < 10 else 0
    obv_sc  = 10 if obv > 3 else 7 if obv > 0 else 5 if obv > -3 else 2
    return frgn_sc + orgn_sc + ssts_sc + loan_sc + obv_sc

def total_excess(cash, total_eval, market_ratio):
    """base = 현금 + 전체 평가금액, target = base*market_ratio/100, excess = 평가금액-target(>0 이면 매도).
    KOSPI/KOSDAQ 구분 없이 트레이딩풀 전체를 대상으로 한 단일 초과분(원)."""
    if market_ratio is None or total_eval <= 0:
        return 0
    base = total_eval + cash
    return int(max(0, total_eval - base * market_ratio / 100))

def sell_priority(strength):
    """수급 또는 차트 strength 약할수록(낮을수록) 우선순위 높음.
    invest_point(quality, 성장/가치 점수)는 매도 우선순위 산정에서 제외(참고용 표시만 유지)."""
    return 100 - strength

def allocate(bucket, excess, cur_price_key="current_price", avail_key="avail_qty"):
    """bucket: sell_priority 계산된 holding dict 리스트. excess 만큼 순위 충전식 배분.
    각 holding dict 는 'sell_priority','eval_sum',cur_price_key,avail_key 를 가진다.
    반환: [(holding, qty), ...]"""
    ranked = sorted(bucket, key=lambda h: -h["sell_priority"])
    orders, filled = [], 0
    for h in ranked:
        if filled >= excess:
            break
        cur = h[cur_price_key]
        avail = int(h.get(avail_key, 0) or 0)
        if cur <= 0 or avail <= 0:
            continue
        # 매도 할당 금액 : 수급 또는 차트 strength > 70 이면 전체 물량, 아니면 절반 물량
        cap_amt = h["eval_sum"] if h["sell_priority"] >= TOP_CUT else h["eval_sum"] * PER_NAME_CAP
        # 시장비율 초과하여 감축할 금액(cap_amt 와 초과한 물량에서 차감한 물량 중 최소 금액)
        amt = min(cap_amt, excess - filled)
        qty = int(amt // cur)
        qty = min(qty, avail)
        if qty > 0:
            orders.append((h, qty))
            filled += qty * cur
    return orders

def build_rebalance_orders(holdings, cash, market_ratio, strength_fn, quality_fn):
    """코어 엔진. holdings 각 dict: code,name,eval_sum,current_price,avail_qty,purchase_price.
    strength_fn(code)->0~100, quality_fn(code)->0~100 주입.
    quality(invest_point)는 참고용으로 h['quality']에 기록만 하고 sell_priority 산정에는 쓰지 않음.
    KOSPI/KOSDAQ 시장 구분 없이 보유종목 전체를 sell_priority 단일 순위로 배분.
    반환: ([(holding, qty), ...], excess)"""
    total_eval = sum(h["eval_sum"] for h in holdings)
    excess = total_excess(cash, total_eval, market_ratio)
    if excess <= 0:
        return [], excess

    for h in holdings:
        st_ = strength_fn(h["code"])
        ql = quality_fn(h["code"])
        h["strength"], h["quality"] = st_, ql
        h["sell_priority"] = sell_priority(st_)      # 수급점수, 차트점수 기반 매도 우선 순위
    return allocate(holdings, excess), excess

def _make_strength_fn(ac, cache):
    at, ak, asec = ac["access_token"], ac["app_key"], ac["app_secret"]

    def fn(code):
        if code in cache:
            return cache[code]
        time.sleep(0.25)
        ohlcv = _fetch_daily_ohlcv(at, ak, asec, code)
        ssts  = _fetch_short_selling(at, ak, asec, code)
        inv   = _fetch_investor(at, ak, asec, code)
        price = _fetch_cur_price_out(at, ak, asec, code)
        chart  = _calc_chart_score(ohlcv)
        supply = _calc_supply_score(ohlcv, inv, price, ssts=ssts)
        chart  = 50 if chart  is None else chart
        supply = 50 if supply is None else supply
        # 합산(차트+수급) strength = W_SUPPLY * supply + W_CHART * chart
        # 차트 strength = W_CHART * chart
        st_ = W_CHART * chart
        cache[code] = st_
        return st_
    return fn

def order_cash(buy_flag, access_token, app_key, app_secret, acct_no, stock_code,
               ord_dvsn, order_qty, order_price, excg_id="KRX"):
    """현금 주문. 매도=False(TTTC0011U). 시장가=ord_dvsn '01', order_price '0'."""
    tr_id = "TTTC0012U" if buy_flag else "TTTC0011U"
    params = {"CANO": acct_no, "ACNT_PRDT_CD": "01", "PDNO": stock_code,
              "ORD_DVSN": ord_dvsn, "ORD_QTY": str(order_qty), "ORD_UNPR": str(order_price),
              "EXCG_ID_DVSN_CD": excg_id}
    res = requests.post(f"{URL_BASE}/uapi/domestic-stock/v1/trading/order-cash",
                        headers=_reb_headers(access_token, app_key, app_secret, tr_id),
                        data=json.dumps(params), verify=False, timeout=10)
    ar = resp.APIResp(res)
    return ar  # 호출부에서 isOK()/output 판단

def record_sell(conn, acct_no, h, qty, est_price, order_no, horizon):
    """매도 접수 후 trading_trail 에 완료-매도(trail_tp='4') 레코드 INSERT.
    est_price 는 예상체결가(현재가). 정확한 체결가는 주문체결 배치가 보정."""
    now = datetime.now()
    yd, hms = now.strftime("%Y%m%d"), now.strftime("%H%M%S")
    basic_price = h["purchase_price"]
    trail_amt = est_price * qty
    trail_rate = round((est_price - basic_price) / basic_price * 100, 2) if basic_price else 0
    loss_amt = int((basic_price - est_price) * qty)
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO trading_trail (
                acct_no, code, name, trail_day, trail_dtm, trail_tp,
                stop_price, target_price, trail_plan,
                basic_price, basic_qty, basic_amt,
                proc_min, trade_tp, exit_price, loss_amt,
                trail_price, trail_qty, trail_amt, trail_rate, trade_result,
                order_no, crt_dt, mod_dt
            ) VALUES (%s,%s,%s,%s,%s,'4', 0,0,%s, %s,%s,%s, %s,'M', 0,%s,
                      %s,%s,%s,%s,%s, %s,%s,%s)
        """, (acct_no, h["code"], h["name"], yd, hms,
              str(int(qty)), basic_price, qty, basic_price * qty,
              hms, loss_amt,
              est_price, qty, trail_amt, trail_rate, f"시장비율 리밸런싱 매도({horizon})",
              str(order_no or ""), now, now))
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        print(f"[record_sell 오류] {h['name']}[{h['code']}]: {e}")

# 계정정보 조회
def account(nickname):

    cur01 = kis_conn.cursor()
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
        cur02 = kis_conn.cursor()
        update_query = "update \"stockAccount_stock_account\" set access_token = %s, token_publ_date = %s, last_chg_date = %s where acct_no = %s"
        # update 인자값 설정
        record_to_update = ([access_token, token_publ_date, datetime.now(), acct_no])
        # DB 연결된 커서의 쿼리 수행
        cur02.execute(update_query, record_to_update)
        kis_conn.commit()
        cur02.close()

    account_rtn = {'acct_no':acct_no, 'access_token':access_token, 'app_key':app_key, 'app_secret':app_secret}

    return account_rtn

# 계좌잔고 조회
def stock_balance(access_token, app_key, app_secret, acct_no):

    t = datetime.now().strftime('%H%M')
    
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8434R"} 
    params = {
                "CANO": acct_no,                # 종합계좌번호 계좌번호 체계(8-2)의 앞 8자리
                'ACNT_PRDT_CD': '01',           # 계좌상품코드 계좌번호 체계(8-2)의 뒤 2자리
                'AFHR_FLPR_YN': 'N' if '0900' <= t < '1530' else 'X',            # N : 기본값, Y : 시간외단일가, X : NXT 정규장 (프리마켓, 메인, 애프터마켓) NXT 거래종목만 시세 등 정보가 NXT 기준으로 변동됩니다. KRX 종목들은 그대로 유지
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
        ar = None
        for attempt in range(3):
            res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
            ar = resp.APIResp(res)
            if ar.isOK():
                body = ar.getBody()
                output1 = body.output1 if hasattr(body, 'output1') else []
                output2 = body.output2 if hasattr(body, 'output2') else {}

                return output1, output2
            time.sleep(0.3 * (attempt + 1))
    
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

st.title("<KIS>")
kis_my_choice = st.selectbox('KIS 닉네임을 선택하세요', kis_nickname)   

ac = account(kis_my_choice)
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
        total_amt = df0['평가금액'].sum()
        cash_amt = df0[df0['종목명'] == '현금']['평가금액'].sum()
        df_filtered = df0[~df0['종목명'].isin(['현금'])]
        total_hold_amt = df_filtered['매입금액'].sum()
        total_eval_amt = df_filtered['평가금액'].sum()
        total_profit_amt = df_filtered['손익금액'].sum()
        profit_rate = (total_profit_amt / total_hold_amt * 100) if total_hold_amt != 0 else 0.0

        # 시장비율 계산 (코스피/코스닥 단기, 중기, 장기 신호 조합)
        market_state = fetch_market_state(kis_conn, acct_no)
        market_ratio = compute_market_ratio(
            market_state['kospi_short'],  market_state['kospi_mid'],  market_state['kospi_long'],
            market_state['kosdak_short'], market_state['kosdak_mid'], market_state['kosdak_long'],
        )

        # 시장비율 구성 다이버징 바 차트 (코스피/코스닥 단기, 중기, 장기 신호 조합)
        # 도넛은 6개 조각 크기가 고정 가중치라 색만으로 상승/하락을 구분해야 했는데,
        # 막대 방향(좌/우)이 직접 상승/하락을 나타내므로 더 명확하게 인식된다.
        UP_COLOR = '#e34948'
        DOWN_COLOR = '#2a78d6'
        NEUTRAL_COLOR = '#898781'

        market_segments = [
            ('코스피 단기', market_state['kospi_short'],  '01', '02', 5),
            ('코스피 중기', market_state['kospi_mid'],    '03', '04', 8),
            ('코스피 장기', market_state['kospi_long'],   '05', '06', 12),
            ('코스닥 단기', market_state['kosdak_short'], '01', '02', 5),
            ('코스닥 중기', market_state['kosdak_mid'],   '03', '04', 8),
            ('코스닥 장기', market_state['kosdak_long'],  '05', '06', 12),
        ]

        market_names, market_scores, market_colors, market_texts = [], [], [], []
        neutral_x, neutral_y = [], []

        for name, signal, bull, bear, weight in market_segments:
            if signal == bull:
                status, color, score, text = '상승', UP_COLOR, weight, f'상승 +{weight}점'
            elif signal == bear:
                status, color, score, text = '하락', DOWN_COLOR, -weight, f'하락 -{weight}점'
            else:
                status, color, score, text = '데이터없음', NEUTRAL_COLOR, 0, '데이터없음'
                neutral_x.append(0)
                neutral_y.append(name)

            market_names.append(name)
            market_scores.append(score)
            market_colors.append(color)
            market_texts.append(text)

        market_fig = go.Figure()

        market_fig.add_trace(go.Bar(
            x=market_scores,
            y=market_names,
            orientation='h',
            marker=dict(color=market_colors),
            text=market_texts,
            textposition='outside',
            cliponaxis=False,
            hovertemplate='<b>%{y}</b><br>%{text}<extra></extra>'
        ))

        if neutral_x:
            market_fig.add_trace(go.Scatter(
                x=neutral_x, y=neutral_y, mode='markers',
                marker=dict(symbol='diamond', size=10, color=NEUTRAL_COLOR,
                            line=dict(width=1, color='#fcfcfb')),
                showlegend=False,
                hovertemplate='<b>%{y}</b><br>데이터없음<extra></extra>'
            ))

        market_fig.update_layout(
            title=f'시장비율 구성 — 코스피·코스닥 단기/중기/장기 (시장비율 {market_ratio}/100)',
            xaxis=dict(title='하락(-) / 상승(+) 기여도', range=[-15, 15],
                       zeroline=True, zerolinewidth=2, zerolinecolor='#0b0b0b'),
            yaxis=dict(autorange='reversed'),
            showlegend=False,
            bargap=0.35,
        )

        st.plotly_chart(market_fig)

        st.title("잔고정보 조회")

        # 트레이딩/투자 대상 조회 (public."stockBalance_stock_balance")
        cur_trading = kis_conn.cursor()
        cur_trading.execute("""
            SELECT code, name, purchase_price, purchase_amount, purchase_sum, current_price, eval_sum,
                    COALESCE(avail_amount, purchase_amount, 0) AS avail_qty
                FROM "stockBalance_stock_balance"
                WHERE acct_no = %s AND proc_yn = 'Y'
                AND (trading_plan <> 'i' OR trading_plan IS NULL)
            UNION ALL
            SELECT 
                '' AS code,	'현금' AS name,	0 AS purchase_price, 0 AS purchase_amount, 0 AS purchase_sum, 0 AS current_price, (SELECT prvs_rcdl_excc_amt FROM public."stockFundMng_stock_fund_mng" WHERE acct_no = %s) AS eval_sum,	0 AS avail_qty                
        """, (str(acct_no),str(acct_no),))
        trading_rows = cur_trading.fetchall()
        cur_trading.close()

        cur_invest = kis_conn.cursor()
        cur_invest.execute("""
            SELECT name, purchase_price, purchase_amount, purchase_sum, current_price, eval_sum
                FROM "stockBalance_stock_balance"
                WHERE acct_no = %s AND proc_yn = 'Y' AND trading_plan = 'i'
        """, (str(acct_no),))
        invest_rows = cur_invest.fetchall()
        cur_invest.close()

        # 추가1) 트레이딩금액 (trading_plan <> 'i' 이거나 NULL, proc_yn = 'Y')
        trading_holdings = [{
            'code': code_,
            'name': name_,
            'purchase_price': int(float(pur_price_ or 0)),
            'purchase_amount': float(pur_amt_ or 0),
            'purchase_sum': float(pur_sum_ or 0),
            'current_price': int(float(cur_price_ or 0)),
            'eval_sum': float(eval_sum_ or 0),
            'avail_qty': int(avail_qty_ or 0),
        } for code_, name_, pur_price_, pur_amt_, pur_sum_, cur_price_, eval_sum_, avail_qty_ in trading_rows]

        trading_purchase_amt = sum(h['purchase_sum'] for h in trading_holdings)
        trading_eval_amt = sum(h['eval_sum'] for h in trading_holdings) - cash_amt
        trading_profit_amt = trading_eval_amt - trading_purchase_amt
        trading_profit_rate = (trading_profit_amt / trading_purchase_amt * 100) if trading_purchase_amt != 0 else 0.0

        # base(운용 base) = 현금 + 트레이딩평가금액
        trading_base = cash_amt + trading_eval_amt
        trading_cash_ratio = (cash_amt / trading_base * 100) if trading_base > 0 else 0.0
        trading_eval_ratio = (trading_eval_amt / trading_base * 100) if trading_base > 0 else 0.0

        st.subheader("📊 트레이딩")
        col07, col08, col09 = st.columns(3)
        col07.metric("매입금액", f"{trading_purchase_amt:,.0f}원")
        col08.metric("평가금액", f"{trading_eval_amt:,.0f}원")
        col09.metric("손익금액", f"{trading_profit_amt:,.0f}원", delta=f"{trading_profit_rate:+.2f}%")
        col10, col11, col12 = st.columns(3)
        col10.metric("트레이딩 총 금액", f"{trading_base:,.0f}원")
        col11.metric("현금", f"{cash_amt:,.0f}원")
        col12.metric("현금(비중)", f"{trading_cash_ratio:.2f}%")

        # 트레이딩 종목별 상세 (총 집계 정보의 종목별 그리드/도넛과 동일한 패턴)
        data_trading = []
        for h in trading_holdings:
            trading_pfls_amt = h['eval_sum'] - h['purchase_sum']
            trading_pfls_rt = (trading_pfls_amt / h['purchase_sum'] * 100) if h['purchase_sum'] != 0 else 0.0
            data_trading.append({
                '코드': h['code'],
                '종목명': h['name'],
                '매입단가': h['purchase_price'],
                '매입수량': h['purchase_amount'],
                '매입금액': h['purchase_sum'],
                '현재가': h['current_price'],
                '평가금액': h['eval_sum'],
                '손익률(%)': trading_pfls_rt,
                '손익금액': trading_pfls_amt,
            })

        df_trading = pd.DataFrame(data_trading)

        if not df_trading.empty:
            # 평가금액 기준 비중 계산
            df_trading['비중(%)'] = df_trading['평가금액'] / df_trading['평가금액'].sum() * 100

            # 비중 순으로 정렬
            df_trading.sort_values(by='비중(%)', ascending=False, inplace=True)

            # 순서 컬럼 추가 (1부터 시작)
            df_trading.insert(0, '순서', range(1, len(df_trading) + 1))

            df_display = df_trading.copy().reset_index(drop=True)

            # Grid 옵션 생성
            gb = GridOptionsBuilder.from_dataframe(df_display)
            # 코드 컬럼 숨기기
            gb.configure_column('코드', hide=True)
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

            df_pie = df_trading[df_trading['평가금액'] > 0].copy()

            # 레이블 생성: 비중 종목명 (매입가, 손익률) [매도대상 시 수량 표기]
            def format_trading_label(row):
                if row['종목명'] == '현금':
                    return f"{row['비중(%)']:.1f}% {row['종목명']}"
                else:
                    profit_rate = f"{row['손익률(%)']:+.2f}%"
                    return f"{row['비중(%)']:.1f}% {row['종목명']} (매입가 {row['매입단가']:,.0f}원, 손익률 {profit_rate}, {row['매입수량']:,.0f}주)"

            df_pie['라벨'] = df_pie.apply(format_trading_label, axis=1)
            df_pie['custom_평가금액'] = df_pie['평가금액'].apply(lambda x: f"{x:,.0f}원")

            df_pie.sort_values(by='비중(%)', ascending=False, inplace=True)

            # 도넛 차트 생성 (매도대상 종목은 다른 색으로 표시)
            trading_fig = go.Figure(
                data=[go.Pie(
                    labels=df_pie['라벨'],
                    values=df_pie['평가금액'],
                    hole=0.4,
                    customdata=df_pie['custom_평가금액'],
                    hovertemplate='<b>%{label}</b><br><span style="color:red">평가금액: %{customdata[0]}</span><extra></extra>'
                )]
            )

            trading_fig.update_layout(title='트레이딩 평가금액 비율')

            # Streamlit에 출력
            st.plotly_chart(trading_fig)

        # 트레이딩매입금액 비율이 시장비율을 초과하면 매도 대상/수량 산정
        # (simul/kis_market_ratio_rebalance.py의 build_rebalance_orders, allocate 참조)
        SELL_COLOR = '#d03b3b'
        HOLD_COLOR = '#2a78d6'
        sell_qty_map = {}

        if trading_eval_ratio > market_ratio and trading_holdings:
            try:
                strength_fn = _make_strength_fn(
                    {'access_token': access_token, 'app_key': app_key, 'app_secret': app_secret}, {}
                )
                quality_fn = lambda code_: 50.0  # 참고용 항목으로 매도 우선순위 산정에서는 제외

                rebal_holdings = [dict(h) for h in trading_holdings if h['eval_sum'] > 0 and h['code'] != '']
                orders, excess = build_rebalance_orders(
                    rebal_holdings, cash_amt, market_ratio, strength_fn, quality_fn
                )
                sell_qty_map = {h['code']: qty for h, qty in orders if qty > 0}

                if sell_qty_map:
                    st.caption(
                        f"⚠️ 트레이딩 평가금액 비율({trading_eval_ratio:.1f}%)이 시장비율({market_ratio}%)을 "
                        f"초과 {len(sell_qty_map)}개 종목 매도 대상(초과금액 {excess:,.0f}원)"
                    )
            except Exception as e:
                st.warning(f"매도 대상 산정 중 오류가 발생했습니다: {e}")

        sell_target_holdings = [h for h in trading_holdings if h['code'] != '']

        if sell_target_holdings:
            # 클릭 매도용 차트 - Plotly Pie(도넛)는 st.plotly_chart(on_select=...) 클릭 선택이
            # 동작하지 않아(Plotly/Streamlit 자체 제약, 실측 확인) 가로 막대 차트로 표시한다.
            trading_labels, trading_values, trading_colors, trading_texts = [], [], [], []
            for h in sell_target_holdings:
                if h['code'] in sell_qty_map:
                    trading_labels.append(f"{h['name']} (매도대상 {sell_qty_map[h['code']]:,}주)")
                    trading_colors.append(SELL_COLOR)
                else:
                    trading_labels.append(h['name'])
                    trading_colors.append(HOLD_COLOR)
                trading_values.append(h['eval_sum'])
                trading_texts.append(f"{h['eval_sum']:,.0f}원")

            trading_fig = go.Figure(
                data=[go.Bar(
                    x=trading_values,
                    y=trading_labels,
                    orientation='h',
                    marker=dict(color=trading_colors),
                    text=trading_texts,
                    textposition='outside',
                    cliponaxis=False,
                    hovertemplate='<b>%{y}</b><br>평가금액: %{x:,.0f}원<extra></extra>'
                )]
            )
            trading_fig.update_layout(
                title='시장비율 초과 매도종목 (막대를 클릭하여 매도)',
                xaxis=dict(title='평가금액(원)'),
                yaxis=dict(autorange='reversed'),
                showlegend=False,
            )

            sell_click_event = st.plotly_chart(
                trading_fig, on_select="rerun", selection_mode="points", key="sell_target_chart"
            )

            clicked_points = sell_click_event.selection.points if sell_click_event and sell_click_event.selection else []

            if clicked_points:
                clicked_h = sell_target_holdings[clicked_points[0]['point_index']]

                if clicked_h['code'] not in sell_qty_map:
                    st.info(f"{clicked_h['name']}은(는) 매도 대상이 아닙니다. (매도 대상 종목만 클릭해서 매도할 수 있습니다)")
                else:
                    sell_qty = sell_qty_map[clicked_h['code']]
                    est_amt = sell_qty * clicked_h['current_price']

                    if st.session_state.get('sell_click_executed') == clicked_h['code']:
                        st.info(f"{clicked_h['name']}[{clicked_h['code']}]은(는) 이미 매도 주문이 접수되었습니다. 다른 종목을 선택해주세요.")
                    else:
                        st.warning(
                            f"선택: **{clicked_h['name']}[{clicked_h['code']}]** {sell_qty:,}주 시장가 매도 "
                            f"(현재가 {clicked_h['current_price']:,}원, 예상 매도금액 {est_amt:,}원)"
                        )
                        # (simul/kis_market_ratio_rebalance.py의 order_cash/record_sell 호출 부분 참조)
                        if st.button(f"⚠️ {clicked_h['name']} {sell_qty:,}주 매도 실행", key=f"sell_confirm_{clicked_h['code']}"):
                            tag = f"{clicked_h['name']}[{clicked_h['code']}] {sell_qty}주"
                            ar = order_cash(False, access_token, app_key, app_secret,
                                            str(acct_no), clicked_h["code"], "01", sell_qty, 0, excg_id="KRX")
                            if ar.isOK():
                                out = ar.getBody().output
                                order_no = (out or {}).get("ODNO", "")
                                st.success(f"✅ 매도접수 {tag} 주문번호={str(int(order_no))}")
                                record_sell(kis_conn, acct_no, clicked_h, sell_qty, clicked_h["current_price"], order_no, "Bar")
                                st.session_state['sell_click_executed'] = clicked_h['code']
                            else:
                                st.error(f"❌ 매도실패 {tag}: {ar.getErrorCode()} {ar.getErrorMessage()}")

        # 추가2) 투자매입금액 (trading_plan = 'i', proc_yn = 'Y')
        data_invest = []
        for name_, pur_price_, pur_amt_, pur_sum_, cur_price_, eval_sum_ in invest_rows:
            invest_pur_sum = float(pur_sum_ or 0)
            invest_eval_sum = float(eval_sum_ or 0)
            invest_pfls_amt = invest_eval_sum - invest_pur_sum
            invest_pfls_rt = (invest_pfls_amt / invest_pur_sum * 100) if invest_pur_sum != 0 else 0.0
            data_invest.append({
                '종목명': name_,
                '매입단가': int(float(pur_price_ or 0)),
                '매입수량': float(pur_amt_ or 0),
                '매입금액': invest_pur_sum,
                '현재가': int(float(cur_price_ or 0)),
                '평가금액': invest_eval_sum,
                '손익률(%)': invest_pfls_rt,
                '손익금액': invest_pfls_amt,
            })

        df_invest = pd.DataFrame(data_invest)

        invest_purchase_amt = df_invest['매입금액'].sum() if not df_invest.empty else 0.0
        invest_eval_amt = df_invest['평가금액'].sum() if not df_invest.empty else 0.0
        invest_profit_amt = invest_eval_amt - invest_purchase_amt
        invest_profit_rate = (invest_profit_amt / invest_purchase_amt * 100) if invest_purchase_amt != 0 else 0.0

        st.subheader("📊 투자")
        col13, col14, col15 = st.columns(3)
        col13.metric("매입금액", f"{invest_purchase_amt:,.0f}원")
        col14.metric("평가금액", f"{invest_eval_amt:,.0f}원")
        col15.metric("손익금액", f"{invest_profit_amt:,.0f}원", delta=f"{invest_profit_rate:+.2f}%")

        if not df_invest.empty:
            # 평가금액 기준 비중 계산
            df_invest['비중(%)'] = df_invest['평가금액'] / df_invest['평가금액'].sum() * 100

            # 비중 순으로 정렬
            df_invest.sort_values(by='비중(%)', ascending=False, inplace=True)

            # 순서 컬럼 추가 (1부터 시작)
            df_invest.insert(0, '순서', range(1, len(df_invest) + 1))

            df_display = df_invest.copy().reset_index(drop=True)

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

            df_pie = df_invest[df_invest['평가금액'] > 0].copy()

            # 레이블 생성: 비중 종목명 (매입가, 손익률)
            def format_invest_label(row):
                profit_rate = f"{row['손익률(%)']:+.2f}%"
                return f"{row['비중(%)']:.1f}% {row['종목명']} (매입가 {row['매입단가']:,.0f}원, 손익률 {profit_rate}, {row['매입수량']:,.0f}주)"

            df_pie['라벨'] = df_pie.apply(format_invest_label, axis=1)
            df_pie['custom_평가금액'] = df_pie['평가금액'].apply(lambda x: f"{x:,.0f}원")

            df_pie.sort_values(by='비중(%)', ascending=False, inplace=True)

            # 도넛 차트 생성
            invest_fig = go.Figure(
                data=[go.Pie(
                    labels=df_pie['라벨'],
                    values=df_pie['평가금액'],
                    hole=0.4,
                    customdata=df_pie['custom_평가금액'],
                    hovertemplate='<b>%{label}</b><br><span style="color:red">평가금액: %{customdata[0]}</span><extra></extra>'
                )]
            )

            invest_fig.update_layout(title='투자 평가금액 비율')

            # Streamlit에 출력
            st.plotly_chart(invest_fig)

        st.subheader("📊 총 집계 정보")
        col1, col2, col3 = st.columns(3)
        col1.metric("총 금액", f"{total_amt:,.0f}원")
        col2.metric("현금", f"{cash_amt:,.0f}원")
        col3.metric("시장비율", f"{market_ratio}/100", delta=f"{market_ratio - 50:+d}")
        col4, col5, col6 = st.columns(3)
        col4.metric("총 매입금액", f"{total_hold_amt:,.0f}원")
        col5.metric("총 평가금액", f"{total_eval_amt:,.0f}원")
        col6.metric("총 손익금액", f"{total_profit_amt:,.0f}원", delta=f"{profit_rate:+.2f}%")
        
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
                return f"{row['비중(%)']:.1f}% {row['종목명']}"
            else:
                profit_rate = f"{row['손익률(%)']:+.2f}%"
                return f"{row['비중(%)']:.1f}% {row['종목명']} (매입가 {row['매입단가']:,.0f}원, 손익률 {profit_rate}, {row['매입수량']:,.0f}주)"

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

# strt_dt = (st.date_input("시작일", datetime.today() - timedelta(days=30))).strftime("%Y%m%d")
strt_dt = (st.date_input("시작일", datetime(2026, 1, 1), key="strt_dt_profit")).strftime("%Y%m%d")
end_dt = (st.date_input("종료일", datetime.today(), key="end_dt_profit")).strftime("%Y%m%d")

# 기간별 총평가
cur05 = kis_conn.cursor()
period_profit_sum = """
    WITH base AS (
        SELECT
            AA.dt,
            AA.tot_evlu_amt,
            AA.evlu_pfls_amt,
            AA.prvs_excc_amt                                                AS 현금,
            (SELECT SUM((A.order_price::numeric - A.hold_price::numeric) * A.total_complete_qty::int)::int
             FROM public."stockOrderComplete_stock_order_complete" A
             WHERE A.acct_no = AA.acct::int
               AND A.order_dt = AA.dt
               AND A.order_type LIKE '%%매도%%'
               AND A.total_complete_qty::int > 0
            )                                                               AS 손수익,
            (SELECT SUM((A.order_price::numeric - A.hold_price::numeric) * A.total_complete_qty::int)::int
             FROM public."stockOrderComplete_stock_order_complete" A
             INNER JOIN public.trading_trail B ON A.acct_no = B.acct_no AND A.name = B.name AND A.order_dt = B.trail_day AND B.trail_tp = '3'
             WHERE A.acct_no = AA.acct::int
               AND A.order_dt = AA.dt
               AND A.order_type LIKE '%%매도%%'
               AND A.total_complete_qty::int > 0
            )                                                               AS 안전마진,
            (SELECT SUM((A.order_price::numeric - A.hold_price::numeric) * A.total_complete_qty::int)::int
             FROM public."stockOrderComplete_stock_order_complete" A
             INNER JOIN public.trading_trail B ON A.acct_no = B.acct_no AND A.name = B.name AND A.order_dt = B.trail_day AND B.trail_tp = '4'
             WHERE A.acct_no = AA.acct::int
               AND A.order_dt = AA.dt
               AND A.order_type LIKE '%%매도%%'
               AND A.total_complete_qty::int > 0
            )                                                               AS 전량매도,
            (SELECT SUM(B.order_price::numeric * B.total_complete_qty::int)::int
             FROM public."stockOrderComplete_stock_order_complete" B
             WHERE B.acct_no = AA.acct::int
               AND B.order_dt = AA.dt
               AND B.order_type LIKE '%%매수%%'
               AND B.total_complete_qty::int > 0
            )                                                               AS 매수총액,
            (SELECT SUM(C.order_price::numeric * C.total_complete_qty::int)::int
             FROM public."stockOrderComplete_stock_order_complete" C
             WHERE C.acct_no = AA.acct::int
               AND C.order_dt = AA.dt
               AND C.order_type LIKE '%%매도%%'
               AND C.total_complete_qty::int > 0
            )                                                               AS 매도총액,
            (SELECT SUM(purchase_amt) FROM public.dly_stock_balance D
             WHERE D.dt = AA.dt AND D.acct = AA.acct
               AND (D.trading_plan IS NULL OR D.trading_plan NOT IN ('i'))
            )                                                               AS 트레이딩매입액,
            (SELECT SUM(eval_sum) FROM public.dly_stock_balance D
             WHERE D.dt = AA.dt AND D.acct = AA.acct
               AND (D.trading_plan IS NULL OR D.trading_plan NOT IN ('i'))
            )                                                               AS 트레이딩평가액,
            (SELECT SUM(purchase_amt) FROM public.dly_stock_balance D
             WHERE D.dt = AA.dt AND D.acct = AA.acct AND D.trading_plan = 'i'
            )                                                               AS 투자매입액,
            (SELECT SUM(eval_sum) FROM public.dly_stock_balance D
             WHERE D.dt = AA.dt AND D.acct = AA.acct AND D.trading_plan = 'i'
            )                                                               AS 투자평가액
        FROM public.dly_acct_balance AA
        WHERE AA.dt BETWEEN %s AND %s
          AND AA.acct = %s

        UNION ALL

        SELECT
            TO_CHAR(CURRENT_DATE, 'yyyymmdd') 								AS dt,
            AA.tot_evlu_amt,
            (SELECT SUM(eval_sum - purchase_sum) FROM public."stockBalance_stock_balance" D
             WHERE D.proc_yn = 'Y' AND D.acct_no = AA.acct_no
               AND (D.trading_plan IS NULL OR D.trading_plan NOT IN ('i', 'h'))
            )                                                               AS evlu_pfls_amt,
            AA.prvs_rcdl_excc_amt                                           AS 현금,
            (SELECT SUM((A.order_price::numeric - A.hold_price::numeric) * A.total_complete_qty::int)::int
             FROM public."stockOrderComplete_stock_order_complete" A
             WHERE A.acct_no = AA.acct_no
               AND A.order_dt = prev_business_day_char(CURRENT_DATE)
               AND A.order_type LIKE '%%매도%%'
               AND A.total_complete_qty::int > 0
            )                                                               AS 손수익,
            (SELECT SUM((A.order_price::numeric - A.hold_price::numeric) * A.total_complete_qty::int)::int
             FROM public."stockOrderComplete_stock_order_complete" A
             INNER JOIN public.trading_trail B ON A.acct_no = B.acct_no AND A.name = B.name AND A.order_dt = B.trail_day AND B.trail_tp = '3'
             WHERE A.acct_no = AA.acct_no
               AND A.order_dt = prev_business_day_char(CURRENT_DATE)
               AND A.order_type LIKE '%%매도%%'
               AND A.total_complete_qty::int > 0
            )                                                               AS 안전마진,
            (SELECT SUM((A.order_price::numeric - A.hold_price::numeric) * A.total_complete_qty::int)::int
             FROM public."stockOrderComplete_stock_order_complete" A
             INNER JOIN public.trading_trail B ON A.acct_no = B.acct_no AND A.name = B.name AND A.order_dt = B.trail_day AND B.trail_tp = '4'
             WHERE A.acct_no = AA.acct_no
               AND A.order_dt = prev_business_day_char(CURRENT_DATE)
               AND A.order_type LIKE '%%매도%%'
               AND A.total_complete_qty::int > 0
            )                                                               AS 전량매도,
            (SELECT SUM(B.order_price::numeric * B.total_complete_qty::int)::int
             FROM public."stockOrderComplete_stock_order_complete" B
             WHERE B.acct_no = AA.acct_no
               AND B.order_dt = prev_business_day_char(CURRENT_DATE)
               AND B.order_type LIKE '%%매수%%'
               AND B.total_complete_qty::int > 0
            )                                                               AS 매수총액,
            (SELECT SUM(C.order_price::numeric * C.total_complete_qty::int)::int
             FROM public."stockOrderComplete_stock_order_complete" C
             WHERE C.acct_no = AA.acct_no
               AND C.order_dt = prev_business_day_char(CURRENT_DATE)
               AND C.order_type LIKE '%%매도%%'
               AND C.total_complete_qty::int > 0
            )                                                               AS 매도총액,
            (SELECT SUM(purchase_sum) FROM public."stockBalance_stock_balance" D
             WHERE D.proc_yn = 'Y' AND D.acct_no = AA.acct_no
               AND (D.trading_plan IS NULL OR D.trading_plan NOT IN ('i'))
            )                                                               AS 트레이딩매입액,
            (SELECT SUM(eval_sum) FROM public."stockBalance_stock_balance" D
             WHERE D.proc_yn = 'Y' AND D.acct_no = AA.acct_no
               AND (D.trading_plan IS NULL OR D.trading_plan NOT IN ('i'))
            )                                                               AS 트레이딩평가액,
            (SELECT SUM(purchase_sum) FROM public."stockBalance_stock_balance" D
             WHERE D.proc_yn = 'Y' AND D.acct_no = AA.acct_no AND D.trading_plan = 'i'
            )                                                               AS 투자매입액,
            (SELECT SUM(eval_sum) FROM public."stockBalance_stock_balance" D
             WHERE D.proc_yn = 'Y' AND D.acct_no = AA.acct_no AND D.trading_plan = 'i'
            )                                                               AS 투자평가액
        FROM public."stockFundMng_stock_fund_mng" AA
        WHERE AA.acct_no = %s
    ),
    ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (ORDER BY dt DESC) AS rn
        FROM base
    )
    SELECT
        dt,
        tot_evlu_amt,
        evlu_pfls_amt,
        현금,
        COALESCE(손수익, 0)                                                 AS 손수익,
        COALESCE(안전마진, 0)                                               AS 안전마진,
        COALESCE(전량매도, 0)                                               AS 전량매도,
        COALESCE(매수총액, 0)                                               AS 매수총액,
        COALESCE(매도총액, 0)                                               AS 매도총액,
        COALESCE(트레이딩매입액, 0)                                           AS 트레이딩매입액,
        COALESCE(트레이딩평가액, 0)                                           AS 트레이딩평가액,
        COALESCE(투자매입액, 0)                                               AS 투자매입액,
        COALESCE(투자평가액, 0)                                               AS 투자평가액,
        ROUND(현금::numeric / NULLIF(tot_evlu_amt, 0) * 100, 2)            AS "현금비율",
        ROUND(COALESCE(트레이딩평가액, 0)::numeric / NULLIF(tot_evlu_amt, 0) * 100, 2) AS "트레이딩비율",
        ROUND(COALESCE(투자평가액, 0)::numeric / NULLIF(tot_evlu_amt, 0) * 100, 2)     AS "투자비율",
        SUM(COALESCE(손수익, 0)) OVER (
            ORDER BY rn DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                                   AS 누적수익합계
    FROM ranked
    ORDER BY dt DESC
"""

cur05.execute(period_profit_sum, (strt_dt, end_dt, str(acct_no), str(acct_no)))
result_five = cur05.fetchall()
cur05.close()

data03 = []
for item in result_five:
    data03.append({
        '일자':         item[0],
        '총평가금액':   float(item[1]) if item[1] is not None else 0.0,
        '평가손익':     float(item[2]) if item[2] is not None else 0.0,
        '현금':         float(item[3]) if item[3] is not None else 0.0,
        '손수익':       float(item[4]) if item[4] is not None else 0.0,
        '안전마진':     float(item[5]) if item[5] is not None else 0.0,
        '전량매도':     float(item[6]) if item[6] is not None else 0.0,
        '매수총액':     float(item[7]) if item[7] is not None else 0.0,
        '매도총액':     float(item[8]) if item[8] is not None else 0.0,
        '트레이딩매입액': float(item[9]) if item[9] is not None else 0.0,
        '트레이딩평가액': float(item[10]) if item[10] is not None else 0.0,
        '투자매입액':   float(item[11]) if item[11] is not None else 0.0,
        '투자평가액':   float(item[12]) if item[12] is not None else 0.0,
        '현금비율(%)':  float(item[13]) if item[13] is not None else 0.0,
        '트레이딩비율(%)': float(item[14]) if item[14] is not None else 0.0,
        '투자비율(%)':  float(item[15]) if item[15] is not None else 0.0,
        '누적수익합계': float(item[16]) if item[16] is not None else 0.0,
    })

df03 = pd.DataFrame(data03)

if df03.empty:
    st.warning("기간별 총평가 조회된 데이터가 없습니다. 조건을 확인해주세요.")
else:
    st.title("기간별 총평가")

    df03['일자'] = pd.to_datetime(df03['일자']).dt.strftime('%Y-%m-%d')

    if st.button('기간별 충평가 상세 데이터'):

        df_display = df03.sort_values(by='일자', ascending=False).copy().reset_index(drop=True)

        gb = GridOptionsBuilder.from_dataframe(df_display)
        gb.configure_pagination(enabled=True, paginationPageSize=20)
        gb.configure_grid_options(domLayout='normal')
        gb.configure_grid_options(enableRangeSelection=True)
        gb.configure_grid_options(enableExcelExport=True)

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
            '일자':         80,
            '총평가금액':   110,
            '평가손익':     100,
            '현금':         100,
            '손수익':       100,
            '안전마진':     100,
            '전량매도':     100,
            '매수총액':     100,
            '매도총액':     100,
            '트레이딩매입액': 110,
            '트레이딩평가액': 110,
            '투자매입액':   100,
            '투자평가액':   100,
            '현금비율(%)':      80,
            '트레이딩비율(%)':  90,
            '투자비율(%)':      80,
            '누적수익합계': 110,
        }

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

        percent_cols = ['현금비율(%)', '트레이딩비율(%)', '투자비율(%)']
        number_cols  = ['총평가금액', '평가손익', '현금', '손수익', '안전마진', '전량매도', '매수총액', '매도총액',
                        '트레이딩매입액', '트레이딩평가액', '투자매입액', '투자평가액', '누적수익합계']

        for col, width in column_widths.items():
            if col in percent_cols:
                gb.configure_column(col, type=['numericColumn'], cellRenderer=percent_format_js, width=width)
            elif col in number_cols:
                gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js, width=width)
            else:
                gb.configure_column(col, width=width)

        grid_options = gb.build()

        AgGrid(
            df_display,
            gridOptions=grid_options,
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,
            use_container_width=True,
            update_mode=GridUpdateMode.NO_UPDATE,
            enable_enterprise_modules=True,
            excel_export_mode='xlsx'
        )

    df03['일자'] = pd.to_datetime(df03['일자'])
    df03 = df03.dropna(subset=['일자'])
    df03 = df03.sort_values(by='일자')
    df03['일자_str'] = df03['일자'].dt.strftime('%Y-%m-%d')
    df03.set_index('일자_str', inplace=True)

    # 총평가금액(현금+트레이딩평가액+투자평가액 스택 영역)을 중심으로, 평가손익·누적수익합계는 별도 패널에 표시
    x03 = df03['일자']

    fig03 = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
        row_heights=[0.62, 0.38],
        subplot_titles=('총평가금액(현금, 트레이딩, 투자)', '평가손익 · 누적수익합계')
    )

    fig03.add_trace(go.Scatter(
        x=x03, y=df03['현금'], name='현금', mode='lines',
        stackgroup='total', fillcolor='rgba(42,120,214,0.55)',
        line=dict(width=0.5, color='#2a78d6'),
        hovertemplate='현금: %{y:,.0f}원<extra></extra>'
    ), row=1, col=1)

    fig03.add_trace(go.Scatter(
        x=x03, y=df03['트레이딩평가액'], name='트레이딩평가액', mode='lines',
        stackgroup='total', fillcolor='rgba(0,131,0,0.45)',
        line=dict(width=0.5, color='#008300'),
        hovertemplate='트레이딩평가액: %{y:,.0f}원<extra></extra>'
    ), row=1, col=1)

    fig03.add_trace(go.Scatter(
        x=x03, y=df03['투자평가액'], name='투자평가액', mode='lines',
        stackgroup='total', fillcolor='rgba(27,175,122,0.45)',
        line=dict(width=0.5, color='#1baf7a'),
        hovertemplate='투자평가액: %{y:,.0f}원<extra></extra>'
    ), row=1, col=1)

    fig03.add_trace(go.Scatter(
        x=x03, y=df03['총평가금액'], name='총평가금액', mode='lines',
        line=dict(width=2.5, color='#0b0b0b', dash='dot'),
        hovertemplate='총평가금액: %{y:,.0f}원<extra></extra>'
    ), row=1, col=1)

    fig03.add_trace(go.Scatter(
        x=x03, y=df03['평가손익'], name='평가손익', mode='lines',
        line=dict(width=2, color='#eb6834'),
        hovertemplate='평가손익: %{y:,.0f}원<extra></extra>'
    ), row=2, col=1)

    fig03.add_trace(go.Scatter(
        x=x03, y=df03['누적수익합계'], name='누적수익합계', mode='lines',
        line=dict(width=2, color='#4a3aa7'),
        hovertemplate='누적수익합계: %{y:,.0f}원<extra></extra>'
    ), row=2, col=1)

    fig03.update_layout(
        height=650,
        hovermode='x unified',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )
    fig03.update_yaxes(title_text='원', tickformat=',', row=1, col=1)
    fig03.update_yaxes(title_text='원', tickformat=',', row=2, col=1,
                        zeroline=True, zerolinewidth=1, zerolinecolor='#898781')

    st.plotly_chart(fig03, use_container_width=True)

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
        st.title("KIS 손익 합산 조회")

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
                '거래소': item['excg_id_dvsn_cd'],
            })

    df4 = pd.DataFrame(data4)

    if df4.empty:
        st.warning("일별주문체결조회된 데이터가 없습니다. 조건을 확인해주세요.")
    else:
        # Streamlit 앱 구성
        st.title("KIS 일별 주문체결 조회")

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
            '거래소': 50,
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
