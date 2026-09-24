import psycopg2 as db
from datetime import datetime
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import JsCode

# PostgreSQL 연결 설정
# conn_string = "dbname='universe' host='localhost' port='5432' user='postgres' password='asdf1234'"
conn_string = "dbname='universe' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"
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
    print("계좌현황 결과가 없습니다.")
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
        st.title("계좌현황")
        
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
    st.title("기간별 총평가")

    df01['일자'] = pd.to_datetime(df01['일자']).dt.strftime('%Y-%m-%d')

    # 버튼을 클릭하면, 데이터프레임이 보이도록 만들기.
    if st.button('기간별 총평가 상세 데이터'):

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

