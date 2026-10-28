import psycopg2 as db
from datetime import datetime
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import JsCode

# 편집 가능한 AG-Grid 하단 상태바의 [변경 적용] 버튼.
# 셀 편집만으로는 Streamlit 재실행이 일어나지 않도록 update_on=['filterChanged'] 만 사용하고,
# 버튼 클릭 시 편집 확정 + 숨김 컬럼(_apply_nonce)에 클릭 시각 기록 + filterChanged 발생으로 한 번만 데이터를 반환한다.
APPLY_EDITS_PANEL_JS = JsCode("""
class ApplyEditsPanel {
    init(params) {
        this.eGui = document.createElement('div');
        this.eGui.style.padding = '4px 0';
        const btn = document.createElement('button');
        btn.innerText = '변경 적용';
        btn.style.cssText = 'padding:4px 14px;background:#ff4b4b;color:#fff;border:none;border-radius:4px;cursor:pointer;font-weight:600;';
        btn.addEventListener('click', () => {
            const api = params.api;
            api.stopEditing();
            let first = null;
            api.forEachNode(n => { if (!first) { first = n; } });
            if (first) { first.setDataValue('_apply_nonce', Date.now()); }
            api.onFilterChanged();
        });
        this.eGui.appendChild(btn);
    }
    getGui() { return this.eGui; }
}
""")


def is_new_apply_request(edited_df, state_key):
    """그리드의 [변경 적용] 버튼 클릭으로 들어온, 아직 처리하지 않은 요청인지 판단 (클릭 시각 nonce 비교)"""
    if edited_df.empty or '_apply_nonce' not in edited_df.columns:
        return False
    nonce = int(pd.to_numeric(edited_df['_apply_nonce'], errors='coerce').fillna(0).max())
    if nonce <= 0 or st.session_state.get(state_key) == nonce:
        return False
    st.session_state[state_key] = nonce
    return True

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
        A.chg_date,
        A.action_price,
        A.stop_price,
        A.trading_plan,
        A.exit_price
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
            '수행가': float(item[20]) if item[20] is not None else 0.0,
            '이탈가': float(item[21]) if item[21] is not None else 0.0,
            '최종이탈가': float(item[23]) if item[23] is not None else 0.0,
            'trading_plan': item[22],
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

        # 수행가/이탈가/최종이탈가 (balance_info 컬럼, 편집 시 같은 시장 모든 계좌의 해당 종목에 반영)
        price_cols = {
            '수행가': 'action_price',
            '이탈가': 'stop_price',
            '최종이탈가': 'exit_price',
        }

        # 매매계획 (trading_plan): 일반=NULL, 투자='i', 홀딩='h'
        # 현금(KRW-KRW) 행은 '' 로 표시하고 편집 불가
        plan_labels = {None: '일반', 'i': '투자', 'h': '홀딩'}
        plan_values = {v: k for k, v in plan_labels.items()}
        df0.insert(df0.columns.get_loc('종목명') + 1, '매매계획', [
            '' if prd == 'KRW-KRW' else plan_labels.get(plan or None, plan)
            for prd, plan in zip(df0['종목명'], df0['trading_plan'])
        ])
        df0.drop(columns=['trading_plan'], inplace=True)

        # 현재가가 존재하는 종목은 손익금액 내림차순, 현재가 없는 항목(현금 등)은 맨 아래
        df_display = pd.concat([
            df0[df0['현재가'] > 0].sort_values(by='손익금액', ascending=False),
            df0[~(df0['현재가'] > 0)],
        ]).reset_index(drop=True)

        # 종목명, 매매계획을 맨 앞 항목으로 배치
        df_display = df_display[['종목명', '매매계획'] + [c for c in df_display.columns if c not in ('종목명', '매매계획')]]
        df_display['_apply_nonce'] = 0.0

        # Grid 옵션 생성
        gb = GridOptionsBuilder.from_dataframe(df_display)
        gb.configure_column('_apply_nonce', hide=True)
        gb.configure_grid_options(statusBar={'statusPanels': [{'statusPanel': APPLY_EDITS_PANEL_JS, 'align': 'left'}]})
        # 페이지당 20개 표시
        gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)
        gb.configure_grid_options(domLayout='normal')
        # Excel 다운로드를 위한 옵션 추가
        gb.configure_grid_options(enableRangeSelection=True)
        gb.configure_grid_options(enableExcelExport=True)

        # 각 컬럼을 셀 내용·컬럼명이 모두 보이는 너비로 자동 맞춤
        # 화면 밖 컬럼도 측정되도록 컬럼 가상화를 끄고, 데이터 렌더링 후 전체 컬럼 자동 크기 재적용
        auto_size_all_js = JsCode("""
            function(params) { params.api.autoSizeAllColumns(false); }
        """)
        gb.configure_grid_options(
            autoSizeStrategy={'type': 'fitCellContents', 'skipHeader': False},
            suppressColumnVirtualisation=True,
            onFirstDataRendered=auto_size_all_js,
            onPaginationChanged=auto_size_all_js,
        )

        column_list = ['매입단가', '매입수량', '매입금액', '현재가', '평가금액', '손익률(%)', '손익금액', '비중(%)',
                       '수행가', '이탈가', '최종이탈가']

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

        # 현금(KRW-KRW) 행은 가격 항목 편집 불가
        price_editable_js = JsCode("""
            function(params) { return params.data['종목명'] !== 'KRW-KRW'; }
        """)

        # 현금(KRW-KRW) 행을 제외한 종목만 매매계획 편집 가능
        plan_editable_js = JsCode("""
            function(params) { return params.data['매매계획'] !== ''; }
        """)

        # 종목명은 좌측 고정 (가로 스크롤 시에도 표시)
        gb.configure_column('종목명', pinned='left')
        gb.configure_column('매매계획', editable=plan_editable_js, cellEditor='agSelectCellEditor',
                            cellEditorParams={'values': list(plan_values)},
                            singleClickEdit=True, cellStyle={'backgroundColor': '#fff8e1'})

        for col in column_list:
            if col in ['손익률(%)', '비중(%)']:
                gb.configure_column(col, type=['numericColumn'], cellRenderer=percent_format_js)
            elif col in price_cols:
                gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js,
                                    editable=price_editable_js, cellEditor='agNumberCellEditor',
                                    cellStyle={'backgroundColor': '#fff8e1'})
            else:
                gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js)

        grid_options = gb.build()

        if 'price_apply_msg' in st.session_state:
            st.success(st.session_state.pop('price_apply_msg'))

        st.caption("매매계획은 셀을 클릭해 콤보박스에서 선택하고, 수행가·이탈가·최종이탈가는 셀을 더블클릭해 수정한 뒤 "
                   f"그리드 하단 [변경 적용]을 누르면 {my_choice1} 모든 계좌의 해당 종목에 한 번에 반영됩니다.")

        # AgGrid를 통해 데이터 출력 (그리드 하단 [변경 적용] 버튼 클릭 시에만 편집값 반환)
        grid_response = AgGrid(
            df_display,
            gridOptions=grid_options,
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,
            use_container_width=True,
            update_mode=GridUpdateMode.NO_UPDATE,
            update_on=['filterChanged'],     # [변경 적용] 버튼이 발생시키는 이벤트에서만 값 반환
            enable_enterprise_modules=True,  # 엑셀 다운로드, 상태바 버튼 위해 필요
            excel_export_mode='xlsx',        # 엑셀(xlsx)로 다운로드
            key=f"total_grid_{my_choice1}_{my_choice2}",
        )

        # [변경 적용] 클릭으로 들어온 새 요청일 때만 편집 전/후 비교로 변경된 매매계획/가격 항목 수집
        # (종목명, 항목, DB컬럼, 변경 전 표시값, 변경 후 표시값, DB 저장값)
        edited_df = pd.DataFrame(grid_response['data'])
        price_changes = []
        apply_requested = is_new_apply_request(edited_df, f"apply_nonce_{my_choice1}_{my_choice2}")
        if apply_requested and '종목명' in edited_df.columns:
            orig_by_prd = df_display.set_index('종목명')
            for _, row in edited_df.iterrows():
                prd_ = row['종목명']
                if prd_ == 'KRW-KRW' or prd_ not in orig_by_prd.index:
                    continue
                old_plan, new_plan = orig_by_prd.at[prd_, '매매계획'], row['매매계획']
                if old_plan != '' and new_plan in plan_values and new_plan != old_plan:
                    price_changes.append((prd_, '매매계획', 'trading_plan', old_plan, new_plan, plan_values[new_plan]))
                for label, db_col in price_cols.items():
                    try:
                        new_val = float(row[label])
                    except (TypeError, ValueError):
                        continue
                    old_val = float(orig_by_prd.at[prd_, label])
                    if abs(new_val - old_val) > 1e-9:
                        price_changes.append((prd_, label, db_col, f"{old_val:,}", f"{new_val:,}", new_val))

        if apply_requested and not price_changes:
            st.info("변경된 항목이 없습니다.")
        elif price_changes:
            cur_upd = conn.cursor()
            try:
                applied = []
                for prd_, label, db_col, old_disp, new_disp, new_val in price_changes:
                    # db_col은 price_cols / 'trading_plan' 화이트리스트 값만 사용
                    cur_upd.execute(f"""
                        UPDATE balance_info
                           SET {db_col} = %s, chg_date = now()
                         WHERE market_name = %s AND prd_nm = %s AND proc_yn = 'Y'
                    """, (new_val, my_choice1, prd_))
                    applied.append(f"{prd_} {label} {old_disp} → {new_disp} ({my_choice1} {cur_upd.rowcount}개 계좌)")
                conn.commit()
                st.session_state['price_apply_msg'] = "변경 완료\n\n" + "\n\n".join(applied)
            except Exception as e:
                conn.rollback()
                st.error(f"변경 실패: {e}")
            finally:
                cur_upd.close()
            if 'price_apply_msg' in st.session_state:
                st.rerun()

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

        # 주문유형 필터 ('전체' 선택 시 전체 주문유형)
        all_types = df4['주문유형'].unique()
        주문유형리스트 = [t for t in all_types if t in ('매수', '매도')]
        선택주문유형 = st.selectbox("주문유형을 선택하세요", ['전체'] + 주문유형리스트, key='crypto_order_type')
        선택주문유형목록 = 주문유형리스트 if 선택주문유형 == '전체' else [선택주문유형]

        # 주문상태 필터 ('전체' 선택 시 전체 주문상태)
        all_states = df4['주문상태'].unique()
        주문상태리스트 = [t for t in all_states if t in ('done', 'cancel', 'wait')]
        선택주문상태 = st.selectbox("주문상태를 선택하세요", ['전체'] + 주문상태리스트, key='crypto_order_state')
        선택주문상태목록 = 주문상태리스트 if 선택주문상태 == '전체' else [선택주문상태]

        # 체결대상: 체결수량이 존재하는 주문만 조회
        체결대상 = st.checkbox("체결대상", key='crypto_ccld_only')

        # 선택한 주문유형 + 주문상태 (+ 체결대상)로 필터링
        선택필터_df = df4[
            (df4['주문유형'].isin(선택주문유형목록)) &
            (df4['주문상태'].isin(선택주문상태목록))
        ].copy()
        if 체결대상:
            선택필터_df = 선택필터_df[선택필터_df['체결수량'] > 0]

        # 날짜/시간 포맷 변경
        선택필터_df['주문일자'] = pd.to_datetime(선택필터_df['주문일자'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
        선택필터_df['주문시각'] = pd.to_datetime(선택필터_df['주문시각'], format='%H%M%S').dt.strftime('%H:%M:%S')

        # 최신 순 정렬
        df_display = 선택필터_df.sort_values(by=['주문일자', '주문시각'], ascending=False).reset_index(drop=True)

        # Grid 옵션 생성
        gb = GridOptionsBuilder.from_dataframe(df_display)
        # 원주문번호 컬럼 숨기기
        gb.configure_column('원주문번호', hide=True)
        # 페이지당 20개 표시
        gb.configure_pagination(enabled=True, paginationPageSize=20)
        gb.configure_grid_options(domLayout='normal')
        # Excel 다운로드를 위한 옵션 추가
        gb.configure_grid_options(enableRangeSelection=True)
        gb.configure_grid_options(enableExcelExport=True)

        # 각 컬럼을 셀 내용·컬럼명이 모두 보이는 너비로 자동 맞춤
        # 화면 밖 컬럼도 측정되도록 컬럼 가상화를 끄고, 데이터 렌더링/페이지 이동 후 전체 컬럼 자동 크기 재적용
        auto_size_all_js = JsCode("""
            function(params) { params.api.autoSizeAllColumns(false); }
        """)
        gb.configure_grid_options(
            autoSizeStrategy={'type': 'fitCellContents', 'skipHeader': False},
            suppressColumnVirtualisation=True,
            onFirstDataRendered=auto_size_all_js,
            onPaginationChanged=auto_size_all_js,
        )

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
        for col in ['보유단가', '보유수량', '주문단가', '주문수량', '체결수량', '잔여수량', '주문금액', '체결금액', '수수료', '손실수익금']:
            gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js)

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

