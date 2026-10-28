import psycopg2 as db
import json
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import JsCode
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# PostgreSQL 연결 설정
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"

# DB 연결
conn = db.connect(conn_string)

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


# 종목코드/종목명 셀 클릭 시 차트 팝업 요청.
# 클릭한 행의 숨김 컬럼(_chart_click)에 클릭 시각 기록 + filterChanged 발생으로 그리드 데이터를 반환한다.
CHART_CLICK_JS = JsCode("""
function(e) {
    e.node.setDataValue('_chart_click', Date.now());
    e.api.onFilterChanged();
}
""")
CHART_LINK_STYLE = {'color': '#4361ee', 'cursor': 'pointer', 'textDecoration': 'underline'}


def new_grid_click(grid_df, nonce_col, state_key):
    """그리드에서 아직 처리하지 않은 클릭(숨김 컬럼 nonce_col 에 기록된 클릭 시각)이 있으면 해당 행 반환"""
    if grid_df.empty or nonce_col not in grid_df.columns:
        return None
    clicks = pd.to_numeric(grid_df[nonce_col], errors='coerce').fillna(0)
    nonce = int(clicks.max())
    if nonce <= 0 or st.session_state.get(state_key) == nonce:
        return None
    st.session_state[state_key] = nonce
    return grid_df.loc[clicks.idxmax()]


def new_chart_click(grid_df, state_key, code_col, name_col):
    """그리드에서 아직 처리하지 않은 차트 클릭이 있으면 (종목코드, 종목명) 반환"""
    row = new_grid_click(grid_df, '_chart_click', state_key)
    return None if row is None else (row[code_col], row[name_col])


# 종목 일봉 OHLCV (C:\project\simul\simul_server.py 의 /api/stock-chart 와 같은 500일 조회, 5분 캐시)
@st.cache_data(ttl=300, show_spinner=False)
def load_ohlcv(code):
    from pykrx import stock  # 무거운 의존성은 차트 조회 시점에만 로드

    end = datetime.now()
    start = end - timedelta(days=500)
    df = stock.get_market_ohlcv_by_date(start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'), code)
    if df is None or df.empty:
        return pd.DataFrame()
    return df[['시가', '고가', '저가', '종가', '거래량']]


# 종목 차트 팝업: 일봉 캔들(상승=빨강/하락=파랑) + 거래량 막대(전일 대비 증가=빨강/감소=파랑)
@st.dialog("종목 차트", width="large")
def show_stock_chart(code, name):
    st.subheader(f"📈 {name} [{code}]")
    if not code or not str(code).isdigit() or len(str(code)) != 6:
        st.error("유효한 종목코드가 없어 차트를 표시할 수 없습니다.")
        return

    with st.spinner("차트 불러오는 중..."):
        try:
            df = load_ohlcv(code)
        except Exception as e:
            st.error(f"차트 데이터 조회 오류: {e}")
            return
    if df.empty:
        st.error(f"차트 데이터가 없습니다({code})")
        return

    x = df.index.strftime('%Y-%m-%d')
    vol_colors = ['red' if d >= 0 else 'blue' for d in df['거래량'].diff().fillna(0)]

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.03)
    fig.add_trace(go.Candlestick(x=x, open=df['시가'], high=df['고가'], low=df['저가'], close=df['종가'],
                                 increasing_line_color='red', increasing_fillcolor='red',
                                 decreasing_line_color='blue', decreasing_fillcolor='blue',
                                 name='일봉'), row=1, col=1)
    fig.add_trace(go.Bar(x=x, y=df['거래량'], marker_color=vol_colors, name='거래량'), row=2, col=1)
    # 휴장일 공백 없이 거래일만 표시
    fig.update_xaxes(type='category', nticks=20, tickangle=-90)
    fig.update_layout(height=650, showlegend=False, xaxis_rangeslider_visible=False,
                      margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

# 계좌번호 조회
def account(nickname):

    cur01 = conn.cursor()
    cur01.execute("select acct_no from \"stockAccount_stock_account\" where nick_name = %s", (nickname,))
    result_one = cur01.fetchone()
    cur01.close()

    return result_one[0]


# 매매노트 대상 종목 조회 (전영업일 체결 + 관심종목 + 트레이딩풀 보유종목, 종목코드 기준 조합)
def note_targets(acct_no, note_dt):

    cur02 = conn.cursor()

    # 작성일자 체결 종목 (체결 테이블에 code 가 없으므로 종목명으로 관심종목/트레이딩풀/일별잔고/트레이딩트레일에서 코드 조회)
    cur02.execute("""
        WITH oc AS (
            SELECT A.name,
                SUM(CASE WHEN A.order_type LIKE '%%매수%%' THEN A.order_price::numeric * A.total_complete_qty::int END) AS buy_amt,
                SUM(CASE WHEN A.order_type LIKE '%%매수%%' THEN A.total_complete_qty::int END)                          AS buy_qty,
                SUM(CASE WHEN A.order_type LIKE '%%매도%%' THEN A.order_price::numeric * A.total_complete_qty::int END) AS sell_amt,
                SUM(CASE WHEN A.order_type LIKE '%%매도%%' THEN A.total_complete_qty::int END)                          AS sell_qty
            FROM public."stockOrderComplete_stock_order_complete" A
            WHERE A.acct_no = %(acct_no)s
              AND A.order_dt = %(note_dt)s
              AND A.total_complete_qty::int > 0
              AND (A.order_type LIKE '%%매수%%' OR A.order_type LIKE '%%매도%%')
            GROUP BY A.name
        )
        SELECT COALESCE(
                   (SELECT I.code FROM public."interestItem_interest_item" I WHERE I.name = oc.name ORDER BY I.last_chg_date DESC LIMIT 1),
                   (SELECT B.code FROM public."stockBalance_stock_balance" B WHERE B.name = oc.name ORDER BY B.last_chg_date DESC LIMIT 1),
                   (SELECT D.code FROM public.dly_stock_balance D WHERE D.name = oc.name AND D.dt = %(note_dt)s LIMIT 1),
                   '') AS code,
               oc.name,
               ROUND(oc.buy_amt / NULLIF(oc.buy_qty, 0))   AS buy_price,
               oc.buy_qty,
               ROUND(oc.sell_amt / NULLIF(oc.sell_qty, 0)) AS sell_price,
               oc.sell_qty
        FROM oc
    """, {'acct_no': acct_no, 'note_dt': note_dt})
    df_complete = pd.DataFrame(cur02.fetchall(), columns=['code', 'name', 'buy_price', 'buy_qty', 'sell_price', 'sell_qty'])
    df_complete['구분'] = '체결'

    # 관심종목
    cur02.execute("""
        SELECT code, name FROM public."interestItem_interest_item"
        WHERE acct_no = %s AND code NOT IN ('0001', '1001') AND proc_yn = 'Y'
    """, (acct_no,))
    df_interest = pd.DataFrame(cur02.fetchall(), columns=['code', 'name'])
    df_interest['구분'] = '관심'

    # 트레이딩풀 보유종목 (체결이 없으면 매입단가/매입수량을 매수가/매수량 기본값으로 사용)
    # 작성일자가 오늘이면 현재 트레이딩풀, 과거 일자면 해당 일자의 일별잔고(dly_stock_balance) 기준
    if note_dt == datetime.now().strftime("%Y%m%d"):
        cur02.execute("""
            SELECT code, name, ROUND(purchase_price) AS buy_price, purchase_amount AS buy_qty
            FROM public."stockBalance_stock_balance"
            WHERE acct_no = %s AND proc_yn = 'Y'
        """, (acct_no,))
    else:
        cur02.execute("""
            SELECT code, name, purchase_price AS buy_price, purchase_qty AS buy_qty
            FROM public.dly_stock_balance
            WHERE acct = %s AND dt = %s
        """, (str(acct_no), note_dt))
    df_balance = pd.DataFrame(cur02.fetchall(), columns=['code', 'name', 'buy_price', 'buy_qty'])
    df_balance['구분'] = '보유'
    cur02.close()

    # 체결 → 보유 → 관심 순으로 우선하여 종목코드(+종목명) 기준 조합 (항목별 첫 번째 유효값 사용)
    df = pd.concat([df_complete, df_balance, df_interest], ignore_index=True)
    grouped = df.groupby(['code', 'name'], sort=False)
    df = grouped[['buy_price', 'buy_qty', 'sell_price', 'sell_qty']].first()
    df['구분'] = grouped['구분'].agg(lambda s: '/'.join(dict.fromkeys(s)))
    return df.reset_index()


# 방향흐름(direction_flow): DB 에는 코드(A~C)만 저장, 화면에는 설명 표시
DIRECTION_FLOWS = {
    'A': '상승',
    'B': '패턴',
    'C': '하락',
}


# 매매대응 결과(principle_yn): DB 에는 코드만 저장, 화면에는 설명 표시
PRINCIPLES = {
    'F': '실수',
    'S': '성공',
    'R': '찜찜',
    'H': '기대',
}


# 기존 매매노트 조회 (작성일자/계좌/종목 기준)
def get_note(acct_no, note_dt, code, name):

    cur03 = conn.cursor()
    cur03.execute("""
        SELECT buy_price, buy_qty, sell_price, sell_qty, buy_reason, sell_reason, principle_yn, good_point, improve_point,
               direction_flow
        FROM public.trading_note
        WHERE acct_no = %s AND note_dt = %s AND code = %s AND name = %s
    """, (acct_no, note_dt, code, name))
    row = cur03.fetchone()
    cur03.close()

    if row is None:
        return None
    return dict(zip(['buy_price', 'buy_qty', 'sell_price', 'sell_qty', 'buy_reason', 'sell_reason',
                     'principle_yn', 'good_point', 'improve_point', 'direction_flow'], row))


# 매매노트 저장 (동일 작성일자/계좌/종목이면 수정)
def save_note(record):

    cur04 = conn.cursor()
    try:
        cur04.execute("""
            INSERT INTO public.trading_note (
                note_dt, acct_no, code, name, buy_price, buy_qty, sell_price, sell_qty,
                buy_reason, sell_reason, principle_yn, good_point, improve_point, direction_flow, crt_dt, mod_dt
            ) VALUES (
                %(note_dt)s, %(acct_no)s, %(code)s, %(name)s, %(buy_price)s, %(buy_qty)s, %(sell_price)s, %(sell_qty)s,
                %(buy_reason)s, %(sell_reason)s, %(principle_yn)s, %(good_point)s, %(improve_point)s, %(direction_flow)s,
                now(), now()
            )
            ON CONFLICT (acct_no, note_dt, code, name) DO UPDATE SET
                buy_price = EXCLUDED.buy_price, buy_qty = EXCLUDED.buy_qty,
                sell_price = EXCLUDED.sell_price, sell_qty = EXCLUDED.sell_qty,
                buy_reason = EXCLUDED.buy_reason, sell_reason = EXCLUDED.sell_reason,
                principle_yn = EXCLUDED.principle_yn, good_point = EXCLUDED.good_point,
                improve_point = EXCLUDED.improve_point, direction_flow = EXCLUDED.direction_flow, mod_dt = now()
        """, record)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        st.error(f"매매노트 저장 오류: {e}")
        return False
    finally:
        cur04.close()


def to_int(value):
    return 0 if value is None or pd.isna(value) else int(value)

nickname = ['phills2', 'chichipa', 'phills75', 'yh480825', 'phills13', 'phills15', 'mamalong', 'honeylong', 'worry106']

# URL 쿼리 파라미터(?nickname=)로 전달된 닉네임이 있으면 기본 선택값으로 사용
query_nickname = st.query_params.get('nickname')
default_index = nickname.index(query_nickname) if query_nickname in nickname else 0

# 매매노트 조회 그리드에서 행을 클릭하면 해당 노트의 닉네임/작성일자/종목을 위젯 생성 전에 설정
# (위젯이 그려진 뒤에는 session_state 로 값을 바꿀 수 없으므로 요청을 저장 후 재실행하여 여기서 반영)
note_load_request = st.session_state.pop('note_load_request', None)
if note_load_request:
    st.session_state['nickname_select'] = note_load_request['nick']
    st.session_state['note_dt_select'] = note_load_request['note_dt']
    st.session_state['note_load_target'] = note_load_request

# 화면 항목명 → DB 컬럼 (UPDATE 시 컬럼명 화이트리스트로도 사용)
interest_price_cols = {
    '1차저항가': 'through_price',
    '1차지지가': 'leave_price',
    '2차저항가': 'resist_price',
    '2차지지가': 'support_price',
    '추세상한가': 'trend_high_price',
    '추세하한가': 'trend_low_price',
}

# 관심유형(watch_type): DB 에는 코드(A~D)를 콤마로 연결해 저장(예: 'A,C'), 화면에는 설명 표시
WATCH_TYPES = {
    'A': '가장 최근 목표가 또는 EPS 수정이 상향',
    'B': '실측 3개 분기 동안 매출·영업이익이 연속 증가',
    'C': '직전 분기 대비 영업이익 증감이 ±10% 이내',
    'D': '흑자전환 또는 적자축소이고, 정기보고서 접수일 이후 주가 상승이 +5% 이하',
}


def parse_watch_type(value):
    """DB 문자열('A,C') 또는 그리드 반환값(리스트)을 A~D 코드만 남긴 정렬 리스트로 변환"""
    if value is None or (not isinstance(value, (list, tuple)) and pd.isna(value)):
        return []
    items = value if isinstance(value, (list, tuple)) else str(value).split(',')
    return sorted({str(v).strip() for v in items if str(v).strip() in WATCH_TYPES})


# 매매노트 조회
st.header("매매노트 조회")

# key 로 값을 관리하므로 session_state 에 값이 없을 때만 기본 index 지정 (중복 지정 경고 방지)
my_choice = st.selectbox('닉네임을 선택하세요', nickname, key='nickname_select',
                         **({} if 'nickname_select' in st.session_state else {'index': default_index}))

acct_no = account(my_choice)

# 작성기간은 시작일~종료일이 모두 보이도록 폭을 넓게 배정
c1, c3, c4, c5 = st.columns([1.6, 1, 1, 1.4])
# 작성기간: 하나의 캘린더에서 시작일/종료일 선택 (시작일만 선택한 상태면 그 하루로 조회)
note_period = c1.date_input("작성기간", (datetime.now() - timedelta(days=30), datetime.now()), format="YYYY-MM-DD")
strt_date, end_date = (note_period[0], note_period[-1]) if note_period else (datetime.now(), datetime.now())
# 작성항목 조건: 단건 선택, 선택한 항목에 입력 내용이 있는 노트 (매수근거→[매수], 매도근거→[매도], 개선할 점→[개선], 잘한점→[성취])
note_item_cols = {'매수': 'buy_reason', '매도': 'sell_reason', '개선': 'improve_point', '성취': 'good_point'}
item_filter = c3.selectbox("작성항목", [None] + list(note_item_cols),
                           format_func=lambda t: '전체' if t is None else f"{t}")
item_cond = f"COALESCE(TRIM({note_item_cols[item_filter]}), '') <> ''" if item_filter else 'TRUE'
# 매매대응 결과(principle_yn) 조건: 단건 선택, 전체 선택 시 조건 없음
result_choice = c4.selectbox("매매대응 결과", [None] + list(PRINCIPLES),
                             format_func=lambda c: '전체' if c is None else PRINCIPLES[c])
result_filter = [result_choice] if result_choice else []
# 닉네임 조건: 여러 개 선택 가능, 선택하지 않으면 전체
nick_filter = c5.multiselect("닉네임", nickname, placeholder="전체")

cur05 = conn.cursor()
cur05.execute("""
    SELECT note_dt, code, name, buy_price, buy_qty, sell_price, sell_qty, direction_flow,
           buy_reason, sell_reason, principle_yn,
           good_point, improve_point, (SELECT nick_name FROM public."stockAccount_stock_account" WHERE acct_no = A.acct_no) AS nick_name,
           acct_no
    FROM public.trading_note A
    WHERE note_dt BETWEEN %(strt_dt)s AND %(end_dt)s
      AND (%(no_result_filter)s OR principle_yn = ANY(%(result_filter)s))
      AND (%(no_nick_filter)s OR acct_no IN (SELECT acct_no FROM public."stockAccount_stock_account"
                                             WHERE nick_name = ANY(%(nick_filter)s)))
      AND (""" + item_cond + """)
    ORDER BY note_dt DESC, name
""", {'strt_dt': strt_date.strftime("%Y%m%d"), 'end_dt': end_date.strftime("%Y%m%d"),
      'no_result_filter': not result_filter, 'result_filter': result_filter,
      'no_nick_filter': not nick_filter, 'nick_filter': nick_filter})
df_notes = pd.DataFrame(cur05.fetchall(), columns=['작성일자', '종목코드', '종목명', '매수가', '매수량', '매도가', '매도량', '방향흐름',
                                                   '매수근거', '매도근거', '매매대응 결과', '잘한점', '개선할 점', '닉네임', '계좌번호'])
cur05.close()

if df_notes.empty:
    st.info("조회된 매매노트가 없습니다.")
else:
    for col in ['매수가', '매도가']:
        df_notes[col] = pd.to_numeric(df_notes[col], errors='coerce')
    df_notes['방향흐름'] = df_notes['방향흐름'].map(lambda c: DIRECTION_FLOWS.get(c, ''))   # 코드 → 설명
    df_notes['매매대응 결과'] = df_notes['매매대응 결과'].map(lambda c: PRINCIPLES.get(c, ''))
    df_notes['_chart_click'] = 0.0
    df_notes['_load_click'] = 0.0

    gb = GridOptionsBuilder.from_dataframe(df_notes)
    gb.configure_column('종목코드', hide=True)   # 차트 조회/노트 불러오기용
    gb.configure_column('계좌번호', hide=True)   # 노트 불러오기용
    gb.configure_column('_chart_click', hide=True)
    gb.configure_column('_load_click', hide=True)

    # 행 클릭(종목명 제외 - 종목명은 차트 팝업) 시 클릭 시각을 _load_click 에 기록하고 그리드 데이터를 반환해
    # 해당 노트를 매매노트 작성 영역에 불러온다. 클릭한 행은 선택 표시.
    load_click_js = JsCode("""
        function(e) {
            if (!e.node || !e.colDef || e.colDef.field === '종목명') { return; }
            e.node.setDataValue('_load_click', Date.now());
            e.api.onFilterChanged();
        }
    """)
    gb.configure_grid_options(onCellClicked=load_click_js,
                              rowSelection={'mode': 'singleRow', 'checkboxes': False, 'enableClickSelection': True})
    gb.configure_pagination(enabled=True, paginationPageSize=20)
    # 컬럼 폭을 데이터 크기에 맞춤 (첫 렌더링/페이지 이동 시)
    autosize_js = JsCode("""
        function(params) {
            params.api.autoSizeAllColumns(false);
        }
    """)
    gb.configure_grid_options(domLayout='normal', onFirstDataRendered=autosize_js, onPaginationChanged=autosize_js)

    # 숫자 포맷을 JS 코드로 적용 (정렬 문제 방지)
    number_format_js = JsCode("""
        function(params) {
            if (params.value === null || params.value === undefined) {
                return '';
            }
            return params.value.toLocaleString();
        }
    """)

    gb.configure_column('작성일자', pinned='left')
    # 종목명 클릭 시 차트 팝업
    gb.configure_column('종목명', pinned='left', onCellClicked=CHART_CLICK_JS, cellStyle=CHART_LINK_STYLE)
    for col in ['매수가', '매수량', '매도가', '매도량']:
        gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js)
    # 긴 텍스트는 최대 폭까지만 늘리고 줄바꿈
    for col in ['매수근거', '매도근거', '잘한점', '개선할 점']:
        gb.configure_column(col, maxWidth=400, wrapText=True, autoHeight=True)

    grid_options = gb.build()

    notes_response = AgGrid(
        df_notes,
        gridOptions=grid_options,
        fit_columns_on_grid_load=False,
        allow_unsafe_jscode=True,
        use_container_width=True,
        update_mode=GridUpdateMode.NO_UPDATE,
        update_on=['filterChanged'],     # 종목명 클릭(차트 팝업)/행 클릭(노트 불러오기) 시 값 반환
        enable_enterprise_modules=True,  # 엑셀 다운로드 위해 필요
        excel_export_mode='xlsx',        # 엑셀(xlsx)로 다운로드
        key=f"notes_grid_{acct_no}",
    )

    notes_grid_df = pd.DataFrame(notes_response['data'])

    # 종목명 클릭으로 들어온 새 요청이면 차트 팝업
    chart_target = new_chart_click(notes_grid_df, f"notes_chart_nonce_{acct_no}", '종목코드', '종목명')
    if chart_target:
        show_stock_chart(*chart_target)

    # 행 클릭으로 들어온 새 요청이면 해당 노트를 매매노트 작성 영역에 불러오기 (위젯 생성 전 반영을 위해 재실행)
    load_row = new_grid_click(notes_grid_df, '_load_click', "notes_load_nonce")
    if load_row is not None:
        if load_row['닉네임'] not in nickname:
            st.warning(f"닉네임({load_row['닉네임']})이 선택 목록에 없어 노트를 불러올 수 없습니다.")
        else:
            st.session_state['note_load_request'] = {
                'nick': load_row['닉네임'], 'acct_no': int(load_row['계좌번호']), 'note_dt': load_row['작성일자'],
                'code': load_row['종목코드'], 'name': load_row['종목명'],
            }
            st.rerun()

st.header("매매노트 작성")

# 작성일자는 영업일(주말/stock_holiday 제외)만 선택 가능, 최근 영업일이 기본값
cur06 = conn.cursor()
cur06.execute("""
    SELECT to_char(d, 'YYYYMMDD')
    FROM generate_series(CURRENT_DATE - 90, CURRENT_DATE, interval '1 day') AS d
    WHERE is_business_day(d::date)
    ORDER BY d DESC
""")
business_days = [row[0] for row in cur06.fetchall()]
cur06.close()

# 조회 그리드에서 불러온 노트의 작성일자가 목록(최근 90일)에 없으면 추가, 목록에 없는 이전 선택값은 초기화
requested_dt = st.session_state.get('note_dt_select')
if requested_dt and requested_dt not in business_days:
    if st.session_state.get('note_load_target'):
        business_days = sorted(business_days + [requested_dt], reverse=True)
    else:
        st.session_state.pop('note_dt_select')

weekday_names = ['월', '화', '수', '목', '금', '토', '일']
note_dt = st.selectbox(
    "작성일자", business_days, key='note_dt_select',
    format_func=lambda d: f"{d[:4]}-{d[4:6]}-{d[6:]} ({weekday_names[datetime.strptime(d, '%Y%m%d').weekday()]})"
)

df_targets = note_targets(acct_no, note_dt)

# 작성일자에 이미 매매노트가 작성된 종목 (대상 조건에서 빠진 종목도 노트를 열 수 있도록 목록에 추가)
cur08 = conn.cursor()
cur08.execute("""
    SELECT code, name, buy_reason, sell_reason, improve_point, good_point
    FROM public.trading_note WHERE acct_no = %s AND note_dt = %s
""", (acct_no, note_dt))
# 입력 내용이 있는 항목별 표시: 매수근거→[매수], 매도근거→[매도], 개선할 점→[개선], 잘한점→[성취] (모두 비어 있으면 [노트])
note_tag_names = ['매수', '매도', '개선', '성취']
note_tags = {}
for c, n, *texts in cur08.fetchall():
    tags = ''.join(f"[{t}]" for t, txt in zip(note_tag_names, texts) if txt and str(txt).strip())
    note_tags[(c, n)] = tags or '[노트]'
noted = set(note_tags)
cur08.close()
missing_noted = sorted(noted - set(zip(df_targets['code'], df_targets['name'])))
if missing_noted:
    df_targets = pd.concat([df_targets, pd.DataFrame([{'code': c, 'name': n, '구분': '노트'} for c, n in missing_noted])],
                           ignore_index=True)

# 레이블이 바뀌어도 선택이 유지되도록 key 고정 (key 가 없으면 노트 표시 변경 시 첫 종목으로 초기화)
target_key = f"note_target_{acct_no}_{note_dt}"
form_widget_prefixes = ['bp', 'bq', 'sp', 'sq', 'df', 'br', 'sr', 'pr', 'gp', 'ip']

# 조회 그리드에서 불러온 노트의 종목을 선택하고, 입력 중이던 값은 지워 저장된 노트 내용으로 다시 채움
note_load_target = st.session_state.pop('note_load_target', None)
if note_load_target and note_load_target['acct_no'] == acct_no and note_load_target['note_dt'] == note_dt:
    hit = df_targets.index[(df_targets['code'] == note_load_target['code']) & (df_targets['name'] == note_load_target['name'])]
    if len(hit):
        st.session_state[target_key] = int(hit[0])
        load_key = f"{acct_no}_{note_dt}_{note_load_target['code']}_{note_load_target['name']}"
        for prefix in form_widget_prefixes:
            st.session_state.pop(f"{prefix}_{load_key}", None)

if df_targets.empty:
    st.info("매매노트 작성 대상 종목이 없습니다.")
else:
    labels = [(f"{r.name} ({r.code}) [{r.구분}]" if r.code else f"{r.name} [{r.구분}]")
              + (f" {note_tags[(r.code, r.name)]}" if (r.code, r.name) in note_tags else "")
              for r in df_targets.itertuples()]
    if st.session_state.get(target_key, 0) >= len(labels):
        st.session_state.pop(target_key)
    selected_idx = st.selectbox("종목명을 선택하세요", range(len(labels)), format_func=lambda i: labels[i],
                                key=target_key)
    target = df_targets.iloc[selected_idx]
    code, name = target['code'], target['name']

    # 기존에 작성한 노트가 있으면 그 값을, 없으면 체결/보유 정보를 기본값으로 사용
    note = get_note(acct_no, note_dt, code, name)
    if note is None:
        note = {
            'buy_price': target.get('buy_price'), 'buy_qty': target.get('buy_qty'),
            'sell_price': target.get('sell_price'), 'sell_qty': target.get('sell_qty'),
            'buy_reason': '', 'sell_reason': '', 'principle_yn': 'S', 'good_point': '', 'improve_point': '',
            'direction_flow': None,
        }
    else:
        st.caption("이미 작성된 매매노트가 있습니다. 저장 시 수정됩니다.")

    # 종목/작성일자가 바뀌면 입력값이 새로 채워지도록 위젯 key 에 포함
    key = f"{acct_no}_{note_dt}_{code}_{name}"

    with st.form(f"note_form_{key}"):
        c1, c2, c3, c4 = st.columns(4)
        buy_price = c1.number_input("매수가", min_value=0, step=1, value=to_int(note['buy_price']), key=f"bp_{key}")
        buy_qty = c2.number_input("매수량", min_value=0, step=1, value=to_int(note['buy_qty']), key=f"bq_{key}")
        sell_price = c3.number_input("매도가", min_value=0, step=1, value=to_int(note['sell_price']), key=f"sp_{key}")
        sell_qty = c4.number_input("매도량", min_value=0, step=1, value=to_int(note['sell_qty']), key=f"sq_{key}")

        # 방향흐름: 코드(A~C) 선택, 화면에는 설명만 표시 (미선택 시 NULL 저장)
        flow_options = [None] + list(DIRECTION_FLOWS)
        direction_flow = st.selectbox("방향흐름", flow_options,
                                      index=flow_options.index(note['direction_flow']) if note['direction_flow'] in DIRECTION_FLOWS else 0,
                                      format_func=lambda c: '선택하세요' if c is None else DIRECTION_FLOWS[c],
                                      key=f"df_{key}")

        buy_reason = st.text_area("매수근거", value=note['buy_reason'] or '', key=f"br_{key}")
        sell_reason = st.text_area("매도근거", value=note['sell_reason'] or '', key=f"sr_{key}")
        principle_options = list(PRINCIPLES)
        principle = st.radio("매매대응 결과", principle_options, format_func=lambda c: PRINCIPLES[c],
                             index=principle_options.index(note['principle_yn']) if note['principle_yn'] in PRINCIPLES
                             else principle_options.index('S'),
                             horizontal=True, key=f"pr_{key}")
        good_point = st.text_area("잘한점", value=note['good_point'] or '', key=f"gp_{key}")
        improve_point = st.text_area("개선할 점", value=note['improve_point'] or '', key=f"ip_{key}")

        submitted = st.form_submit_button("저장")

    if submitted:
        record = {
            'note_dt': note_dt, 'acct_no': acct_no, 'code': code, 'name': name,
            'buy_price': buy_price, 'buy_qty': buy_qty, 'sell_price': sell_price, 'sell_qty': sell_qty,
            'buy_reason': buy_reason, 'sell_reason': sell_reason,
            'principle_yn': principle,
            'good_point': good_point, 'improve_point': improve_point,
            'direction_flow': direction_flow,
        }
        if save_note(record):
            # 종목 레이블의 노트 표시([매수][매도] 등)가 바로 반영되도록 재실행 (저장 메시지는 재실행 후 표시)
            st.session_state['note_save_msg'] = f"{name} 매매노트가 저장되었습니다."
            st.rerun()

    if 'note_save_msg' in st.session_state:
        st.success(st.session_state.pop('note_save_msg'))

# 관심종목 조회/변경 (조회·삭제는 선택 계좌 기준, 가격 변경은 모든 계좌의 해당 종목에 반영)
st.header("관심종목")

cur07 = conn.cursor()
# 선택 계좌의 관심종목
cur07.execute(f"""
    SELECT code, name, watch_type, {', '.join(interest_price_cols.values())}
    FROM public."interestItem_interest_item"
    WHERE acct_no = %s AND code NOT IN ('0001', '1001') AND proc_yn = 'Y'
    ORDER BY code
""", (acct_no,))
df_interest = pd.DataFrame(cur07.fetchall(), columns=['코드', '종목명', '관심유형'] + list(interest_price_cols))
cur07.close()

if 'interest_apply_msg' in st.session_state:
    st.success(st.session_state.pop('interest_apply_msg'))

if df_interest.empty:
    st.info("관심종목이 없습니다.")
else:
    for col in interest_price_cols:
        df_interest[col] = pd.to_numeric(df_interest[col], errors='coerce').fillna(0).astype(int)
    # 리스트 컬럼은 Arrow 직렬화 시 그리드에서 JS 배열로 읽히지 않으므로 'A,C' 문자열로 전달
    df_interest['관심유형'] = df_interest['관심유형'].map(lambda v: ','.join(parse_watch_type(v)))
    df_interest['삭제'] = False
    df_interest['_apply_nonce'] = 0.0
    df_interest['_chart_click'] = 0.0

    # 그리드 컬럼 순서: 종목명 → 가격 항목 → 삭제 → 관심유형 (코드/_ 컬럼은 숨김)
    df_interest = df_interest[['코드', '종목명'] + list(interest_price_cols)
                              + ['삭제', '관심유형', '_apply_nonce', '_chart_click']]

    gb = GridOptionsBuilder.from_dataframe(df_interest)
    gb.configure_column('_apply_nonce', hide=True)
    gb.configure_column('_chart_click', hide=True)
    gb.configure_grid_options(statusBar={'statusPanels': [{'statusPanel': APPLY_EDITS_PANEL_JS, 'align': 'left'}]})
    gb.configure_pagination(enabled=True, paginationPageSize=20)

    # 컬럼 폭을 데이터 크기에 맞춤 (첫 렌더링/페이지 이동 시)
    autosize_js = JsCode("""
        function(params) {
            params.api.autoSizeAllColumns(false);
        }
    """)
    gb.configure_grid_options(domLayout='normal', onFirstDataRendered=autosize_js, onPaginationChanged=autosize_js)

    # 숫자 포맷을 JS 코드로 적용 (정렬 문제 방지)
    number_format_js = JsCode("""
        function(params) {
            if (params.value === null || params.value === undefined) {
                return '';
            }
            return params.value.toLocaleString();
        }
    """)

    # 종목명 클릭 시 차트 팝업
    gb.configure_column('코드', hide=True)   # 변경 적용/차트 조회용
    gb.configure_column('종목명', pinned='left', onCellClicked=CHART_CLICK_JS, cellStyle=CHART_LINK_STYLE)
    for col in interest_price_cols:
        gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js,
                            editable=True, cellEditor='agNumberCellEditor',
                            cellStyle={'backgroundColor': '#fff8e1'})

    # 관심유형: 행 데이터는 'A,C' 문자열, 그리드 안에서만 코드 배열로 변환해 다중 선택
    # (valueGetter: 문자열 → 배열, valueSetter: 선택 배열 → 문자열), 셀/선택목록에는 코드 설명 표시
    watch_type_js = f"const WATCH_TYPES = {json.dumps(WATCH_TYPES, ensure_ascii=False)};"
    gb.configure_column('관심유형', editable=True, cellDataType=False, singleClickEdit=True,
                        valueGetter=JsCode("""
                            function(params) {
                                const v = params.data ? params.data['관심유형'] : null;
                                return v ? String(v).split(',').filter(c => c) : [];
                            }
                        """),
                        valueSetter=JsCode("""
                            function(params) {
                                const nv = params.newValue;
                                const codes = Array.isArray(nv) ? nv : (nv ? String(nv).split(',') : []);
                                const joined = Array.from(new Set(codes.filter(c => c))).sort().join(',');
                                if (joined === (params.data['관심유형'] || '')) { return false; }
                                params.data['관심유형'] = joined;
                                return true;
                            }
                        """),
                        cellEditor='agRichSelectCellEditor', cellEditorPopup=True,
                        cellEditorParams={
                            'values': list(WATCH_TYPES),
                            'multiSelect': True,
                            'formatValue': JsCode("""
                                function(v) { %s return WATCH_TYPES[v] ? '[' + v + '] ' + WATCH_TYPES[v] : v; }
                            """ % watch_type_js),
                        },
                        valueFormatter=JsCode("""
                            function(params) {
                                %s
                                const v = params.value;
                                if (!v || v.length === 0) { return ''; }
                                return (Array.isArray(v) ? v : String(v).split(',')).map(c => WATCH_TYPES[c] ? '[' + c + '] ' + WATCH_TYPES[c] : c).join('\\n');   // 항목별 줄바꿈
                            }
                        """ % watch_type_js),
                        maxWidth=500, wrapText=True, autoHeight=True,
                        # pre-line: 항목 사이 줄바꿈은 유지하고 긴 설명은 자동 줄바꿈
                        cellStyle={'backgroundColor': '#fff8e1', 'whiteSpace': 'pre-line', 'lineHeight': '1.6'})
    gb.configure_column('삭제', cellDataType='boolean', editable=True,
                        cellRenderer='agCheckboxCellRenderer', cellEditor='agCheckboxCellEditor')

    grid_options = gb.build()

    st.caption("가격 항목은 셀을 더블클릭해 수정하고, 관심유형은 셀을 클릭해 여러 개를 선택하며, 삭제할 종목은 [삭제]를 체크한 뒤 "
               "그리드 하단 [변경 적용]을 누르면 가격·관심유형 변경은 모든 계좌의 해당 종목에, 삭제는 선택한 계좌에만 반영됩니다. "
               "종목명을 클릭하면 차트가 표시됩니다.")

    # AgGrid를 통해 데이터 출력 (그리드 하단 [변경 적용] 버튼 클릭 시에만 편집값 반환)
    grid_response = AgGrid(
        df_interest,
        gridOptions=grid_options,
        fit_columns_on_grid_load=False,
        allow_unsafe_jscode=True,
        use_container_width=True,
        update_mode=GridUpdateMode.NO_UPDATE,
        update_on=['filterChanged'],     # [변경 적용] 버튼/차트 클릭이 발생시키는 이벤트에서만 값 반환
        enable_enterprise_modules=True,  # 상태바 버튼 위해 필요
        key=f"interest_grid_{acct_no}",
    )

    edited_df = pd.DataFrame(grid_response['data'])

    # 코드/종목명 클릭으로 들어온 새 요청이면 차트 팝업
    chart_target = new_chart_click(edited_df, f"interest_chart_nonce_{acct_no}", '코드', '종목명')
    if chart_target:
        show_stock_chart(*chart_target)

    # [변경 적용] 클릭으로 들어온 새 요청일 때만 편집 전/후 비교로 변경 항목 수집
    apply_requested = is_new_apply_request(edited_df, f"interest_apply_nonce_{acct_no}")
    price_changes = []   # (코드, 종목명, 항목, DB컬럼, 변경 전, 변경 후)
    deletes = []         # (코드, 종목명)
    watch_changes = []   # (코드, 종목명, 변경 전 코드리스트, 변경 후 코드리스트)
    if apply_requested and '코드' in edited_df.columns:
        orig_by_code = df_interest.set_index('코드')
        for _, row in edited_df.iterrows():
            code_ = row['코드']
            if code_ not in orig_by_code.index:
                continue
            if row.get('삭제') in (True, 'true', 'True'):
                deletes.append((code_, row['종목명']))
                continue
            old_watch = parse_watch_type(orig_by_code.at[code_, '관심유형'])
            new_watch = parse_watch_type(row.get('관심유형'))
            if new_watch != old_watch:
                watch_changes.append((code_, row['종목명'], old_watch, new_watch))
            for label, db_col in interest_price_cols.items():
                try:
                    new_val = int(float(row[label]))
                except (TypeError, ValueError):
                    continue
                old_val = int(orig_by_code.at[code_, label])
                if new_val != old_val:
                    price_changes.append((code_, row['종목명'], label, db_col, old_val, new_val))

    if apply_requested and not price_changes and not deletes and not watch_changes:
        st.info("변경된 항목이 없습니다.")
    elif price_changes or deletes or watch_changes:
        cur_upd = conn.cursor()
        try:
            applied = []
            for code_, name_, label, db_col, old_val, new_val in price_changes:
                # db_col은 interest_price_cols 화이트리스트 값만 사용, 가격은 모든 계좌의 해당 종목에 반영
                cur_upd.execute(f"""
                    UPDATE public."interestItem_interest_item"
                       SET {db_col} = %s, last_chg_date = now()
                     WHERE code = %s AND proc_yn = 'Y'
                """, (new_val, code_))
                applied.append(f"{name_} {label} {old_val:,} → {new_val:,} ({cur_upd.rowcount}개 계좌)")
            for code_, name_, old_watch, new_watch in watch_changes:
                # 관심유형도 가격과 같이 모든 계좌의 해당 종목에 반영 (선택 없으면 NULL)
                cur_upd.execute("""
                    UPDATE public."interestItem_interest_item"
                       SET watch_type = %s, last_chg_date = now()
                     WHERE code = %s AND proc_yn = 'Y'
                """, (','.join(new_watch) or None, code_))
                applied.append(f"{name_} 관심유형 {','.join(old_watch) or '없음'} → {','.join(new_watch) or '없음'} "
                               f"({cur_upd.rowcount}개 계좌)")
            for code_, name_ in deletes:
                cur_upd.execute("""
                    UPDATE public."interestItem_interest_item"
                       SET proc_yn = 'N', last_chg_date = now()
                     WHERE acct_no = %s AND code = %s AND proc_yn = 'Y'
                """, (acct_no, code_))
                applied.append(f"{name_} 삭제")
            conn.commit()
            st.session_state['interest_apply_msg'] = "변경 완료\n\n" + "\n\n".join(applied)
        except Exception as e:
            conn.rollback()
            st.error(f"변경 실패: {e}")
        finally:
            cur_upd.close()
        if 'interest_apply_msg' in st.session_state:
            st.rerun()