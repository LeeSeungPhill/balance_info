import psycopg2 as db
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import JsCode

# PostgreSQL 연결 설정
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"
# conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
# DB 연결
conn = db.connect(conn_string)

# 계좌번호 조회
def account(nickname):

    cur01 = conn.cursor()
    cur01.execute("select acct_no from \"stockAccount_stock_account\" where nick_name = %s", (nickname,))
    result_one = cur01.fetchone()
    cur01.close()

    return result_one[0]


# 매매노트 대상 종목 조회 (전영업일 체결 + 관심종목 + 트레이딩풀 보유종목, 종목코드 기준 조합)
def note_targets(acct_no):

    cur02 = conn.cursor()

    # 전영업일 체결 종목 (체결 테이블에 code 가 없으므로 종목명으로 관심종목/트레이딩풀/트레이딩트레일에서 코드 조회)
    cur02.execute("""
        WITH oc AS (
            SELECT A.name,
                SUM(CASE WHEN A.order_type LIKE '%%매수%%' THEN A.order_price::numeric * A.total_complete_qty::int END) AS buy_amt,
                SUM(CASE WHEN A.order_type LIKE '%%매수%%' THEN A.total_complete_qty::int END)                          AS buy_qty,
                SUM(CASE WHEN A.order_type LIKE '%%매도%%' THEN A.order_price::numeric * A.total_complete_qty::int END) AS sell_amt,
                SUM(CASE WHEN A.order_type LIKE '%%매도%%' THEN A.total_complete_qty::int END)                          AS sell_qty
            FROM public."stockOrderComplete_stock_order_complete" A
            WHERE A.acct_no = %s
              AND A.order_dt = prev_business_day_char(CURRENT_DATE)
              AND A.total_complete_qty::int > 0
              AND (A.order_type LIKE '%%매수%%' OR A.order_type LIKE '%%매도%%')
            GROUP BY A.name
        )
        SELECT COALESCE(
                   (SELECT I.code FROM public."interestItem_interest_item" I WHERE I.name = oc.name ORDER BY I.last_chg_date DESC LIMIT 1),
                   (SELECT B.code FROM public."stockBalance_stock_balance" B WHERE B.name = oc.name ORDER BY B.last_chg_date DESC LIMIT 1),
                   (SELECT T.code FROM public.trading_trail T WHERE T.name = oc.name ORDER BY T.crt_dt DESC LIMIT 1),
                   '') AS code,
               oc.name,
               ROUND(oc.buy_amt / NULLIF(oc.buy_qty, 0))   AS buy_price,
               oc.buy_qty,
               ROUND(oc.sell_amt / NULLIF(oc.sell_qty, 0)) AS sell_price,
               oc.sell_qty
        FROM oc
    """, (acct_no,))
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
    cur02.execute("""
        SELECT code, name, ROUND(purchase_price) AS buy_price, purchase_amount AS buy_qty
        FROM public."stockBalance_stock_balance"
        WHERE acct_no = %s AND proc_yn = 'Y'
    """, (acct_no,))
    df_balance = pd.DataFrame(cur02.fetchall(), columns=['code', 'name', 'buy_price', 'buy_qty'])
    df_balance['구분'] = '보유'
    cur02.close()

    # 체결 → 보유 → 관심 순으로 우선하여 종목코드(+종목명) 기준 조합 (항목별 첫 번째 유효값 사용)
    df = pd.concat([df_complete, df_balance, df_interest], ignore_index=True)
    grouped = df.groupby(['code', 'name'], sort=False)
    df = grouped[['buy_price', 'buy_qty', 'sell_price', 'sell_qty']].first()
    df['구분'] = grouped['구분'].agg(lambda s: '/'.join(dict.fromkeys(s)))
    return df.reset_index()


# 기존 매매노트 조회 (작성일자/계좌/종목 기준)
def get_note(acct_no, note_dt, code, name):

    cur03 = conn.cursor()
    cur03.execute("""
        SELECT buy_price, buy_qty, sell_price, sell_qty, buy_reason, sell_reason, principle_yn, good_point, improve_point
        FROM public.trading_note
        WHERE acct_no = %s AND note_dt = %s AND code = %s AND name = %s
    """, (acct_no, note_dt, code, name))
    row = cur03.fetchone()
    cur03.close()

    if row is None:
        return None
    return dict(zip(['buy_price', 'buy_qty', 'sell_price', 'sell_qty', 'buy_reason', 'sell_reason',
                     'principle_yn', 'good_point', 'improve_point'], row))


# 매매노트 저장 (동일 작성일자/계좌/종목이면 수정)
def save_note(record):

    cur04 = conn.cursor()
    try:
        cur04.execute("""
            INSERT INTO public.trading_note (
                note_dt, acct_no, code, name, buy_price, buy_qty, sell_price, sell_qty,
                buy_reason, sell_reason, principle_yn, good_point, improve_point, crt_dt, mod_dt
            ) VALUES (
                %(note_dt)s, %(acct_no)s, %(code)s, %(name)s, %(buy_price)s, %(buy_qty)s, %(sell_price)s, %(sell_qty)s,
                %(buy_reason)s, %(sell_reason)s, %(principle_yn)s, %(good_point)s, %(improve_point)s, now(), now()
            )
            ON CONFLICT (acct_no, note_dt, code, name) DO UPDATE SET
                buy_price = EXCLUDED.buy_price, buy_qty = EXCLUDED.buy_qty,
                sell_price = EXCLUDED.sell_price, sell_qty = EXCLUDED.sell_qty,
                buy_reason = EXCLUDED.buy_reason, sell_reason = EXCLUDED.sell_reason,
                principle_yn = EXCLUDED.principle_yn, good_point = EXCLUDED.good_point,
                improve_point = EXCLUDED.improve_point, mod_dt = now()
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

my_choice = st.selectbox('닉네임을 선택하세요', nickname, index=default_index)

acct_no = account(my_choice)

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

weekday_names = ['월', '화', '수', '목', '금', '토', '일']
note_dt = st.selectbox(
    "작성일자", business_days,
    format_func=lambda d: f"{d[:4]}-{d[4:6]}-{d[6:]} ({weekday_names[datetime.strptime(d, '%Y%m%d').weekday()]})"
)

df_targets = note_targets(acct_no)

if df_targets.empty:
    st.info("매매노트 작성 대상 종목이 없습니다.")
else:
    labels = [f"{r.name} ({r.code}) [{r.구분}]" if r.code else f"{r.name} [{r.구분}]" for r in df_targets.itertuples()]
    selected_idx = st.selectbox("종목명을 선택하세요", range(len(labels)), format_func=lambda i: labels[i])
    target = df_targets.iloc[selected_idx]
    code, name = target['code'], target['name']

    # 기존에 작성한 노트가 있으면 그 값을, 없으면 체결/보유 정보를 기본값으로 사용
    note = get_note(acct_no, note_dt, code, name)
    if note is None:
        note = {
            'buy_price': target.get('buy_price'), 'buy_qty': target.get('buy_qty'),
            'sell_price': target.get('sell_price'), 'sell_qty': target.get('sell_qty'),
            'buy_reason': '', 'sell_reason': '', 'principle_yn': 'Y', 'good_point': '', 'improve_point': '',
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

        buy_reason = st.text_area("매수근거", value=note['buy_reason'] or '', key=f"br_{key}")
        sell_reason = st.text_area("매도근거", value=note['sell_reason'] or '', key=f"sr_{key}")
        principle = st.radio("매매원칙 준수여부", ['준수', '미준수'], index=0 if note['principle_yn'] != 'N' else 1,
                             horizontal=True, key=f"pr_{key}")
        good_point = st.text_area("잘한점", value=note['good_point'] or '', key=f"gp_{key}")
        improve_point = st.text_area("개선할 점", value=note['improve_point'] or '', key=f"ip_{key}")

        submitted = st.form_submit_button("저장")

    if submitted:
        record = {
            'note_dt': note_dt, 'acct_no': acct_no, 'code': code, 'name': name,
            'buy_price': buy_price, 'buy_qty': buy_qty, 'sell_price': sell_price, 'sell_qty': sell_qty,
            'buy_reason': buy_reason, 'sell_reason': sell_reason,
            'principle_yn': 'Y' if principle == '준수' else 'N',
            'good_point': good_point, 'improve_point': improve_point,
        }
        if save_note(record):
            st.success(f"{name} 매매노트가 저장되었습니다.")

# 매매노트 조회
st.header("매매노트 조회")

c1, c2 = st.columns(2)
strt_date = c1.date_input("조회시작일", datetime.now() - timedelta(days=30))
end_date = c2.date_input("조회종료일", datetime.now())

cur05 = conn.cursor()
cur05.execute("""
    SELECT note_dt, name, buy_price, buy_qty, sell_price, sell_qty,
           buy_reason, sell_reason, CASE principle_yn WHEN 'Y' THEN '준수' WHEN 'N' THEN '미준수' ELSE '' END,
           good_point, improve_point
    FROM public.trading_note
    WHERE acct_no = %s AND note_dt BETWEEN %s AND %s
    ORDER BY note_dt DESC, name
""", (acct_no, strt_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")))
df_notes = pd.DataFrame(cur05.fetchall(), columns=['작성일자', '종목명', '매수가', '매수량', '매도가', '매도량',
                                                   '매수근거', '매도근거', '매매원칙 준수여부', '잘한점', '개선할 점'])
cur05.close()

if df_notes.empty:
    st.info("조회된 매매노트가 없습니다.")
else:
    for col in ['매수가', '매도가']:
        df_notes[col] = pd.to_numeric(df_notes[col], errors='coerce')

    gb = GridOptionsBuilder.from_dataframe(df_notes)
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
    gb.configure_column('종목명', pinned='left')
    for col in ['매수가', '매수량', '매도가', '매도량']:
        gb.configure_column(col, type=['numericColumn'], cellRenderer=number_format_js)
    # 긴 텍스트는 최대 폭까지만 늘리고 줄바꿈
    for col in ['매수근거', '매도근거', '잘한점', '개선할 점']:
        gb.configure_column(col, maxWidth=400, wrapText=True, autoHeight=True)

    grid_options = gb.build()

    AgGrid(
        df_notes,
        gridOptions=grid_options,
        fit_columns_on_grid_load=False,
        allow_unsafe_jscode=True,
        use_container_width=True,
        update_mode=GridUpdateMode.NO_UPDATE,
        enable_enterprise_modules=True,  # 엑셀 다운로드 위해 필요
        excel_export_mode='xlsx'         # 엑셀(xlsx)로 다운로드
    )
