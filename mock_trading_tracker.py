import psycopg2 as db
from datetime import datetime
import io
import time
import requests
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# PostgreSQL 연결 설정
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"

# DB 연결
# universe_balance_info.py 에서 runpy 로 매 재실행마다 수행되므로, st.rerun() 등으로 중간에 끝난
# 이전 실행의 연결은 여기서 닫고, 정상 종료 시에는 스크립트 끝에서 닫는다.
# (위젯 on_change 콜백은 이전 실행의 닫힌 연결을 참조하므로 콜백에서는 DB 를 쓰지 않고 session_state 에 요청만 기록)
prev_conn = st.session_state.pop('mock_trading_conn', None)
if prev_conn is not None and not prev_conn.closed:
    prev_conn.close()
conn = db.connect(conn_string)
st.session_state['mock_trading_conn'] = conn

SLOTS = ['1차', '2차', '3차']
DEFAULT_INIT_CASH = 10_000_000
UP_COLOR, DOWN_COLOR = '#16a34a', '#dc2626'

st.markdown("""
<style>
.mt-badge {display:inline-block;padding:3px 12px;border-radius:12px;font-size:0.85rem;font-weight:700;margin-bottom:6px;}
.mt-badge.buy {background:#e8f7f1;color:#0f8a5f;}
.mt-badge.sell {background:#fdecec;color:#dc2626;}
.mt-expect {font-size:0.85rem;line-height:1.5;}
.mt-expect b {color:#0284c7;}
</style>
""", unsafe_allow_html=True)


# 계좌번호 조회
def account(nickname):

    cur01 = conn.cursor()
    cur01.execute("select acct_no from \"stockAccount_stock_account\" where nick_name = %s", (nickname,))
    result_one = cur01.fetchone()
    cur01.close()

    return result_one[0]


# KRX(KIND) 전체 상장종목 목록 (C:\project\simul\simul_server.py 의 _load_krx 와 같은 방식)
# 6시간 캐시. 로드 실패 시 예외를 올려 캐시되지 않게 하고, 호출부에서 다른 목록/DB 조회로 대체한다.
def _fetch_krx():
    res = requests.get('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download', timeout=15)
    res.encoding = 'EUC-KR'
    df = pd.read_html(io.StringIO(res.text), header=0)[0][['회사명', '종목코드']]
    df = df.rename(columns={'회사명': 'company', '종목코드': 'code'})
    df['company'] = df['company'].astype(str).str.strip()
    df['code'] = df['code'].apply(lambda c: str(c).strip().lstrip('A').zfill(6)[-6:])
    # 신주인수권증서·특수종목 등은 코드에 영문자가 섞이므로(예: '0015N0') 6자리 영숫자까지 허용
    return df[df['code'].str.match(r'^[0-9A-Za-z]{6}$')].reset_index(drop=True)


@st.cache_data(ttl=6 * 3600, show_spinner=False)
def load_krx():
    return _fetch_krx()


# 조회 실패(오늘 상장 등) 시 강제 재로드: 10분 단위 bucket 별로 1회만 다시 받는다
@st.cache_data(ttl=600, show_spinner=False)
def load_krx_fresh(bucket):
    return _fetch_krx()


def _krx_match(df, name):
    """종목명 정확일치 → 앞부분 일치 순으로 종목코드 조회"""
    if df is None or df.empty:
        return None
    rows = df[df['company'] == name]
    if rows.empty:
        rows = df[df['company'].str.startswith(name, na=False)]
    return rows.iloc[0]['code'] if not rows.empty else None


# 종목명 → 종목코드 (KRX 상장목록 우선, 없으면 관심종목/트레이딩풀 보유종목/일별잔고 순으로 조회 — ETF 등 KIND 목록에 없는 종목 대비)
def find_code(name):

    name = (name or '').strip()
    if not name:
        return None
    for loader in (load_krx, lambda: load_krx_fresh(int(time.time() // 600))):
        try:
            code = _krx_match(loader(), name)
        except Exception as e:
            print(f"[KRX] 종목목록 로드 오류: {e}")
            code = None
        if code:
            return code

    cur02 = conn.cursor()
    cur02.execute("""
        SELECT COALESCE(
            (SELECT I.code FROM public."interestItem_interest_item" I WHERE I.name = %(name)s ORDER BY I.last_chg_date DESC LIMIT 1),
            (SELECT B.code FROM public."stockBalance_stock_balance" B WHERE B.name = %(name)s ORDER BY B.last_chg_date DESC LIMIT 1),
            (SELECT D.code FROM public.dly_stock_balance D WHERE D.name = %(name)s ORDER BY D.dt DESC LIMIT 1)
        )
    """, {'name': name})
    row = cur02.fetchone()
    cur02.close()

    return row[0] if row else None


# 신규 트래커 종목명 선택 목록 (관심종목 + 트레이딩풀 보유종목)
def stock_names(acct_no):

    cur03 = conn.cursor()
    cur03.execute("""
        SELECT name FROM public."interestItem_interest_item"
         WHERE acct_no = %(acct_no)s AND proc_yn = 'Y' AND code NOT IN ('0001', '1001')
        UNION
        SELECT name FROM public."stockBalance_stock_balance" WHERE acct_no = %(acct_no)s AND proc_yn = 'Y'
        ORDER BY name
    """, {'acct_no': acct_no})
    names = [r[0] for r in cur03.fetchall() if r[0]]
    cur03.close()

    return names


def run_write(sql_params, err_label):
    """[(sql, params), ...] 를 한 트랜잭션으로 수행 (실패 시 롤백 후 오류 표시)"""
    cur = conn.cursor()
    try:
        for sql, params in sql_params:
            cur.execute(sql, params)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        st.error(f"{err_label} 오류: {e}")
        return False
    finally:
        cur.close()


# 트래커 생성 (생성된 id 반환, 실패 시 None)
def create_tracker(acct_no, code, name, init_cash):

    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO public.mock_tracker (acct_no, code, name, init_cash, crt_dt)
            VALUES (%s, %s, %s, %s, now()) RETURNING id
        """, (str(acct_no), code, name, init_cash))
        new_id = cur.fetchone()[0]
        conn.commit()
        return new_id
    except Exception as e:
        conn.rollback()
        st.error(f"트래커 생성 오류: {e}")
        return None
    finally:
        cur.close()


# 매매 저장 + 해당 슬롯 현재가를 체결가로 갱신
def save_trade(tracker_id, trade_dt, slot, trade_tp, price, qty):
    return run_write([
        ("""INSERT INTO public.mock_tracker_trade (tracker_id, trade_dt, slot, trade_tp, price, qty, crt_dt)
            VALUES (%s, %s, %s, %s, %s, %s, now())""", (tracker_id, trade_dt, slot, trade_tp, price, qty)),
        ("""INSERT INTO public.mock_tracker_price (tracker_id, slot, cur_price, mod_dt) VALUES (%s, %s, %s, now())
            ON CONFLICT (tracker_id, slot) DO UPDATE SET cur_price = EXCLUDED.cur_price, mod_dt = now()""",
         (tracker_id, slot, price)),
    ], "매매 저장")


def save_price(tracker_id, slot, price):
    return run_write([
        ("""INSERT INTO public.mock_tracker_price (tracker_id, slot, cur_price, mod_dt) VALUES (%s, %s, %s, now())
            ON CONFLICT (tracker_id, slot) DO UPDATE SET cur_price = EXCLUDED.cur_price, mod_dt = now()""",
         (tracker_id, slot, price)),
    ], "현재가 저장")


def replay(trades, init_cash):
    """매매내역을 순서대로 재생하여 슬롯별 보유현황, 거래별 실현손익/잔여현금, 거래 시점 수익률 추이 계산"""
    cash = float(init_cash)
    pos = {}         # slot -> {'qty', 'cost'}
    last_px = {}     # slot -> 최근 체결가 (거래 시점 평가용)
    realized_total = 0.0
    history, curve = [], [('시작', 0.0)]
    for t in trades.itertuples():
        p = pos.setdefault(t.slot, {'qty': 0, 'cost': 0.0})
        amount = t.price * t.qty
        realized = None
        if t.trade_tp == 'B':
            cash -= amount
            p['qty'] += t.qty
            p['cost'] += amount
        else:
            avg = p['cost'] / p['qty'] if p['qty'] else 0
            realized = (t.price - avg) * t.qty
            realized_total += realized
            p['cost'] -= avg * t.qty
            p['qty'] -= t.qty
            cash += amount
        last_px[t.slot] = t.price
        equity = cash + sum(v['qty'] * last_px.get(s, 0) for s, v in pos.items())
        curve.append((f"{t.slot}{'매수' if t.trade_tp == 'B' else '매도'}", (equity / init_cash - 1) * 100))
        history.append({'id': t.id, 'trade_dt': t.trade_dt, 'trade_tp': t.trade_tp, 'slot': t.slot, 'qty': t.qty,
                        'price': t.price, 'amount': amount, 'realized': realized, 'cash': cash})
    return cash, pos, realized_total, pd.DataFrame(history), curve


def fmt_dt(d):
    return f"{d[:4]}-{d[4:6]}-{d[6:]}" if d and len(d) == 8 else d


def signed(v, suffix=''):
    return f"{v:+,.0f}{suffix}"


def pl_color(v):
    if v is None or pd.isna(v) or v == 0:
        return ''
    return f"color: {UP_COLOR}" if v > 0 else f"color: {DOWN_COLOR}"


nickname = ['phills2', 'chichipa', 'phills75', 'yh480825', 'phills13', 'phills15', 'mamalong', 'honeylong', 'worry106']

# 닉네임은 화면에서 선택하지 않고 URL 쿼리 파라미터(?nickname=)로 전달받은 값 사용 (없거나 목록에 없으면 첫 번째 닉네임)
query_nickname = st.query_params.get('nickname')
my_choice = query_nickname if query_nickname in nickname else nickname[0]
acct_no = account(my_choice)

st.header("모의투자 트래커")

if 'mock_msg' in st.session_state:
    st.success(st.session_state.pop('mock_msg'))

# 트래커(종목) 선택 / 신규 생성
cur04 = conn.cursor()
cur04.execute("SELECT id, code, name, init_cash FROM public.mock_tracker WHERE acct_no = %s ORDER BY crt_dt DESC",
              (str(acct_no),))
trackers = cur04.fetchall()
cur04.close()

# 트래커는 id 로 선택 (종목코드 보완 등으로 표시 레이블이 바뀌어도 선택 유지), None 은 신규 생성
tracker_key = f"mock_tracker_{acct_no}"
tracker_by_id = {t[0]: t for t in trackers}

# 신규 생성한 트래커 선택: 위젯이 그려진 뒤에는 session_state 로 값을 바꿀 수 없으므로
# 생성 시 요청만 저장 후 재실행하여 위젯 생성 전에 반영
# (신규 종목은 NEW_TRACKER(0) 로 표시 — serial id 는 1부터이므로 겹치지 않음)
NEW_TRACKER = 0
select_request = st.session_state.pop('mock_tracker_select', None)
if select_request in tracker_by_id:
    st.session_state[tracker_key] = select_request
elif tracker_key in st.session_state and st.session_state[tracker_key] not in (*tracker_by_id, NEW_TRACKER):
    st.session_state.pop(tracker_key)   # 삭제된 트래커

c1, _ = st.columns([2, 3])
tracker_choice = c1.selectbox(
    "종목명", list(tracker_by_id) + [NEW_TRACKER], key=tracker_key,
    format_func=lambda i: '+ 신규 종목' if i == NEW_TRACKER else
    (f"{tracker_by_id[i][2]} ({tracker_by_id[i][1]})" if tracker_by_id[i][1] else tracker_by_id[i][2]),
)

if tracker_choice == NEW_TRACKER:
    with st.form("mock_new_tracker"):
        names = stock_names(acct_no)
        c1, c2, c3 = st.columns([2, 2, 1.5])
        name_pick = c1.selectbox("종목명 선택", ['직접 입력'] + names)
        name_input = c2.text_input("종목명 직접 입력", placeholder="목록에 없는 종목")
        init_cash = c3.number_input("초기자금 (원)", min_value=10_000, step=1_000_000, value=DEFAULT_INIT_CASH)
        created = st.form_submit_button("트래커 생성")
    if created:
        new_name = name_input.strip() if name_pick == '직접 입력' else name_pick
        if not new_name:
            st.error("종목명을 입력하세요.")
        elif new_name in [n for _, _, n, _ in trackers]:
            st.error(f"{new_name} 트래커가 이미 있습니다.")
        elif (new_id := create_tracker(acct_no, (new_code := find_code(new_name)), new_name, init_cash)):
            st.session_state['mock_tracker_select'] = new_id
            st.session_state['mock_msg'] = (f"{new_name} ({new_code}) 트래커가 생성되었습니다." if new_code else
                                            f"{new_name} 트래커가 생성되었습니다. (종목코드를 찾지 못함 — 종목명을 확인하세요)")
            st.rerun()
else:
    tracker_id, code, name, init_cash = tracker_by_id[tracker_choice]

    # 생성 시 종목코드를 찾지 못한 트래커는 다시 조회하여 보완 (그 사이 신규상장 등)
    if not code and (code := find_code(name)):
        run_write([("UPDATE public.mock_tracker SET code = %s WHERE id = %s", (code, tracker_id))], "종목코드 보완")
    init_cash = float(init_cash)

    # 현재가 업데이트 입력(콜백에서 기록한 요청) 반영
    for slot, price in st.session_state.pop('mock_price_request', {}).items():
        if price and price > 0:
            save_price(tracker_id, slot, price)

    cur05 = conn.cursor()
    cur05.execute("""
        SELECT id, trade_dt, slot, trade_tp, price, qty FROM public.mock_tracker_trade
        WHERE tracker_id = %s ORDER BY trade_dt, id
    """, (tracker_id,))
    trades = pd.DataFrame(cur05.fetchall(), columns=['id', 'trade_dt', 'slot', 'trade_tp', 'price', 'qty'])
    cur05.execute("SELECT slot, cur_price FROM public.mock_tracker_price WHERE tracker_id = %s", (tracker_id,))
    cur_prices = {s: float(p) for s, p in cur05.fetchall()}
    cur05.close()
    trades['price'] = pd.to_numeric(trades['price']).astype(float)

    cash, pos, realized_total, df_hist, curve = replay(trades, init_cash)
    holdings = {s: v for s, v in pos.items() if v['qty'] > 0}
    eval_amt = sum(v['qty'] * cur_prices.get(s, v['cost'] / v['qty']) for s, v in holdings.items())
    total_rt = (cash + eval_amt - init_cash) / init_cash * 100

    # 상단 요약 카드
    m1, m2, m3, m4 = st.columns(4)
    for col, label, value in [(m1, '가용 현금', f"{cash:,.0f}"), (m2, '평가금액', f"{eval_amt:,.0f}"),
                              (m3, '실현손익', signed(realized_total)), (m4, '전체 수익률', f"{total_rt:+.2f}%")]:
        with col.container(border=True):
            st.metric(label, value)

    # 거래 입력: 선택 슬롯에 보유분이 없으면 매수 모드(가용현금 전액 기준), 있으면 매도 모드
    with st.container(border=True):
        st.markdown("**● 거래 입력**")
        nonce = st.session_state.get(f"mock_nonce_{tracker_id}", 0)   # 거래 후 입력값 초기화용
        slot = st.session_state.get(f"mock_slot_{tracker_id}", SLOTS[0])
        hold = holdings.get(slot)
        sell_mode = hold is not None
        st.markdown(f"<span class='mt-badge {'sell' if sell_mode else 'buy'}'>● {'매도' if sell_mode else '매수'} 모드</span>",
                    unsafe_allow_html=True)

        if sell_mode:
            c1, c2, c3, c4, c5, c6 = st.columns([1, 1.3, 4, 1, 0.8, 0.9], vertical_alignment="bottom")
        else:
            c1, c2, c3, c4, c6 = st.columns([1, 1.3, 4, 1.8, 0.9], vertical_alignment="bottom")
        slot = c1.selectbox("슬롯", SLOTS, key=f"mock_slot_{tracker_id}")
        trade_date = c2.date_input("매매일자", datetime.now(), format="YYYY-MM-DD", key=f"mock_dt_{tracker_id}")
        price = c3.number_input("단가 (원)", min_value=0, step=100, value=None, placeholder="0",
                                key=f"mock_price_{tracker_id}_{nonce}")

        if sell_mode:
            qty_key = f"mock_sell_qty_{tracker_id}_{nonce}"
            sell_qty = c4.number_input("매도 수량", min_value=0, max_value=hold['qty'], step=1, value=None,
                                       placeholder="전량", key=qty_key)
            c5.button("전량", key=f"mock_all_{tracker_id}_{nonce}",
                      on_click=lambda k=qty_key, q=hold['qty']: st.session_state.__setitem__(k, q))
            if c6.button("매도", type="primary", use_container_width=True, key=f"mock_sell_{tracker_id}"):
                qty = int(sell_qty) if sell_qty else hold['qty']
                if not price:
                    st.error("단가를 입력하세요.")
                elif qty <= 0 or qty > hold['qty']:
                    st.error(f"매도 수량은 1~{hold['qty']:,}주 사이여야 합니다.")
                elif save_trade(tracker_id, trade_date.strftime('%Y%m%d'), slot, 'S', price, qty):
                    pl = (price - hold['cost'] / hold['qty']) * qty
                    st.session_state[f"mock_nonce_{tracker_id}"] = nonce + 1
                    st.session_state['mock_msg'] = f"{name} {slot} {qty:,}주 매도 (실현손익 {signed(pl, '원')})"
                    st.rerun()
        else:
            buy_qty = int(cash // price) if price else 0
            if price:
                expect = (f"<b>{buy_qty:,}주</b> 매수 가능<br>{buy_qty * price:,.0f}원 투입 · 잔여 "
                          f"{cash - buy_qty * price:,.0f}원")
            else:
                expect = "단가 입력 시 자동 계산"
            c4.markdown(f"<div class='mt-expect'><b style='color:#475569'>예상</b><br>{expect}</div>",
                        unsafe_allow_html=True)
            if c6.button("매수", type="primary", use_container_width=True, key=f"mock_buy_{tracker_id}"):
                if not price:
                    st.error("단가를 입력하세요.")
                elif buy_qty <= 0:
                    st.error("가용 현금이 부족합니다.")
                elif save_trade(tracker_id, trade_date.strftime('%Y%m%d'), slot, 'B', price, buy_qty):
                    st.session_state[f"mock_nonce_{tracker_id}"] = nonce + 1
                    st.session_state['mock_msg'] = f"{name} {slot} {buy_qty:,}주 매수 ({buy_qty * price:,.0f}원)"
                    st.rerun()

    # 수익률 추이 (거래 시점별 전체 수익률, 거래 시점 평가는 슬롯별 최근 체결가 기준)
    with st.container(border=True):
        st.markdown("**● 수익률 추이**")
        labels = [lbl for lbl, _ in curve]
        values = [v for _, v in curve]
        line_color = UP_COLOR if values[-1] >= 0 else DOWN_COLOR
        fig = go.Figure(go.Scatter(
            x=list(range(len(values))), y=values, mode='lines+markers', line_shape='spline',
            line=dict(color=line_color, width=2), marker=dict(size=7, color=line_color),
            fill='tozeroy', fillcolor='rgba(22,163,74,0.06)' if values[-1] >= 0 else 'rgba(220,38,38,0.06)',
            customdata=labels, hovertemplate="%{customdata}<br>%{y:+.2f}%<extra></extra>",
        ))
        fig.update_xaxes(tickmode='array', tickvals=list(range(len(values))), ticktext=labels, showgrid=True)
        fig.update_yaxes(ticksuffix='%', tickformat='+.1f', zeroline=True)
        fig.update_layout(height=260, margin=dict(l=10, r=10, t=10, b=10), showlegend=False)
        st.plotly_chart(fig, use_container_width=True, key=f"mock_curve_{tracker_id}")

    # 보유종목 / 거래내역
    with st.container(border=True):
        tab_hold, tab_hist = st.tabs(["보유종목", "거래내역"])
        with tab_hold:
            if not holdings:
                st.caption("보유 종목이 없습니다")
            else:
                rows = []
                for s, v in holdings.items():
                    avg = v['cost'] / v['qty']
                    cur = cur_prices.get(s, avg)
                    pl = (cur - avg) * v['qty']
                    rows.append({'슬롯': s, '수량': v['qty'], '평균단가': avg, '현재가': cur, '평가손익': pl,
                                 '수익률': (cur / avg - 1) * 100, '기여도': pl / init_cash * 100})
                df_hold = pd.DataFrame(rows)
                st.dataframe(
                    df_hold.style.format({'수량': '{:,}', '평균단가': '{:,.0f}', '현재가': '{:,.0f}',
                                          '평가손익': '{:+,.0f}원', '수익률': '{:+.2f}%', '기여도': '{:+.2f}%'})
                    .map(pl_color, subset=['평가손익', '수익률', '기여도']),
                    hide_index=True, use_container_width=True)
        with tab_hist:
            if df_hist.empty:
                st.caption("거래 내역이 없습니다")
            else:
                df_view = pd.DataFrame({
                    '매매일자': df_hist['trade_dt'].map(fmt_dt),
                    '구분': df_hist['trade_tp'].map({'B': 'BUY', 'S': 'SELL'}),
                    '슬롯': df_hist['slot'], '수량': df_hist['qty'], '단가': df_hist['price'],
                    '거래금액': df_hist['amount'], '실현손익': df_hist['realized'], '잔여현금': df_hist['cash'],
                }).iloc[::-1]
                st.dataframe(
                    df_view.style.format({'수량': '{:,}', '단가': '{:,.0f}', '거래금액': '{:,.0f}',
                                          '실현손익': lambda v: '—' if pd.isna(v) else f"{v:+,.0f}원",
                                          '잔여현금': '{:,.0f}'})
                    .map(lambda v: f"color: {UP_COLOR if v == 'BUY' else DOWN_COLOR}; font-weight: 700",
                         subset=['구분'])
                    .map(pl_color, subset=['실현손익']),
                    hide_index=True, use_container_width=True)

                # 마지막 거래 취소 (재생 방식이라 마지막 거래만 취소해야 이후 잔고가 일관됨)
                last = df_hist.iloc[-1]
                if st.button(f"마지막 거래 취소 ({fmt_dt(last['trade_dt'])} {last['slot']} "
                             f"{'매수' if last['trade_tp'] == 'B' else '매도'} {last['qty']:,}주)",
                             key=f"mock_undo_{tracker_id}"):
                    if run_write([("DELETE FROM public.mock_tracker_trade WHERE id = %s", (int(last['id']),))],
                                 "거래 취소"):
                        st.session_state['mock_msg'] = "마지막 거래가 취소되었습니다."
                        st.rerun()

    # 현재가 업데이트 (보유 슬롯별 입력 → 평가금액/평가손익 반영)
    with st.container(border=True):
        st.markdown("**● 현재가 업데이트**")
        if not holdings:
            st.caption("보유 종목이 없습니다")
        else:
            def request_price(slot_, key_):
                st.session_state.setdefault('mock_price_request', {})[slot_] = st.session_state[key_]

            cols = st.columns(6)
            for i, (s, v) in enumerate(holdings.items()):
                key_ = f"mock_cur_{tracker_id}_{s}"
                st.session_state[key_] = int(cur_prices.get(s, v['cost'] / v['qty']))
                cols[i % 6].number_input(f"{s} (원)", min_value=0, step=100, key=key_,
                                         on_change=request_price, args=(s, key_))

    with st.expander("트래커 관리"):
        st.caption(f"초기자금 {init_cash:,.0f}원 · 종목코드 {code or '-'}")
        if st.button(f"{name} 트래커 삭제 (매매내역 포함)", key=f"mock_del_{tracker_id}"):
            if run_write([("DELETE FROM public.mock_tracker WHERE id = %s", (tracker_id,))], "트래커 삭제"):
                st.session_state['mock_msg'] = f"{name} 트래커가 삭제되었습니다."
                st.rerun()

# 정상 종료 시 DB 연결 닫기 (st.rerun() 으로 중간에 끝난 경우는 다음 실행 시작 시 닫음)
conn.close()
st.session_state.pop('mock_trading_conn', None)
