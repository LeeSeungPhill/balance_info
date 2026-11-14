import psycopg2 as db
from datetime import datetime
import io
import os
import re
import time
import requests
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import JsCode

# 편집 가능한 AG-Grid 하단 상태바의 [변경 적용] 버튼 (bit_balance_info.py 와 같은 방식).
# 셀 편집만으로는 재실행되지 않도록 update_on=['filterChanged'] 만 사용하고,
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


# 판단모드(check_mode) 코드 ↔ 콤보박스 표시값 ('' 는 미입력)
CHECK_MODES = {'S': '잘함 (S)', 'M': '보통 (M)', 'F': '미흡 (F)'}
CHECK_MODE_CODES = {v: k for k, v in CHECK_MODES.items()}

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
/* 입력란 높이에 맞춰 세로 가운데 (마크다운 요소의 음수 하단 여백 -1rem 을 상쇄해 입력란과 같은 높이에 위치) */
.mt-expect {font-size:0.85rem;line-height:1.5;min-height:2.5rem;display:flex;align-items:center;margin-bottom:1rem;}
.mt-expect b {color:#0284c7;}
/* 매매통계 요약 카드 */
.mt-stat-title {font-size:0.95rem;font-weight:700;margin:6px 0 6px;}
.mt-stat-title span {font-size:0.78rem;font-weight:400;color:#64748b;}
.mt-stat-row {display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:8px;}
.mt-stat {border:1px solid rgba(49,51,63,0.2);border-radius:8px;padding:12px 14px;}
.mt-stat .lbl {font-size:0.85rem;color:#64748b;}
.mt-stat .val {font-size:1.5rem;font-weight:600;line-height:1.4;word-break:keep-all;}
.mt-stat .sub {font-size:0.78rem;color:#64748b;min-height:1.1em;}
/* 단가 입력란의 +/- 버튼 숨김 */
div[class*="st-key-mt_price_box_"] [data-testid="stNumberInputStepUp"],
div[class*="st-key-mt_price_box_"] [data-testid="stNumberInputStepDown"] {display:none;}
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


# ── 가져오기 / 전체저장 / 전체삭제 (C:\project\simul\simul_server.py 의 import-csv / save-all / delete-all 과 같은 방식) ──
# 단, 모의투자 테이블은 여러 계좌(닉네임)가 함께 쓰므로 모든 처리는 현재 계좌(acct_no)의 트래커 범위로 한정한다.
# 테이블 순서 = 가져오기/저장 순서 (mock_tracker 가 부모, 나머지는 tracker_id FK ON DELETE CASCADE)
MOCK_TABLES = ['mock_tracker', 'mock_tracker_trade', 'mock_tracker_price']
MOCK_EXPORT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mock_exports')
# 파일명 → 테이블 (mock_tracker 가 다른 두 테이블명의 접두어이므로 긴 이름부터 비교)
_MOCK_FILE_RE = re.compile(r'^(mock_tracker_trade|mock_tracker_price|mock_tracker)(_|\.|$)')


def _acct_filter(table):
    """테이블별 현재 계좌 범위 WHERE 절 (파라미터 acct_no 1개)"""
    if table == 'mock_tracker':
        return "acct_no = %s"
    return "tracker_id IN (SELECT id FROM public.mock_tracker WHERE acct_no = %s)"


def _table_columns(cur, table):
    cur.execute("""SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s ORDER BY ordinal_position""", (table,))
    return [r[0] for r in cur.fetchall()]


def mock_save_all(acct_no):
    """현재 계좌의 3개 테이블을 CSV(BOM 포함)로 저장하고, _save 테이블이 있으면 save_dtm 을 붙여 스냅샷 복사.
    반환: [{'table', 'rows', 'file', 'saved_to_table'}]"""
    os.makedirs(MOCK_EXPORT_DIR, exist_ok=True)
    now = datetime.now()
    now_str, save_dtm = now.strftime('%Y%m%d_%H%M%S'), now.strftime('%Y%m%d%H%M%S')
    results = []
    cur = conn.cursor()
    try:
        for table in MOCK_TABLES:
            cols = ', '.join(_table_columns(cur, table))
            where = cur.mogrify(_acct_filter(table), (str(acct_no),)).decode()
            order = 'tracker_id, slot' if table == 'mock_tracker_price' else 'id'
            saved = False
            cur.execute("SELECT to_regclass(%s)", (f'public.{table}_save',))
            if cur.fetchone()[0]:
                cur.execute(f"INSERT INTO public.{table}_save (save_dtm, {cols}) "
                            f"SELECT %s, {cols} FROM public.{table} WHERE {where}", (save_dtm,))
                saved = True
            buf = io.BytesIO()
            cur.copy_expert(f"COPY (SELECT {cols} FROM public.{table} WHERE {where} ORDER BY {order}) "
                            f"TO STDOUT WITH CSV HEADER", buf)
            fname = f'{table}_{acct_no}_{now_str}.csv'
            with open(os.path.join(MOCK_EXPORT_DIR, fname), 'wb') as fout:
                fout.write(b'\xef\xbb\xbf')
                fout.write(buf.getvalue())
            cur.execute(f"SELECT count(*) FROM public.{table} WHERE {where}")   # 여러 줄 텍스트가 있어 줄 수로 세지 않음
            rows = cur.fetchone()[0]
            results.append({'table': table, 'rows': rows, 'file': fname, 'saved_to_table': saved})
        conn.commit()
        return results
    except Exception as e:
        conn.rollback()
        st.error(f"전체저장 오류: {e}")
        return None
    finally:
        cur.close()


def mock_import_csv(acct_no, files):
    """업로드한 CSV(파일명으로 테이블 인식)로 현재 계좌 데이터를 교체. 한 트랜잭션으로 처리하며 하나라도 실패하면 전체 롤백.
    - 가져오기 순서: mock_tracker → mock_tracker_trade → mock_tracker_price
    - mock_tracker 행은 모두 현재 계좌(acct_no)여야 하고, trade/price 행은 현재 계좌 트래커의 tracker_id 여야 함
    - mock_tracker 를 교체하면 FK CASCADE 로 기존 매매/현재가도 삭제되므로 세 파일을 함께 가져오는 것을 권장"""
    by_table = {}
    for f in files:
        m = _MOCK_FILE_RE.match(f.name or '')
        if not m:
            st.error(f"{f.name}: 파일명에서 테이블을 인식할 수 없습니다. ({' / '.join(MOCK_TABLES)} 중 하나로 시작해야 합니다)")
            return None
        if m.group(1) in by_table:
            st.error(f"{m.group(1)} 파일이 두 개 이상 선택되었습니다.")
            return None
        by_table[m.group(1)] = f
    acct = str(acct_no)
    msgs = []
    cur = conn.cursor()
    try:
        for table in [t for t in MOCK_TABLES if t in by_table]:
            raw = by_table[table].getvalue()
            if raw.startswith(b'\xef\xbb\xbf'):
                raw = raw[3:]
            cols = ', '.join(_table_columns(cur, table))
            cur.execute(f"CREATE TEMP TABLE _mock_import (LIKE public.{table}) ON COMMIT DROP")
            cur.copy_expert(f"COPY _mock_import ({cols}) FROM STDIN WITH CSV HEADER NULL ''", io.BytesIO(raw))
            # 다른 계좌 데이터가 섞여 있으면 거부
            if table == 'mock_tracker':
                cur.execute("SELECT count(*) FROM _mock_import WHERE acct_no IS DISTINCT FROM %s", (acct,))
            else:
                cur.execute("SELECT count(*) FROM _mock_import WHERE tracker_id NOT IN "
                            "(SELECT id FROM public.mock_tracker WHERE acct_no = %s)", (acct,))
            bad = cur.fetchone()[0]
            if bad:
                raise ValueError(f"{table}: 현재 계좌({acct})가 아닌 데이터 {bad}건이 포함되어 있습니다.")
            cur.execute(f"DELETE FROM public.{table} WHERE {_acct_filter(table)}", (acct,))
            deleted = cur.rowcount
            cur.execute(f"INSERT INTO public.{table} ({cols}) SELECT {cols} FROM _mock_import")
            msgs.append(f"{table} 가져오기 완료 ({cur.rowcount}건, 기존 {deleted}건 삭제)")
            cur.execute("DROP TABLE _mock_import")
        # 가져온 id 가 시퀀스보다 크면 시퀀스를 올려 다음 생성 시 id 충돌 방지 (낮추지는 않음, setval 은 롤백되지 않음)
        for table in ('mock_tracker', 'mock_tracker_trade'):
            cur.execute("SELECT pg_get_serial_sequence(%s, 'id')", (f'public.{table}',))
            seq = cur.fetchone()[0]
            cur.execute(f"SELECT last_value FROM {seq}")
            last = cur.fetchone()[0]
            cur.execute(f"SELECT max(id) FROM public.{table}")
            max_id = cur.fetchone()[0] or 0
            if max_id > last:
                cur.execute("SELECT setval(%s, %s)", (seq, max_id))
        conn.commit()
        return msgs
    except Exception as e:
        conn.rollback()
        st.error(f"가져오기 오류 (전체 취소됨): {e}")
        return None
    finally:
        cur.close()


def mock_delete_all(acct_no):
    """현재 계좌의 트래커 전체 삭제 (매매/현재가는 FK CASCADE 로 함께 삭제). 반환: 테이블별 삭제 건수"""
    acct = str(acct_no)
    cur = conn.cursor()
    try:
        counts = {}
        for table in reversed(MOCK_TABLES):   # 자식 테이블 건수 먼저 집계 후 삭제
            cur.execute(f"DELETE FROM public.{table} WHERE {_acct_filter(table)}", (acct,))
            counts[table] = cur.rowcount
        conn.commit()
        return counts
    except Exception as e:
        conn.rollback()
        st.error(f"전체삭제 오류: {e}")
        return None
    finally:
        cur.close()


def render_data_tools(acct_no, nickname_):
    """화면 상단 [가져오기] [전체저장] [전체삭제] 도구 모음"""
    # 전체삭제 확인 대화상자: 대화상자(fragment) 재실행 시점에는 이전 실행의 DB 연결이 닫혀 있을 수 있으므로
    # 확인 버튼은 요청만 기록하고 전체 재실행 후 여기서 처리한다.
    if st.session_state.pop('mock_delete_all_request', None) == str(acct_no):
        counts = mock_delete_all(acct_no)
        if counts is not None:
            st.session_state.pop('mock_saved_files', None)
            st.session_state['mock_msg'] = (f"전체삭제 완료 (총 {sum(counts.values())}건 — 트래커 {counts['mock_tracker']}, "
                                            f"매매 {counts['mock_tracker_trade']}, 현재가 {counts['mock_tracker_price']})")
            st.rerun()

    @st.dialog("전체삭제")
    def confirm_delete_all():
        st.warning(f"**{nickname_}** 계좌({acct_no})의 모의투자 데이터 전체를 삭제합니다.\n\n"
                   "- mock_tracker (트래커)\n- mock_tracker_trade (매매내역)\n- mock_tracker_price (현재가)\n\n"
                   "삭제 전에 전체저장을 먼저 실행하시길 권장합니다.")
        c1, c2 = st.columns(2)
        if c1.button("삭제", type="primary", use_container_width=True):
            st.session_state['mock_delete_all_request'] = str(acct_no)
            st.rerun()
        if c2.button("취소", use_container_width=True):
            st.rerun()

    b1, b2, b3, _ = st.columns([1.6, 1.6, 1.6, 3.2])
    with b1.popover("📂 가져오기", use_container_width=True):
        st.caption("전체저장으로 받은 CSV를 선택하세요. 파일명이 mock_tracker / mock_tracker_trade / "
                   "mock_tracker_price 로 시작해야 하며, 현재 계좌 데이터가 파일 내용으로 교체됩니다. "
                   "mock_tracker 를 가져오면 기존 매매·현재가도 함께 삭제되므로 세 파일을 함께 선택하세요.")
        up_key = f"mock_import_files_{st.session_state.get('mock_import_nonce', 0)}"
        files = st.file_uploader("CSV 파일", type=['csv'], accept_multiple_files=True, key=up_key,
                                 label_visibility='collapsed')
        if st.button("가져오기 실행", type="primary", disabled=not files, use_container_width=True):
            msgs = mock_import_csv(acct_no, files)
            if msgs is not None:
                st.session_state['mock_import_nonce'] = st.session_state.get('mock_import_nonce', 0) + 1
                st.session_state['mock_msg'] = "\n\n".join(msgs)
                st.rerun()
    if b2.button("💾 전체저장", use_container_width=True):
        results = mock_save_all(acct_no)
        if results is not None:
            st.session_state['mock_saved_files'] = results
            st.session_state['mock_msg'] = "전체저장 완료" + (
                "" if all(r['saved_to_table'] for r in results) else
                " (CSV만 저장 — _save 테이블이 없어 DB 스냅샷은 생략)")
            st.rerun()
    if b3.button("🗑️ 전체삭제", use_container_width=True):
        confirm_delete_all()

    # 전체저장 다운로드 패널
    saved = st.session_state.get('mock_saved_files')
    if saved:
        with st.container(border=True):
            h1, h2 = st.columns([6, 1])
            h1.markdown("**📥 저장된 파일 다운로드**")
            if h2.button("✕", key="mock_saved_close"):
                st.session_state.pop('mock_saved_files', None)
                st.rerun()
            cols = st.columns(len(saved))
            for col, r in zip(cols, saved):
                path = os.path.join(MOCK_EXPORT_DIR, r['file'])
                if os.path.exists(path):
                    with open(path, 'rb') as fin:
                        col.download_button(f"{r['table']} ({r['rows']}건)", fin.read(), file_name=r['file'],
                                            mime='text/csv', key=f"mock_dl_{r['file']}", use_container_width=True)


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
def save_trade(tracker_id, trade_dt, slot, trade_tp, price, qty, reason=None):
    reason = (reason or '').strip() or None
    return run_write([
        ("""INSERT INTO public.mock_tracker_trade (tracker_id, trade_dt, slot, trade_tp, price, qty, reason, crt_dt)
            VALUES (%s, %s, %s, %s, %s, %s, %s, now())""", (tracker_id, trade_dt, slot, trade_tp, price, qty, reason)),
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
        realized = realized_rt = None   # 매도 거래만: 실현손익, 평균단가 대비 수익률(%)
        if t.trade_tp == 'B':
            cash -= amount
            p['qty'] += t.qty
            p['cost'] += amount
        else:
            avg = p['cost'] / p['qty'] if p['qty'] else 0
            realized = (t.price - avg) * t.qty
            realized_total += realized
            realized_rt = (t.price / avg - 1) * 100 if avg else None
            p['cost'] -= avg * t.qty
            p['qty'] -= t.qty
            cash += amount
        last_px[t.slot] = t.price
        equity = cash + sum(v['qty'] * last_px.get(s, 0) for s, v in pos.items())
        curve.append((f"{t.slot}{'매수' if t.trade_tp == 'B' else '매도'}", (equity / init_cash - 1) * 100))
        history.append({'id': t.id, 'trade_dt': t.trade_dt, 'trade_tp': t.trade_tp, 'slot': t.slot, 'qty': t.qty,
                        'price': t.price, 'amount': amount, 'realized': realized, 'realized_rt': realized_rt,
                        'cash': cash, 'reason': t.reason,
                        'check_mode': t.check_mode, 'result_check': t.result_check})
    return cash, pos, realized_total, pd.DataFrame(history), curve


def fmt_dt(d):
    return f"{d[:4]}-{d[4:6]}-{d[6:]}" if d and len(d) == 8 else d


# 매매통계: 계좌의 전체 트래커(종목) 매매를 종목별로 재생해 매도 시점 실현손익을 구한 뒤 기간별로 합산
STAT_PERIODS = {'일별': 'D', '주별': 'W', '월별': 'M'}


def load_all_trade_history(acct_no, trackers):
    """트래커별 replay 결과(거래내역)에 종목명/초기자금을 붙여 하나의 DataFrame 으로 반환"""
    if not trackers:
        return pd.DataFrame()
    cur = conn.cursor()
    cur.execute("""
        SELECT tracker_id, id, trade_dt, slot, trade_tp, price, qty, reason, check_mode, result_check
          FROM public.mock_tracker_trade
         WHERE tracker_id = ANY(%s) ORDER BY tracker_id, trade_dt, id
    """, ([t[0] for t in trackers],))
    rows = cur.fetchall()
    cur.close()
    all_trades = pd.DataFrame(rows, columns=['tracker_id', 'id', 'trade_dt', 'slot', 'trade_tp', 'price', 'qty',
                                             'reason', 'check_mode', 'result_check'])
    if all_trades.empty:
        return pd.DataFrame()
    all_trades['price'] = pd.to_numeric(all_trades['price']).astype(float)
    frames = []
    for tid, code_, name_, cash0 in trackers:
        tr = all_trades[all_trades['tracker_id'] == tid]
        if tr.empty:
            continue
        hist = replay(tr, float(cash0))[3]
        hist['name'] = name_
        frames.append(hist)
    df = pd.concat(frames, ignore_index=True)
    df['date'] = pd.to_datetime(df['trade_dt'], format='%Y%m%d', errors='coerce')
    df['realized'] = pd.to_numeric(df['realized'])
    df['realized_rt'] = pd.to_numeric(df['realized_rt'])
    return df.dropna(subset=['date'])


def period_label(dates, unit):
    if unit == 'D':
        return dates.dt.strftime('%Y-%m-%d')
    if unit == 'W':   # 해당 주 월요일 기준
        return (dates - pd.to_timedelta(dates.dt.weekday, unit='D')).dt.strftime('%Y-%m-%d') + ' 주'
    return dates.dt.strftime('%Y-%m')


def build_positions(df_all):
    """종목별 포지션 구성 (kis_balance_info.py 의 종목별 손익 / 첫 매도 손익 조회와 같은 기준)
    - 포지션: 종목의 전체 슬롯 합산 보유수량이 0 에서 매수로 시작해 다시 0 이 될 때까지
    - 최종 손익: 전량 청산된 포지션의 모든 매도 실현손익 합 (청산일 기준, 미청산 포지션 제외)
    - 첫매도 손익: 포지션 진입 후 첫 번째 매도 1건의 실현손익 (첫매도일 기준, 전량 청산 여부 무관)
    실현손익은 매도 시점 슬롯 평균단가 기준(replay), 수수료·세금 미반영"""
    rows = []
    for name_, g in df_all.sort_values(['name', 'date', 'id']).groupby('name', sort=False):
        held, p = 0, None
        for t in g.itertuples():
            if t.trade_tp == 'B':
                if held == 0:   # 새 포지션 진입
                    p = {'name': name_, 'entry_dt': t.date, 'exit_dt': None, 'buy_cnt': 0, 'sell_cnt': 0,
                         'buy_amt': 0.0, 'pnl': 0.0, 'cost_sold': 0.0,
                         'first_dt': None, 'first_pnl': None, 'first_rt': None, 'first_buy_cnt': 0}
                    rows.append(p)
                held += t.qty
                p['buy_cnt'] += 1
                p['buy_amt'] += t.amount
            elif p is not None:
                held -= t.qty
                p['sell_cnt'] += 1
                p['pnl'] += t.realized
                p['cost_sold'] += t.amount - t.realized    # 매도분의 평균단가 기준 원가
                if p['first_dt'] is None:
                    p['first_dt'], p['first_pnl'], p['first_rt'] = t.date, t.realized, t.realized_rt
                    p['first_buy_cnt'] = p['buy_cnt']
                if held <= 0:
                    held, p['exit_dt'] = 0, t.date
    pos = pd.DataFrame(rows)
    if not pos.empty:
        pos['final_rt'] = pos['pnl'] / pos['cost_sold'].where(pos['cost_sold'] != 0) * 100
    return pos


def render_trade_stats(acct_no, trackers):
    df_all = load_all_trade_history(acct_no, trackers)
    with st.container(border=True):
        st.markdown("**● 매매통계**")
        if df_all.empty:
            st.caption("매매 내역이 없습니다")
            return

        first_day, last_day = df_all['date'].min().date(), max(df_all['date'].max().date(), datetime.now().date())
        f1, f2, f3 = st.columns([2.2, 2.4, 2.4])
        unit_label = f1.radio("기간 단위", list(STAT_PERIODS), index=2, horizontal=True, key="mock_stat_unit")
        rng = f2.date_input("매매기간", (first_day, last_day), min_value=first_day, max_value=last_day,
                            format="YYYY-MM-DD", key="mock_stat_range")
        basis = f3.radio("차트·종목별 기준", ['최종 손익', '첫매도 손익'], horizontal=True, key="mock_stat_basis")
        start, end = (rng[0], rng[1]) if isinstance(rng, (tuple, list)) and len(rng) == 2 else (first_day, last_day)

        pos = build_positions(df_all)
        if pos.empty:
            st.caption("매도(실현손익) 내역이 없습니다")
            return

        def in_range(col):
            # Timestamp 끼리 비교 (값이 전부 NaT 인 경우 .dt.date 가 datetime64 로 남아 date 와 비교 시 TypeError)
            d = pd.to_datetime(pos[col])
            return pos[d.notna() & (d >= pd.Timestamp(start)) & (d < pd.Timestamp(end) + pd.Timedelta(days=1))]

        # 기준별 대상: 최종 = 기간 내 청산된 포지션, 첫매도 = 기간 내 첫매도가 있는 포지션
        final = in_range('exit_dt').assign(dt=lambda x: x['exit_dt'], pl=lambda x: x['pnl'], rt=lambda x: x['final_rt'])
        first = in_range('first_dt').assign(dt=lambda x: x['first_dt'], pl=lambda x: x['first_pnl'],
                                            rt=lambda x: x['first_rt'])
        open_cnt = int(pos['exit_dt'].isna().sum())

        # 요약 카드 (st.metric 은 긴 값이 '…' 로 잘리므로 HTML 카드, 보조 정보는 아래 작은 글씨)
        def tile(label, value, sub='', color=''):
            style = f" style='color:{color}'" if color else ''
            return (f"<div class='mt-stat'><div class='lbl'>{label}</div><div class='val'{style}>{value}</div>"
                    f"<div class='sub'>{sub}</div></div>")

        def stat_row(title, d, is_final):
            wins, losses = d[d['pl'] > 0], d[d['pl'] < 0]
            # 매매 건수: 최종 = 포지션의 전체 매수/매도, 첫매도 = 첫매도까지의 매수 + 첫매도 1건
            buy_n = int(d['buy_cnt'].sum() if is_final else d['first_buy_cnt'].sum())
            sell_n = int(d['sell_cnt'].sum()) if is_final else len(d)
            n = len(d)
            total = d['pl'].sum()
            avg_win = wins['pl'].mean() if not wins.empty else 0.0
            avg_loss = abs(losses['pl'].mean()) if not losses.empty else 0.0
            payoff = f"{avg_win / avg_loss:.2f}" if avg_loss > 0 else '—'
            win_rt = f"{wins['rt'].mean():+.2f}%" if not wins.empty else '—'
            loss_rt = f"{losses['rt'].mean():+.2f}%" if not losses.empty else '—'
            color = UP_COLOR if total > 0 else DOWN_COLOR if total < 0 else ''
            st.markdown(f"<div class='mt-stat-title'>{title}</div><div class='mt-stat-row'>" + "".join([
                tile('승률 (건수)', f"{len(wins) / n * 100:.1f}% ({n}건)" if n else '—',
                     f"수익 {len(wins)}건 / 손실 {len(losses)}건"),
                tile('손익비 (평균수익/평균손실)', payoff, f"{avg_win:,.0f}원 / {avg_loss:,.0f}원"),
                tile('실현손익 합계', signed(total, '원') if n else '—', f"평균 수익률 {win_rt} / 손실률 {loss_rt}", color),
                tile('매매 건수', f"{buy_n + sell_n:,}건",
                     f"매수 {buy_n} · {'매도' if is_final else '첫매도'} {sell_n}"),
            ]) + "</div>", unsafe_allow_html=True)

        stat_row("📉 첫매도 손익 <span>(포지션 진입 후 첫 번째 매도 기준, 전량 청산 여부 무관)</span>", first, False)
        stat_row(f"📊 최종 손익합산 <span>(전량 청산된 포지션의 매도 손익 합산 · 미청산 {open_cnt}건 제외)</span>",
                 final, True)

        sel = final if basis == '최종 손익' else first
        if sel.empty:
            st.caption(f"선택한 기간에 {basis} 내역이 없습니다")
            return

        # 기간별 손익 막대 + 누적 손익 선 (같은 원 단위 축). 최종=청산일, 첫매도=첫매도일 기준
        unit = STAT_PERIODS[unit_label]
        by_period = (sel.assign(period=period_label(pd.to_datetime(sel['dt']), unit))
                     .groupby('period').agg(pl=('pl', 'sum'), cnt=('pl', 'size'),
                                            wins=('pl', lambda s: int((s > 0).sum())),
                                            losses=('pl', lambda s: int((s < 0).sum())))
                     .reset_index().sort_values('period'))
        by_period['cum'] = by_period['pl'].cumsum()
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=by_period['period'], y=by_period['pl'], name=f'기간 {basis}', showlegend=False,
            marker=dict(color=[UP_COLOR if v >= 0 else DOWN_COLOR for v in by_period['pl']], cornerradius=4),
            customdata=by_period[['cnt', 'wins', 'losses']],
            hovertemplate=f"%{{x}}<br>{basis} %{{y:+,.0f}}원<br>%{{customdata[0]}}건 "
                          "(수익 %{customdata[1]} / 손실 %{customdata[2]})<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=by_period['period'], y=by_period['cum'], name=f'누적 {basis}', mode='lines+markers',
            line=dict(color='#475569', width=2), marker=dict(size=8),
            hovertemplate="%{x}<br>누적 %{y:+,.0f}원<extra></extra>",
        ))
        fig.update_xaxes(type='category')
        fig.update_yaxes(tickformat=',.0f', ticksuffix='원', zeroline=True, zerolinecolor='#94a3b8')
        fig.update_layout(height=280, margin=dict(l=10, r=10, t=30, b=10), bargap=0.35, hovermode='closest',
                          title=dict(text=f"기간별 {basis} (막대: 이익 초록 / 손실 빨강)", font=dict(size=13), x=0),
                          legend=dict(orientation='h', yanchor='bottom', y=1.02, x=1, xanchor='right'))
        st.plotly_chart(fig, use_container_width=True, key="mock_stat_chart")

        # 종목별 합산 (선택 기준)
        by_name = (sel.groupby('name').agg(
            건수=('pl', 'size'), 수익=('pl', lambda s: int((s > 0).sum())), 손실=('pl', lambda s: int((s < 0).sum())),
            실현손익=('pl', 'sum'), 평균수익률=('rt', 'mean'))
            .reset_index().rename(columns={'name': '종목명', '평균수익률': '평균 수익률'}))
        by_name['승률'] = by_name['수익'] / by_name['건수'] * 100
        by_name = by_name[['종목명', '건수', '수익', '손실', '승률', '실현손익', '평균 수익률']] \
            .sort_values('실현손익', ascending=False)
        st.dataframe(
            by_name.style.format({'실현손익': '{:+,.0f}원', '승률': '{:.1f}%',
                                  '평균 수익률': lambda v: '—' if pd.isna(v) else f"{v:+.2f}%"})
            .map(pl_color, subset=['실현손익', '평균 수익률']),
            hide_index=True, use_container_width=True)
        st.caption("포지션은 종목의 전체 슬롯 합산 보유수량이 0에서 매수로 시작해 다시 0이 될 때까지입니다. "
                   "최종 손익은 청산일, 첫매도 손익은 첫매도일 기준으로 기간에 합산되며, 실현손익은 매도 시점 "
                   "슬롯 평균단가 기준(수수료·세금 미반영)입니다.")


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

st.header("150개 형성된 봉 기준으로 마지막봉을 보고 판단(모의투자)")

# 가져오기 / 전체저장 / 전체삭제 (현재 계좌 범위)
render_data_tools(acct_no, my_choice)

if 'mock_msg' in st.session_state:
    st.success(st.session_state.pop('mock_msg'))

# 트래커(종목) 선택 / 신규 생성
cur04 = conn.cursor()
cur04.execute("SELECT id, code, name, init_cash FROM public.mock_tracker WHERE acct_no = %s ORDER BY crt_dt DESC",
              (str(acct_no),))
trackers = cur04.fetchall()
cur04.close()

# 매매통계 대시보드 (닉네임 계좌의 전체 종목, 매매기간별 합산 실현손익)
render_trade_stats(acct_no, trackers)

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
        SELECT id, trade_dt, slot, trade_tp, price, qty, reason, check_mode, result_check
          FROM public.mock_tracker_trade
         WHERE tracker_id = %s ORDER BY trade_dt, id
    """, (tracker_id,))
    trades = pd.DataFrame(cur05.fetchall(), columns=['id', 'trade_dt', 'slot', 'trade_tp', 'price', 'qty', 'reason',
                                                     'check_mode', 'result_check'])
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
            c1, c2, c3, c4, c5, c6 = st.columns([1, 1.3, 2, 2, 1.2, 0.9], vertical_alignment="bottom")
        else:
            c1, c2, c3, c4, c6 = st.columns([1, 1.3, 2, 3.8, 0.9], vertical_alignment="bottom")
        # 사유는 거래 입력 행 아래 줄에 전체 너비로 표시 (매수/매도 버튼 처리 전에 값을 읽어야 하므로 여기서 생성)
        reason = st.container().text_area("사유", max_chars=500, placeholder="매매 사유 (선택)", height=100,
                                          key=f"mock_reason_{tracker_id}_{nonce}")
        slot = c1.selectbox("슬롯", SLOTS, key=f"mock_slot_{tracker_id}")
        trade_date = c2.date_input("매매일자", datetime.now(), format="YYYY-MM-DD", key=f"mock_dt_{tracker_id}")
        price = c3.container(key=f"mt_price_box_{tracker_id}").number_input("단가 (원)", min_value=0, step=100, value=None, placeholder="0",
                                key=f"mock_price_{tracker_id}_{nonce}")

        if sell_mode:
            qty_key = f"mock_sell_qty_{tracker_id}_{nonce}"
            sell_qty = c4.number_input("매도 수량", min_value=0, max_value=hold['qty'], step=1, value=None,
                                       placeholder="전량", key=qty_key)
            c5.button("전량", use_container_width=True, key=f"mock_all_{tracker_id}_{nonce}",
                      on_click=lambda k=qty_key, q=hold['qty']: st.session_state.__setitem__(k, q))
            if c6.button("매도", type="primary", use_container_width=True, key=f"mock_sell_{tracker_id}"):
                qty = int(sell_qty) if sell_qty else hold['qty']
                if not price:
                    st.error("단가를 입력하세요.")
                elif qty <= 0 or qty > hold['qty']:
                    st.error(f"매도 수량은 1~{hold['qty']:,}주 사이여야 합니다.")
                elif save_trade(tracker_id, trade_date.strftime('%Y%m%d'), slot, 'S', price, qty, reason):
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
            c4.markdown(f"<div class='mt-expect'><div>{expect}</div></div>",
                        unsafe_allow_html=True)
            if c6.button("매수", type="primary", use_container_width=True, key=f"mock_buy_{tracker_id}"):
                if not price:
                    st.error("단가를 입력하세요.")
                elif buy_qty <= 0:
                    st.error("가용 현금이 부족합니다.")
                elif save_trade(tracker_id, trade_date.strftime('%Y%m%d'), slot, 'B', price, buy_qty, reason):
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
        # 탭 전환 시 재실행하고 선택된 탭만 그린다(.open). 거래내역 AG-Grid 가 숨겨진(비활성) 탭에서 생성되면
        # 폭을 0으로 측정해 가로 스크롤바가 나오지 않으므로, 탭이 보일 때 그리드를 생성하기 위함.
        tab_hold, tab_hist = st.tabs(["보유종목", "거래내역"], key="mock_hold_hist_tabs", on_change="rerun")
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
            if not tab_hist.open:
                pass   # 거래내역 탭이 선택되지 않았으면 그리지 않음 (탭 선택 시 재실행되어 그려짐)
            elif df_hist.empty:
                st.caption("거래 내역이 없습니다")
            else:
                df_view = pd.DataFrame({
                    'id': df_hist['id'].astype(int),
                    '매매일자': df_hist['trade_dt'].map(fmt_dt),
                    '구분': df_hist['trade_tp'].map({'B': 'BUY', 'S': 'SELL'}),
                    '슬롯': df_hist['slot'], '수량': df_hist['qty'], '단가': df_hist['price'],
                    '거래금액': df_hist['amount'],
                    '실현손익': pd.to_numeric(df_hist['realized']).round(0),
                    '수익률': pd.to_numeric(df_hist['realized_rt']).round(2), '잔여현금': df_hist['cash'],
                    '사유': df_hist['reason'].fillna(''),
                    '판단모드': df_hist['check_mode'].map(lambda c: CHECK_MODES.get((c or '').strip(), '')),
                    '판단결과': df_hist['result_check'].fillna(''),
                }).iloc[::-1].reset_index(drop=True)
                df_view['_apply_nonce'] = 0.0

                number_fmt_js = JsCode("""
                    function(p) { return (p.value === null || p.value === undefined) ? '' :
                                  Math.round(p.value).toLocaleString(); }
                """)
                signed_won_js = JsCode("""
                    function(p) { if (p.value === null || p.value === undefined || isNaN(p.value)) return '—';
                                  return (p.value > 0 ? '+' : '') + Math.round(p.value).toLocaleString() + '원'; }
                """)
                signed_pct_js = JsCode("""
                    function(p) { if (p.value === null || p.value === undefined || isNaN(p.value)) return '—';
                                  return (p.value > 0 ? '+' : '') + p.value.toFixed(2) + '%'; }
                """)
                pl_style_js = JsCode(f"""
                    function(p) {{ if (!p.value) return null;
                                   return {{color: p.value > 0 ? '{UP_COLOR}' : '{DOWN_COLOR}'}}; }}
                """)
                tp_style_js = JsCode(f"""
                    function(p) {{ return {{color: p.value === 'BUY' ? '{UP_COLOR}' : '{DOWN_COLOR}',
                                           fontWeight: 700}}; }}
                """)
                # 열 폭: 표시 문자열(헤더 포함) 중 가장 긴 것 기준으로 계산해 고정
                # (거래내역은 탭 안이라 숨겨진 상태에서 그려지면 JS 자동 맞춤이 폭을 재지 못하므로 Python 에서 계산)
                def text_px(s):
                    s = str(s).replace('\n', ' ')
                    return sum(14 if ord(ch) > 0x2E80 else 8 for ch in s)

                def fmt_cell(col, v):
                    if v is None or (isinstance(v, float) and pd.isna(v)):
                        return '—'
                    if col in ('수량', '단가', '거래금액', '잔여현금'):
                        return f"{v:,.0f}"
                    if col == '실현손익':
                        return f"{v:+,.0f}원"
                    if col == '수익률':
                        return f"{v:+.2f}%"
                    return str(v)

                col_widths = {
                    col: min(1200, max([text_px(col) + 50] + [text_px(fmt_cell(col, v)) + 30 for v in df_view[col]]))
                    for col in df_view.columns if col not in ('id', '_apply_nonce')
                }

                gb = GridOptionsBuilder.from_dataframe(df_view)
                gb.configure_default_column(editable=False, sortable=True, resizable=True)
                gb.configure_column('id', hide=True)
                gb.configure_column('_apply_nonce', hide=True)
                gb.configure_column('구분', cellStyle=tp_style_js)
                for col in ['수량', '단가', '거래금액', '잔여현금']:
                    gb.configure_column(col, type=['numericColumn'], valueFormatter=number_fmt_js)
                gb.configure_column('실현손익', type=['numericColumn'], valueFormatter=signed_won_js, cellStyle=pl_style_js)
                gb.configure_column('수익률', type=['numericColumn'], valueFormatter=signed_pct_js, cellStyle=pl_style_js)
                # 편집 항목: 사유·판단결과는 여러 줄 텍스트 팝업, 판단모드는 콤보박스
                gb.configure_column('사유', editable=True, cellEditor='agLargeTextCellEditor', cellEditorPopup=True,
                                    cellEditorParams={'maxLength': 500, 'rows': 6, 'cols': 50},
                                    tooltipField='사유', cellStyle={'backgroundColor': '#fff8e1'})
                gb.configure_column('판단모드', editable=True, cellEditor='agSelectCellEditor',
                                    cellEditorParams={'values': [''] + list(CHECK_MODES.values())},
                                    singleClickEdit=True, cellStyle={'backgroundColor': '#fff8e1'})
                gb.configure_column('판단결과', editable=True, cellEditor='agLargeTextCellEditor', cellEditorPopup=True,
                                    cellEditorParams={'maxLength': 2000, 'rows': 6, 'cols': 50},
                                    tooltipField='판단결과', cellStyle={'backgroundColor': '#fff8e1'})
                for col, w in col_widths.items():
                    gb.configure_column(col, width=w, minWidth=w, suppressSizeToFit=True)
                # 매매일자·구분은 좌측 고정, 나머지 항목만 가로 스크롤
                for col in ('매매일자', '구분'):
                    gb.configure_column(col, pinned='left', lockPinned=True)
                # 열 폭 합계가 화면보다 넓으면 가로 스크롤
                # (from_dataframe 이 기본으로 넣는 autoSizeStrategy=fitGridWidth 는 열을 화면 폭에 강제로 맞추므로 해제)
                gb.configure_grid_options(
                    autoSizeStrategy=None,
                    statusBar={'statusPanels': [{'statusPanel': APPLY_EDITS_PANEL_JS, 'align': 'left'}]},
                    tooltipShowDelay=300,
                    suppressColumnVirtualisation=True, alwaysShowHorizontalScroll=True,
                )
                gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)

                st.caption("판단모드는 셀을 클릭해 콤보박스에서 선택하고, 사유·판단결과는 셀을 더블클릭해 입력한 뒤 "
                           "그리드 하단 [변경 적용]을 누르면 저장됩니다.")
                grid_response = AgGrid(
                    df_view,
                    gridOptions=gb.build(),
                    # 높이는 행 수(페이지당 최대 20행)로 직접 계산: 헤더 33 + 행 29×N + 페이지바 29 + 상태바 35 + 가로스크롤 16 + 여유
                    # (height=None 자동 높이는 탭 전환 후 생성될 때 iframe 높이가 0이 되어 그리드가 보이지 않음)
                    height=33 + 29 * min(len(df_view), 20) + 29 + 35 + 16 + 12,
                    allow_unsafe_jscode=True,
                    use_container_width=True,
                    update_mode=GridUpdateMode.NO_UPDATE,
                    update_on=['filterChanged'],     # [변경 적용] 버튼이 발생시키는 이벤트에서만 값 반환
                    enable_enterprise_modules=True,  # 상태바 버튼 위해 필요
                    key=f"mock_hist_grid_{tracker_id}",
                )

                # [변경 적용] 클릭으로 들어온 새 요청일 때만 편집 전/후 비교로 변경된 사유/판단모드/판단결과 저장
                edited_df = pd.DataFrame(grid_response['data'])
                if is_new_apply_request(edited_df, f"mock_hist_apply_{tracker_id}") and 'id' in edited_df.columns:
                    orig = df_view.set_index('id')
                    updates = []
                    for _, row in edited_df.iterrows():
                        tid = int(row['id'])
                        if tid not in orig.index:
                            continue
                        new_mode = row.get('판단모드')
                        new_mode = new_mode.strip() if isinstance(new_mode, str) else ''
                        new_result = row.get('판단결과')
                        new_result = new_result.strip() if isinstance(new_result, str) else ''
                        new_reason = row.get('사유')
                        new_reason = new_reason.strip() if isinstance(new_reason, str) else ''
                        if (new_mode != orig.at[tid, '판단모드'] or new_result != orig.at[tid, '판단결과'].strip()
                                or new_reason != orig.at[tid, '사유'].strip()):
                            updates.append((
                                """UPDATE public.mock_tracker_trade
                                      SET reason = %s, check_mode = %s, result_check = %s, mod_dt = now()
                                    WHERE id = %s""",
                                (new_reason or None, CHECK_MODE_CODES.get(new_mode), new_result or None, tid)))
                    if not updates:
                        st.info("변경된 항목이 없습니다.")
                    elif run_write(updates, "판단 저장"):
                        st.session_state['mock_msg'] = f"거래 {len(updates)}건의 사유/판단모드/판단결과가 저장되었습니다."
                        st.rerun()

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
