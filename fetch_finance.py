#!/usr/bin/env python3
"""
fetch_finance.py  —  종목 재무 데이터 → wiki 자동 업데이트
────────────────────────────────────────────────────────
데이터 소스
  - FinanceDataReader (FDR) : 주가 (OHLCV)
  - dart-fss               : 연간·분기 재무제표 (DART 공시)
  - 계산                   : PER = 주가/EPS, PBR = 주가/BPS, 시가총액 = 주가×주식수

사용법
  python fetch_finance.py 삼성전자              # 기본 (작년 회계연도)
  python fetch_finance.py 005930                # 종목코드 직접 입력
  python fetch_finance.py 삼성전자 --dry-run    # wiki 수정 없이 미리보기
  python fetch_finance.py 삼성전자 --year 2025  # 특정 회계연도 지정
  python fetch_finance.py --all                 # wiki/stocks/ 전체 종목 일괄 업데이트

환경변수 (.env 파일 또는 직접 설정)
  DART_API_KEY=0c427a2ab7993504425917100251964bdae972dd

DART 공시 시차
  - 연간보고서(사업보고서): 회계연도 종료 후 3개월 이내 제출 (예: 2025 회계연도 → 2026년 3월)
  - 분기보고서: 분기 종료 후 45일 이내 제출
"""

import os, sys, re, argparse
from datetime import datetime, timedelta
from pathlib import Path

# ── .env 로드 ─────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

DART_API_KEY = os.environ.get("DART_API_KEY", "")
WIKI_ROOT    = Path(__file__).parent / "wiki" / "stocks"


# ══════════════════════════════════════════════════════════════════════
#  유틸리티
# ══════════════════════════════════════════════════════════════════════

def _data_cols(df):
    """DataFrame 컬럼에서 실제 데이터 컬럼(날짜)만 추출.
    메타데이터 컬럼: second element = str ('label_ko', 'class0' 등)
    데이터 컬럼    : second element = tuple (('연결재무제표',) 등)
    """
    return [c for c in df.columns if isinstance(c, tuple) and isinstance(c[1], tuple)]


def _label_col(df):
    """DataFrame 컬럼에서 label_ko 컬럼 반환"""
    for c in df.columns:
        if isinstance(c, tuple) and c[1] == 'label_ko':
            return c
    return None


def _label_map(df):
    """label_ko → row 인덱스 딕셔너리 반환"""
    lc = _label_col(df)
    if lc is None:
        return {}
    return {str(df[lc].iloc[i]): i for i in range(len(df))}


def _get_val(df, label_map, date_col, candidates):
    """후보 label 이름 순서대로 시도해 첫 번째로 찾은 값(float) 반환. 실패 시 None."""
    for name in candidates:
        row_i = label_map.get(name)
        if row_i is not None:
            try:
                v = df[date_col].iloc[row_i]
                if v is not None and str(v) not in ('nan', 'None', ''):
                    return float(v)
            except Exception:
                pass
    return None


def _fmt(value, unit='조'):
    """숫자(원 단위) → '12.3조' or '1,234억' 포맷."""
    if value is None:
        return '-'
    try:
        v = float(value)
        if unit == '조':
            if abs(v) >= 1e12:
                return f"{round(v / 1e12, 1)}조"
            elif abs(v) >= 1e8:
                return f"{round(v / 1e8):,}억"
        return f"{v:,.0f}"
    except Exception:
        return '-'


def _fmt_ratio(value):
    """부채비율 등 퍼센트 포맷."""
    if value is None:
        return '-'
    try:
        return f"{round(float(value), 1)}%"
    except Exception:
        return '-'


# ══════════════════════════════════════════════════════════════════════
#  1. 종목코드 조회 (FDR → 주가 포함)
# ══════════════════════════════════════════════════════════════════════

def get_stock_info(name_or_code: str) -> dict:
    """종목명 or 6자리 코드 → {code, name, price, price_date}"""
    import FinanceDataReader as fdr

    # 종목코드 결정
    if re.match(r"^\d{6}$", name_or_code):
        code = name_or_code
        name = code
    else:
        # FDR StockListing은 현재 불안정 → DART에서 종목코드 찾기
        # 일단 name을 그대로 사용하고, DART에서 name으로 검색
        code = None
        name = name_or_code

    # 주가 조회 (최근 5거래일)
    price, price_date = None, None
    if code:
        try:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=10)
            df = fdr.DataReader(code, start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d'))
            if not df.empty:
                price = float(df['Close'].iloc[-1])
                price_date = df.index[-1].strftime('%Y-%m-%d')
        except Exception as e:
            print(f"  [WARN] FDR 주가 조회 실패: {e}")

    return {'code': code, 'name': name, 'price': price, 'price_date': price_date}


# ══════════════════════════════════════════════════════════════════════
#  2. DART 재무 데이터 (연결 기준)
# ══════════════════════════════════════════════════════════════════════

def _setup_dart():
    """dart-fss 초기화. 스피너 Unicode 에러 억제."""
    if not DART_API_KEY:
        raise ValueError("DART_API_KEY 없음. .env 파일 또는 --dart-key 옵션으로 설정하세요.")
    import dart_fss as dart
    dart.set_api_key(DART_API_KEY)
    return dart


def _find_corp(dart, name_or_code: str):
    """종목명 or 코드로 DART Corp 객체 반환. 코드 없으면 name으로 검색."""
    corps = dart.get_corp_list()

    if re.match(r"^\d{6}$", name_or_code):
        corp = corps.find_by_stock_code(name_or_code)
        if corp is None:
            raise ValueError(f"DART에서 코드 {name_or_code} 미발견")
        return corp, name_or_code, corp.corp_name

    # 이름 검색
    results = corps.find_by_corp_name(name_or_code, exactly=True)
    if not results:
        results = corps.find_by_corp_name(name_or_code, exactly=False)
    if not results:
        raise ValueError(f"DART에서 '{name_or_code}' 기업 미발견")
    corp = results[0] if isinstance(results, list) else results
    code = getattr(corp, 'stock_code', '') or ''
    return corp, code.strip(), corp.corp_name


def fetch_dart_annual(corp, year: int) -> dict:
    """연간 재무제표 (연결 기준) 추출. 반환: {매출, 영업이익, 순이익, EPS, 부채비율, BPS}"""
    result = {}
    try:
        # 연간보고서는 다음 해 1~4월에 제출
        fs = corp.extract_fs(
            bgn_de=f"{year + 1}0101",
            end_de=f"{year + 1}0430",
            report_tp='annual',
            separate=False,
        )
    except Exception as e:
        print(f"  [WARN] annual {year} 연결 조회 실패: {e}")
        return result

    # ── 손익계산서 (is 없으면 cis 포괄손익계산서 fallback) ──
    try:
        inc = fs['is']
        if inc is None:
            inc = fs['cis']
        if inc is None:
            raise ValueError("IS/CIS 모두 없음")
        dc = _data_cols(inc)
        if not dc:
            raise ValueError("IS 데이터 컬럼 없음")
        latest = dc[0]  # 가장 최근 (요청한 회계연도)
        lm = _label_map(inc)

        result['매출']   = _get_val(inc, lm, latest, ['매출액', '수익(매출액)', '영업수익', '매출'])
        result['영업이익'] = _get_val(inc, lm, latest, ['영업이익', '영업이익(손실)'])
        result['순이익']  = _get_val(inc, lm, latest, ['당기순이익', '당기순이익(손실)', '계속영업당기순이익'])
        result['EPS']   = _get_val(inc, lm, latest, ['기본주당이익', '기본주당순이익'])
    except Exception as e:
        print(f"  [WARN] IS 파싱 실패: {e}")

    # ── 재무상태표 ──
    try:
        bs = fs['bs']
        dc_bs = _data_cols(bs)
        if dc_bs:
            latest_bs = dc_bs[0]
            lm_bs = _label_map(bs)
            debt  = _get_val(bs, lm_bs, latest_bs, ['부채총계', '부채 합계'])
            equity = _get_val(bs, lm_bs, latest_bs, ['자본총계', '자본 합계', '지배기업 소유주지분'])
            if debt is not None and equity is not None and equity != 0:
                result['부채비율_raw'] = debt / equity * 100
            shares = _get_total_shares(result)
            if shares and equity:
                result['BPS'] = equity / shares
    except Exception as e:
        print(f"  [WARN] BS 파싱 실패: {e}")

    # 연결 BS가 None인 경우 별도 BS로 fallback
    if result.get('부채비율_raw') is None:
        try:
            fs_sep = corp.extract_fs(
                bgn_de=f"{year + 1}0101",
                end_de=f"{year + 1}0430",
                report_tp='annual',
                separate=True,
            )
            bs_sep = fs_sep['bs']
            dc_sep = _data_cols(bs_sep)
            if dc_sep:
                lm_sep = _label_map(bs_sep)
                debt  = _get_val(bs_sep, lm_sep, dc_sep[0], ['부채총계'])
                equity = _get_val(bs_sep, lm_sep, dc_sep[0], ['자본총계'])
                if debt is not None and equity is not None and equity != 0:
                    result['부채비율_raw'] = debt / equity * 100
                    shares = _get_total_shares(result)
                    if shares and equity:
                        result['BPS'] = equity / shares
                    result['BS_별도'] = True
        except Exception as e:
            print(f"  [WARN] 별도 BS fallback 실패: {e}")

    return result


def _get_total_shares(fin_data: dict):
    """EPS와 순이익으로 총 발행주식수 추정."""
    eps = fin_data.get('EPS')
    net = fin_data.get('순이익')
    if eps and net and eps != 0:
        return net / eps
    return None


def fetch_dart_quarters(corp, year: int, annual: dict = None) -> list:
    """분기 재무제표 (연결 기준) 추출. 반환: [{분기, 매출, 영업이익, 순이익, 부채비율}, ...]
    annual: fetch_dart_annual() 결과. Q4 = 연간 - 9개월 누적 계산에 사용.
    """
    records = []
    try:
        fs = corp.extract_fs(
            bgn_de=f"{year}0101",
            end_de=f"{year}1231",
            report_tp='quarter',
            separate=False,
        )
    except Exception as e:
        print(f"  [WARN] quarter {year} 조회 실패: {e}")
        return records

    try:
        inc = fs['is']
        if inc is None:
            inc = fs['cis']
        if inc is None:
            raise ValueError("IS/CIS 모두 없음")
        dc = _data_cols(inc)
        lm = _label_map(inc)

        IS_KEYS = {
            '매출':    ['매출액', '수익(매출액)', '영업수익', '매출'],
            '영업이익': ['영업이익', '영업이익(손실)'],
            '순이익':  ['당기순이익', '당기순이익(손실)'],
        }

        # 3개월짜리 단독 분기 컬럼 식별 (형식: 'YYYYMMDD-YYYYMMDD', 85~100일 범위)
        q_cols = {}
        cum9_col = None  # 9개월 누적 컬럼 (Q4 계산용)
        for col in dc:
            period_str = col[0]
            if not isinstance(period_str, str) or '-' not in period_str:
                continue
            parts = period_str.split('-')
            if len(parts) != 2:
                continue
            s, e = parts[0].strip(), parts[1].strip()
            if len(s) != 8 or len(e) != 8:
                continue
            try:
                sd = datetime.strptime(s, '%Y%m%d')
                ed = datetime.strptime(e, '%Y%m%d')
                days = (ed - sd).days
                if sd.year == year and sd.month == 1 and '0930' in e:
                    cum9_col = col  # 9개월 누적 (1월~9월)
                if 85 <= days <= 100 and sd.year == year:
                    q_map = {1: 'Q1', 4: 'Q2', 7: 'Q3', 10: 'Q4'}
                    q_key = q_map.get(sd.month)
                    if q_key:
                        q_cols[f"{year}.{q_key}"] = col
            except Exception:
                continue

        # BS (분기별 부채비율)
        bs, bs_lm, bs_dc = None, {}, []
        try:
            bs = fs['bs']
            bs_dc = _data_cols(bs)
            bs_lm = _label_map(bs)
        except Exception:
            pass

        def _debt_ratio(q_end_str):
            if not bs_lm:
                return None
            q_col_bs = next((c for c in bs_dc if isinstance(c[0], str) and q_end_str in c[0]), None)
            if q_col_bs is None:
                return None
            debt   = _get_val(bs, bs_lm, q_col_bs, ['부채총계', '부채 합계'])
            equity = _get_val(bs, bs_lm, q_col_bs, ['자본총계', '자본 합계', '지배기업 소유주지분'])
            if debt and equity and equity != 0:
                return debt / equity * 100
            return None

        # Q1 ~ Q3: 단독 분기 컬럼에서 직접 추출
        for q_label in [f"{year}.Q1", f"{year}.Q2", f"{year}.Q3"]:
            col = q_cols.get(q_label)
            if col is None:
                continue
            rec = {'분기': q_label}
            for field, keys in IS_KEYS.items():
                rec[field] = _get_val(inc, lm, col, keys)
            q_end = col[0].split('-')[-1]
            rec['부채비율_raw'] = _debt_ratio(q_end)
            records.append(rec)

        # Q4: 연간 - 9개월 누적 (annual 데이터와 cum9_col 모두 있을 때만)
        if annual and cum9_col:
            rec_q4 = {'분기': f"{year}.Q4"}
            has_data = False
            for field, keys in IS_KEYS.items():
                ann_val = annual.get(field)
                cum9_val = _get_val(inc, lm, cum9_col, keys)
                if ann_val is not None and cum9_val is not None:
                    rec_q4[field] = ann_val - cum9_val
                    has_data = True
                else:
                    rec_q4[field] = None
            # Q4 부채비율 = 연간 말(12월 31일) BS → 분기 report에 없으므로 annual 값 사용
            rec_q4['부채비율_raw'] = annual.get('부채비율_raw') or _debt_ratio(f'{year}1231')
            if has_data:
                records.append(rec_q4)

    except Exception as e:
        print(f"  [WARN] 분기 파싱 실패: {e}")

    return records


# ══════════════════════════════════════════════════════════════════════
#  3. 마켓 데이터 계산 (DART 기반)
# ══════════════════════════════════════════════════════════════════════

def compute_market_metrics(stock_info: dict, annual: dict) -> dict:
    """주가 + DART 연간 데이터로 PER/PBR/시가총액 계산."""
    result = {}
    price = stock_info.get('price')
    if not price:
        return result

    eps = annual.get('EPS')
    bps = annual.get('BPS')
    shares = _get_total_shares(annual)

    if eps and eps > 0:
        result['PER'] = round(price / eps, 1)
    if bps and bps > 0:
        result['PBR'] = round(price / bps, 2)
    if shares:
        cap_won = price * shares
        result['시가총액'] = _fmt(cap_won, '조')

    return result


# ══════════════════════════════════════════════════════════════════════
#  4. 마크다운 재무 섹션 생성
# ══════════════════════════════════════════════════════════════════════

def build_finance_section(
    stock_info: dict,
    annual: dict,
    quarters: list,
    market: dict,
    year: int,
    code: str,
) -> str:
    today = datetime.now().strftime('%Y-%m-%d')
    price = stock_info.get('price')
    price_date = stock_info.get('price_date', today)

    lines = [
        "## 재무 현황",
        "",
        f"> 출처: DART OpenAPI + FDR(주가) | 기준: {today} | 종목코드: {code}",
        f"> 회계연도: {year}년 (연결 기준){' | 주가: ' + str(int(price)) + '원 (' + price_date + ')' if price else ''}",
        "",
    ]

    # ── 밸류에이션 ──
    lines += [
        "### 밸류에이션",
        "",
        "| 항목 | 수치 |",
        "|------|------|",
        f"| 시가총액 | {market.get('시가총액', '[⏳ 계산 불가]')} |",
        f"| PER | {str(market['PER']) + '배' if 'PER' in market else '[⏳]'} |",
        f"| PBR | {str(market['PBR']) + '배' if 'PBR' in market else '[⏳]'} |",
        f"| EPS | {str(int(annual['EPS'])) + '원' if annual.get('EPS') else '-'} |",
        f"| BPS | {str(int(annual['BPS'])) + '원' if annual.get('BPS') else '-'} |",
        "",
    ]

    # ── 연간 실적 ──
    lines += [
        f"### {year}년 연간 실적 (DART 연결)",
        "",
        "| 항목 | 수치 |",
        "|------|------|",
        f"| 매출액 | {_fmt(annual.get('매출'))} |",
        f"| 영업이익 | {_fmt(annual.get('영업이익'))} |",
        f"| 순이익 | {_fmt(annual.get('순이익'))} |",
        f"| 부채비율 | {_fmt_ratio(annual.get('부채비율_raw'))}{' (별도 기준)' if annual.get('BS_별도') else ''} |",
        "",
    ]

    # ── 분기 실적 ──
    if quarters:
        lines += [
            "### 분기 실적 (DART 연결)",
            "",
            "| 분기 | 매출 | 영업이익 | 순이익 | 부채비율 |",
            "|------|------|----------|--------|----------|",
        ]
        for r in quarters:
            lines.append(
                f"| {r.get('분기', '-')} "
                f"| {_fmt(r.get('매출'))} "
                f"| {_fmt(r.get('영업이익'))} "
                f"| {_fmt(r.get('순이익'))} "
                f"| {_fmt_ratio(r.get('부채비율_raw'))} |"
            )
        lines.append("")
    else:
        lines += [
            "### 분기 실적",
            "",
            "| 분기 | 매출 | 영업이익 |",
            "|------|------|----------|",
            "| - | [⏳ DART 분기 미수집] | - |",
            "",
        ]

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════
#  5. wiki 파일 업데이트
# ══════════════════════════════════════════════════════════════════════

def update_wiki(name: str, section: str, dry_run: bool):
    candidates = [
        WIKI_ROOT / f"{name}.md",
        WIKI_ROOT / f"{name.replace(' ', '')}.md",
    ]
    wiki_file = next((p for p in candidates if p.exists()), None)

    if not wiki_file:
        print(f"\n[INFO] wiki 파일 없음 ({WIKI_ROOT}/{name}.md) — 섹션 출력:\n")
        print(section)
        return

    content = wiki_file.read_text(encoding='utf-8')

    # ## 재무 현황 ~ 다음 ## 까지 교체 (또는 파일 끝까지)
    pattern = r'## 재무 현황\n.*?(?=\n## |\Z)'
    if re.search(pattern, content, flags=re.DOTALL):
        new_content = re.sub(pattern, section, content, flags=re.DOTALL)
    else:
        # 섹션 없으면 ## 차트·거래량 앞에 삽입
        insert_marker = "## 차트·거래량 메모"
        if insert_marker in content:
            new_content = content.replace(insert_marker, section + "\n\n---\n\n" + insert_marker)
        else:
            new_content = content + "\n\n---\n\n" + section

    if dry_run:
        print(f"\n[DRY-RUN] {wiki_file.name} — 변경될 재무 섹션:")
        print("=" * 60)
        print(section)
        print("=" * 60)
    else:
        wiki_file.write_text(new_content, encoding='utf-8')
        print(f"  [✓] {wiki_file} 재무 섹션 업데이트 완료")


# ══════════════════════════════════════════════════════════════════════
#  6. 단일 종목 처리
# ══════════════════════════════════════════════════════════════════════

def process_stock(name_or_code: str, year: int, dry_run: bool):
    print(f"\n{'─' * 50}")
    print(f"  종목   : {name_or_code}")
    print(f"  회계연도: {year}년")
    print(f"  모드   : {'DRY-RUN' if dry_run else '실제 업데이트'}")
    print(f"{'─' * 50}")

    # DART 초기화
    dart = _setup_dart()
    print("\n[1/4] DART 기업 검색...")
    corp, code, corp_name = _find_corp(dart, name_or_code)
    print(f"  → {corp_name} (종목코드: {code})")

    # 주가 조회
    print("\n[2/4] FDR 주가 조회...")
    stock_info = get_stock_info(code or name_or_code)
    if stock_info.get('price'):
        print(f"  → {int(stock_info['price']):,}원 ({stock_info['price_date']})")
    else:
        print("  → 주가 조회 실패 (PER/PBR/시가총액 계산 불가)")
    stock_info['name'] = corp_name

    # DART 연간 데이터
    print(f"\n[3/4] DART {year}년 연간 재무제표 조회...")
    annual = fetch_dart_annual(corp, year)
    if annual.get('매출'):
        print(f"  → 매출: {_fmt(annual.get('매출'))}  영업이익: {_fmt(annual.get('영업이익'))}  순이익: {_fmt(annual.get('순이익'))}")
        print(f"     부채비율: {_fmt_ratio(annual.get('부채비율_raw'))}  EPS: {annual.get('EPS')}원")
    else:
        print("  → 연간 데이터 없음 (분기 데이터로 진행)")

    # DART 분기 데이터
    print(f"\n[3b/4] DART {year}년 분기 재무제표 조회...")
    quarters = fetch_dart_quarters(corp, year, annual)
    print(f"  → {len(quarters)}개 분기 데이터 수집: {[r['분기'] for r in quarters]}")

    # 밸류에이션 계산
    market = compute_market_metrics(stock_info, annual)
    if market:
        print(f"\n  계산된 밸류에이션: 시총 {market.get('시가총액','-')}  PER {market.get('PER','-')}배  PBR {market.get('PBR','-')}배")

    # 섹션 생성 및 wiki 업데이트
    print("\n[4/4] wiki 재무 섹션 업데이트...")
    section = build_finance_section(stock_info, annual, quarters, market, year, code)
    update_wiki(corp_name, section, dry_run)

    print("\n✓ 완료")
    return corp_name


# ══════════════════════════════════════════════════════════════════════
#  main
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="종목 재무 데이터를 wiki/stocks/[종목명].md에 자동으로 채웁니다."
    )
    parser.add_argument("stock", nargs="?", help="종목명 또는 6자리 코드")
    parser.add_argument("--all",      action="store_true", help="wiki/stocks/ 전체 종목 일괄 업데이트")
    parser.add_argument("--dry-run",  action="store_true", help="wiki 수정 없이 미리보기")
    parser.add_argument("--year",     type=int, default=datetime.now().year - 1,
                        help=f"회계연도 (기본: 직전 연도 = {datetime.now().year - 1})")
    parser.add_argument("--dart-key", help="DART API 키 직접 입력 (.env 대신)")
    args = parser.parse_args()

    global DART_API_KEY
    if args.dart_key:
        DART_API_KEY = args.dart_key

    if not DART_API_KEY:
        print("[ERROR] DART_API_KEY가 설정되지 않았습니다.")
        print("  .env 파일에 DART_API_KEY=키값 추가 또는 --dart-key 옵션 사용")
        sys.exit(1)

    if args.all:
        # wiki/stocks/ 내 모든 .md 파일 (템플릿 제외)
        stock_files = [f for f in WIKI_ROOT.glob("*.md") if not f.name.startswith("_")]
        print(f"[배치] {len(stock_files)}개 종목 일괄 업데이트 시작")
        for f in stock_files:
            name = f.stem
            try:
                process_stock(name, args.year, args.dry_run)
            except Exception as e:
                print(f"  [ERROR] {name}: {e}")
    elif args.stock:
        process_stock(args.stock, args.year, args.dry_run)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    # Windows cp949 터미널에서 스피너 Unicode 에러 억제
    if sys.platform == 'win32':
        sys.stderr = open(os.devnull, 'w', encoding='utf-8')
    main()
