"""
📊 한국 주식 외국인 지분율 추적기 + 텔레그램 알림
- pykrx 없이 KRX 웹사이트에 직접 HTTP 요청
- 전일 대비 변화량 계산 후 텔레그램 발송
- 매일 장마감 후 자동 실행

필요 패키지:
    pip install requests pandas schedule python-telegram-bot
"""

import io
import json
import time
import asyncio
import logging
import schedule
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests
import pandas as pd
from telegram import Bot
from telegram.constants import ParseMode

# ──────────────────────────────────────────────
# 파일 경로
# ──────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
CONFIG_FILE = BASE_DIR / "config.json"
DATA_FILE   = BASE_DIR / "foreign_ratio_history.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(BASE_DIR / "tracker.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# 설정 로드
# ──────────────────────────────────────────────
def load_config() -> dict:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError("config.json 파일을 먼저 설정하세요.")
    with open(CONFIG_FILE, encoding="utf-8") as f:
        cfg = json.load(f)

    # GitHub Actions: 환경변수가 있으면 config.json보다 우선 적용
    import os
    if os.environ.get("TELEGRAM_TOKEN"):
        cfg["telegram_token"] = os.environ["TELEGRAM_TOKEN"]
    if os.environ.get("TELEGRAM_CHAT_ID"):
        cfg["telegram_chat_id"] = os.environ["TELEGRAM_CHAT_ID"]

    return cfg


# ──────────────────────────────────────────────
# KRX 데이터 수집 (pykrx 없이 직접 HTTP)
# ──────────────────────────────────────────────
KRX_SESSION = requests.Session()
KRX_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer":    "http://data.krx.co.kr/",
})


def _fetch_one_market(date: str, market: str) -> pd.DataFrame:
    """유가증권(STK) 또는 코스닥(KSQ) 외국인 지분율 CSV 다운로드"""
    # 1) OTP 발급
    otp_url  = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    otp_data = {
        "locale":        "ko_KR",
        "market":        market,
        "trdDd":         date,
        "money":         "1",
        "csvxls_isNo":   "false",
        "name":          "fileDown",
        "url":           "dbms/MDC/STAT/standard/MDCSTAT03402",
    }
    otp = KRX_SESSION.post(otp_url, data=otp_data, timeout=10).text.strip()
    if not otp:
        raise ValueError(f"OTP 발급 실패 (market={market}, date={date})")

    # 2) CSV 다운로드
    csv_resp = KRX_SESSION.post(
        "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd",
        data={"code": otp}, timeout=15
    )
    raw = csv_resp.content.decode("euc-kr", errors="replace")
    df  = pd.read_csv(io.StringIO(raw))

    # 3) 컬럼 정규화
    col_map = {}
    for c in df.columns:
        if   "종목코드"  in c: col_map[c] = "티커"
        elif "종목명"    in c: col_map[c] = "종목명"
        elif "지분율"    in c: col_map[c] = "지분율(%)"
        elif "보유수량"  in c: col_map[c] = "보유수량"
        elif "상장주식수" in c: col_map[c] = "상장수량"
        elif "한도소진율" in c: col_map[c] = "한도소진율(%)"
    df = df.rename(columns=col_map)

    for col in ["지분율(%)", "보유수량", "상장수량", "한도소진율(%)"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", "").str.replace("%", ""),
                errors="coerce"
            ).fillna(0)

    df["티커"] = df["티커"].astype(str).str.zfill(6)
    return df


def fetch_all_markets(date: str) -> pd.DataFrame:
    """유가증권 + 코스닥 합산"""
    frames = []
    for market in ("STK", "KSQ"):
        try:
            df = _fetch_one_market(date, market)
            frames.append(df)
            log.info(f"{market} {len(df)}종목 조회 완료")
        except Exception as e:
            log.error(f"{market} 조회 실패: {e}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ──────────────────────────────────────────────
# 관심종목 필터링
# ──────────────────────────────────────────────
def filter_watchlist(all_df: pd.DataFrame, watchlist: dict, date: str) -> pd.DataFrame:
    results = []
    for ticker, name in watchlist.items():
        row = all_df[all_df["티커"] == ticker]
        if not row.empty:
            r = row.iloc[0]
            results.append({
                "날짜":          date,
                "티커":          ticker,
                "종목명":        name,
                "지분율(%)":     round(float(r.get("지분율(%)", 0)), 2),
                "보유수량":      int(r.get("보유수량", 0)),
                "상장수량":      int(r.get("상장수량", 0)),
                "한도소진율(%)": round(float(r.get("한도소진율(%)", 0)), 2),
            })
        else:
            log.warning(f"{name}({ticker}) 데이터 없음")
    return pd.DataFrame(results)


# ──────────────────────────────────────────────
# 히스토리 저장 / 로드
# ──────────────────────────────────────────────
def load_history() -> pd.DataFrame:
    if DATA_FILE.exists():
        return pd.read_csv(DATA_FILE, dtype={"티커": str})
    return pd.DataFrame()


def save_history(df: pd.DataFrame):
    combined = pd.concat([load_history(), df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["날짜", "티커"], keep="last")
    combined = combined.sort_values(["티커", "날짜"])
    combined.to_csv(DATA_FILE, index=False, encoding="utf-8-sig")
    log.info(f"히스토리 저장 ({len(combined)}행 누적)")


# ──────────────────────────────────────────────
# 전일 대비 변화량 계산
# ──────────────────────────────────────────────
def calc_changes(today_df: pd.DataFrame) -> pd.DataFrame:
    history = load_history()
    if history.empty:
        today_df["변화(%)"]  = 0.0
        today_df["변화수량"] = 0
        return today_df

    today_date = today_df["날짜"].iloc[0]
    prev = history[history["날짜"] < today_date]
    if prev.empty:
        today_df["변화(%)"]  = 0.0
        today_df["변화수량"] = 0
        return today_df

    prev_map = history[history["날짜"] == prev["날짜"].max()].set_index("티커")

    rows = []
    for _, r in today_df.iterrows():
        t = r["티커"]
        if t in prev_map.index:
            dr = round(r["지분율(%)"] - prev_map.loc[t, "지분율(%)"], 2)
            dq = int(r["보유수량"]    - prev_map.loc[t, "보유수량"])
        else:
            dr, dq = 0.0, 0
        rows.append({**r.to_dict(), "변화(%)": dr, "변화수량": dq})
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────
# 텔레그램 메시지 포맷
# ──────────────────────────────────────────────
def format_message(df: pd.DataFrame, date: str) -> str:
    date_fmt = f"{date[:4]}.{date[4:6]}.{date[6:]}"
    lines = [
        "📊 *외국인 지분율 일간 리포트*",
        f"📅 {date_fmt} 장마감 기준\n",
        "─────────────────────────",
    ]

    for _, r in df.sort_values("변화(%)", ascending=False).iterrows():
        chg, qty = r["변화(%)"], r["변화수량"]
        if   chg > 0: arrow, chg_str = "🔺", f"+{chg:.2f}%  (+{qty:,}주)"
        elif chg < 0: arrow, chg_str = "🔻", f"{chg:.2f}%  ({qty:,}주)"
        else:         arrow, chg_str = "➖", "0.00%  (변동없음)"

        lines += [
            f"\n*{r['종목명']}* `{r['티커']}`",
            f"  지분율: *{r['지분율(%)']:.2f}%*  {arrow} {chg_str}",
            f"  한도소진: {r['한도소진율(%)']:.1f}%  |  보유: {r['보유수량']:,}주",
        ]

    lines += ["\n─────────────────────────", "📌 _데이터 출처: 한국거래소(KRX)_"]
    return "\n".join(lines)


# ──────────────────────────────────────────────
# 텔레그램 전송
# ──────────────────────────────────────────────
async def send_telegram(token: str, chat_id: str, message: str):
    bot = Bot(token=token)
    for i in range(0, len(message), 4000):
        await bot.send_message(
            chat_id=chat_id,
            text=message[i:i+4000],
            parse_mode=ParseMode.MARKDOWN,
        )
    log.info("텔레그램 전송 완료")


# ──────────────────────────────────────────────
# 메인 작업
# ──────────────────────────────────────────────
def run_daily_job():
    log.info("=== 외국인 지분율 추적 시작 ===")
    config    = load_config()
    token     = config["telegram_token"]
    chat_id   = config["telegram_chat_id"]
    watchlist = config["watchlist"]

    today  = datetime.today().strftime("%Y%m%d")
    log.info(f"조회 날짜: {today}")

    all_df = fetch_all_markets(today)
    if all_df.empty:
        msg = f"⚠️ {today} KRX 데이터를 가져오지 못했습니다.\n공휴일이거나 아직 데이터 미확정일 수 있습니다."
        log.warning(msg)
        asyncio.run(send_telegram(token, chat_id, msg))
        return

    today_df = filter_watchlist(all_df, watchlist, today)
    if today_df.empty:
        log.warning("관심종목 매칭 결과 없음")
        return

    today_df = calc_changes(today_df)
    save_history(today_df)
    asyncio.run(send_telegram(token, chat_id, format_message(today_df, today)))
    log.info("=== 완료 ===")


# ──────────────────────────────────────────────
# 스케줄러
# ──────────────────────────────────────────────
def start_scheduler():
    config   = load_config()
    run_time = config.get("run_time", "16:30")
    log.info(f"스케줄러 시작 — 매일 {run_time} 실행")
    schedule.every().day.at(run_time).do(run_daily_job)
    if config.get("run_on_start", False):
        run_daily_job()
    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--now":
        run_daily_job()
    else:
        start_scheduler()
