"""
📊 한국 주식 외국인 지분율 추적기 + 텔레그램 알림
- 네이버 금융 모바일 API 사용 (로그인/OTP 불필요)
- 종목별로 지분율 직접 조회 → 전일 대비 변화량 계산
- 매일 장마감 후 텔레그램 자동 발송

필요 패키지:
    pip install requests pandas schedule python-telegram-bot
"""

import json
import time
import asyncio
import logging
import schedule
import sys
import os
import ast
import re
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
    # GitHub Actions: 환경변수 우선 적용
    if os.environ.get("TELEGRAM_TOKEN"):
        cfg["telegram_token"] = os.environ["TELEGRAM_TOKEN"]
    if os.environ.get("TELEGRAM_CHAT_ID"):
        cfg["telegram_chat_id"] = os.environ["TELEGRAM_CHAT_ID"]
    return cfg


# ──────────────────────────────────────────────
# 네이버 금융 API로 외국인 지분율 조회
# ──────────────────────────────────────────────
NAVER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
                  "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    "Referer": "https://m.stock.naver.com/",
}

def fetch_naver_foreign_ratio(ticker: str, name: str) -> dict | None:
    """
    네이버 금융 모바일 API로 종목의 외국인 지분율 조회
    URL: https://m.stock.naver.com/front-api/external/chart/domestic/info
         ?symbol=005930&requestType=1&startTime=YYYYMMDD&endTime=YYYYMMDD&timeframe=day
    반환 컬럼: 날짜, 시가, 고가, 저가, 종가, 거래량, 외국인소진율
    """
    today     = datetime.today().strftime("%Y%m%d")
    # 최근 5일치 요청해서 가장 최신 데이터 사용 (공휴일 대비)
    start     = (datetime.today() - timedelta(days=7)).strftime("%Y%m%d")

    url = (
        f"https://m.stock.naver.com/front-api/external/chart/domestic/info"
        f"?symbol={ticker}&requestType=1"
        f"&startTime={start}&endTime={today}&timeframe=day"
    )

    try:
        resp = requests.get(url, headers=NAVER_HEADERS, timeout=10)
        resp.raise_for_status()
        raw = resp.text.strip()

        # 응답 포맷: [[컬럼...], [데이터행...], ...]
        # 앞에 불필요한 문자가 붙을 수 있어 첫 '[' 부터 파싱
        start_idx = raw.find("[[")
        if start_idx == -1:
            log.warning(f"{name}({ticker}): 응답 파싱 실패 — {raw[:100]}")
            return None

        data_list = ast.literal_eval(raw[start_idx:])
        headers_row = data_list[0]   # ['날짜','시가','고가','저가','종가','거래량','외국인소진율']
        rows        = data_list[1:]  # 실제 데이터

        if not rows:
            log.warning(f"{name}({ticker}): 데이터 없음")
            return None

        # 가장 최근 행
        latest = dict(zip(headers_row, rows[-1]))
        ratio  = float(latest.get("외국인소진율", 0))
        date   = str(latest.get("날짜", today))

        log.info(f"  {name}({ticker}): 외국인소진율={ratio}%, 날짜={date}")
        return {
            "날짜":          date,
            "티커":          ticker,
            "종목명":        name,
            "지분율(%)":     round(ratio, 2),
            "보유수량":      0,   # 네이버 API는 수량 미제공
            "상장수량":      0,
            "한도소진율(%)": round(ratio, 2),
        }

    except Exception as e:
        log.error(f"{name}({ticker}) 조회 실패: {e}")
        return None


def fetch_all_watchlist(watchlist: dict) -> pd.DataFrame:
    """관심종목 전체 조회"""
    results = []
    for ticker, name in watchlist.items():
        row = fetch_naver_foreign_ratio(ticker, name)
        if row:
            results.append(row)
        time.sleep(0.3)  # 서버 부하 방지
    return pd.DataFrame(results)


# ──────────────────────────────────────────────
# 히스토리 저장 / 로드
# ──────────────────────────────────────────────
def load_history() -> pd.DataFrame:
    if DATA_FILE.exists():
        return pd.read_csv(DATA_FILE, dtype={"티커": str, "날짜": str})  # ← "날짜": str 추가
    return pd.DataFrame()

def save_history(df: pd.DataFrame):
    combined = pd.concat([load_history(), df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["날짜", "티커"], keep="last")
    combined = combined.sort_values(["티커", "날짜"])
    combined.to_csv(DATA_FILE, index=False, encoding="utf-8-sig")
    log.info(f"히스토리 저장 완료 ({len(combined)}행 누적)")


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
        else:
            dr = 0.0
        rows.append({**r.to_dict(), "변화(%)": dr, "변화수량": 0})
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
        chg = r["변화(%)"]
        if   chg > 0: arrow, chg_str = "🔺", f"+{chg:.2f}%p"
        elif chg < 0: arrow, chg_str = "🔻", f"{chg:.2f}%p"
        else:         arrow, chg_str = "➖", "변동없음"

        lines += [
            f"\n*{r['종목명']}* `{r['티커']}`",
            f"  외국인소진율: *{r['지분율(%)']:.2f}%*  {arrow} {chg_str}",
        ]

    lines += [
        "\n─────────────────────────",
        "📌 _데이터 출처: 네이버 금융_",
    ]
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

    today_df = fetch_all_watchlist(watchlist)
    if today_df.empty:
        msg = "⚠️ 오늘 데이터를 가져오지 못했습니다. 공휴일이거나 장 미개장일 수 있습니다."
        log.warning(msg)
        asyncio.run(send_telegram(token, chat_id, msg))
        return

    today = today_df["날짜"].iloc[0]
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

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--now":
        run_daily_job()
    else:
        start_scheduler()
