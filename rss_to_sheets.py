# rss_to_sheets.py
# Берёт RSS публичных Telegram-каналов через RSSHub и дозаписывает новые посты в Google Таблицу.
# Дедупликация через лист "State": запоминаем последнюю обработанную ссылку на канал.
# Запускается локально:  python rss_to_sheets.py
# В GitHub Actions: см. .github/workflows/rss.yml (нужны Secrets: GSHEET_TITLE, SHEET_NAME, GCP_JSON).

import os, datetime as dt, re, html
import feedparser
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# -------- Каналы и базовые настройки --------
# По умолчанию используем эти 4; можно переопределить переменной окружения CHANNELS_CSV
DEFAULT_CHANNELS = ["MELOCHOV", "ABKS07", "jjsbossj", "toolsSADA"]
FEED_BASE = os.getenv("RSS_BASE", "https://rsshub.app")  # можно поставить свой инстанс RSSHub
INITIAL_LIMIT = int(os.getenv("INITIAL_LIMIT", "20"))     # сколько взять при самом первом запуске (на канал)

GSHEET_TITLE = os.getenv("GSHEET_TITLE", "Telegram Posts Inbox")
SHEET_NAME   = os.getenv("SHEET_NAME", "Posts")
GCP_JSON     = os.getenv("GCP_JSON_PATH", "gcp_sa.json")

# -------- Вспомогательные функции --------
def parse_channels_from_env():
    """
    Берёт список каналов из CHANNELS_CSV (через запятую) или возвращает DEFAULT_CHANNELS.
    Принимает варианты: @name, name, https://t.me/name
    """
    csv = os.getenv("CHANNELS_CSV", "").strip()
    if not csv:
        return DEFAULT_CHANNELS
    parts = [p.strip().lstrip("@").split("/")[-1] for p in csv.split(",") if p.strip()]
    return [p for p in parts if p]

def gs_sheet():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GCP_JSON, scope)
    gc = gspread.authorize(creds)
    sh = gc.open(GSHEET_TITLE)
    # основной лист
    try:
        ws = sh.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(SHEET_NAME, rows=2000, cols=5)
        ws.append_row(["Date","Channel","Title","Link","Text"])
    # лист состояния
    try:
        st = sh.worksheet("State")
    except gspread.WorksheetNotFound:
        st = sh.add_worksheet("State", rows=100, cols=2)
        st.append_row(["Channel","LastLink"])
    return ws, st

def state_map(st):
    """Собираем dict: канал -> последняя обработанная ссылка."""
    records = st.get_all_records()
    return { (r.get("Channel") or "").strip(): (r.get("LastLink") or "").strip() for r in records if r.get("Channel") }

def set_state(st, channel, last_link):
    """Обновляем/добавляем last_link для канала в лист State."""
    cells = st.findall(channel, in_column=1)
    if cells:
        row = cells[-1].row
        st.update_cell(row, 2, last_link)
    else:
        st.append_row([channel, last_link])

def strip_html(x):
    if not x: return ""
    x = html.unescape(x)
    return re.sub(r"<[^>]+>", "", x)

def feed_url(username):
    u = username.lstrip("@")
    return f"{FEED_BASE}/telegram/channel/{u}"

# -------- Основная логика --------
def main():
    channels = parse_channels_from_env()
    ws, st = gs_sheet()
    smap = state_map(st)

    for ch in channels:
        url = feed_url(ch)
        feed = feedparser.parse(url)
        entries = list(feed.entries)
        if not entries:
            print(f"[warn] no entries for {ch} ({url})")
            continue

        last_link = smap.get(ch, "")
        fresh = []

        # идём от старых к новым (чтобы писать в правильном порядке)
        for e in reversed(entries):
            link = getattr(e, "link", "") or getattr(e, "id", "")
            if not link:
                # резервный идентификатор, если нет link/id
                link = (getattr(e, "title", "") + "|" + getattr(e, "published", "")).strip()
            if not link:
                continue

            if not last_link:
                # первая инициализация для канала — берём только N последних
                fresh.append(e)
                if len(fresh) > INITIAL_LIMIT:
                    fresh = fresh[-INITIAL_LIMIT:]
            else:
                # пишем до тех пор, пока не встретим прошлую "последнюю" ссылку
                fresh.append(e)
                if link == last_link:
                    fresh = fresh[:-1]  # известную не пишем
                    break

        if not fresh:
            print(f"[ok] {ch}: no new items")
            continue

        for e in fresh:
            date = getattr(e, "published", "") or dt.datetime.utcnow().isoformat()
            title = getattr(e, "title", "")
            link  = getattr(e, "link", "") or getattr(e, "id", "")
            text  = getattr(e, "summary", "")
            text  = strip_html(text)[:45000]  # безопасная длина для ячейки
            ws.append_row([date, ch, title, link, text])
            last_link = link

        set_state(st, ch, last_link)
        print(f"[append] {ch}: {len(fresh)} rows")

if __name__ == "__main__":
    main()
