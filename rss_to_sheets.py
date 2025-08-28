import os, datetime as dt, re, html, time, json
import feedparser, gspread, requests
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup

# ------------ Настройки ------------
DEFAULT_CHANNELS = ["MELOCHOV", "ABKS07", "jjsbossj", "toolsSADA"]
# можно указать несколько баз через запятую (будем пробовать по очереди)
RSS_BASES = [b.strip() for b in os.getenv("RSS_BASES", os.getenv("RSS_BASE","https://rsshub.app")).split(",") if b.strip()]
INITIAL_LIMIT = int(os.getenv("INITIAL_LIMIT", "50"))   # возьмём побольше на первый запуск

GSHEET_TITLE = os.getenv("GSHEET_TITLE", "Telegram Posts Inbox")
SHEET_NAME   = os.getenv("SHEET_NAME", "Posts")
STATE_SHEET  = os.getenv("STATE_SHEET", "State")
GCP_JSON     = os.getenv("GCP_JSON_PATH", "gcp_sa.json")

UA = "Mozilla/5.0 (compatible; tg-rss-to-sheets/1.1)"

# ------------ Утилиты ------------
def norm_channels(csv: str | None):
    if not csv:
        return DEFAULT_CHANNELS
    parts = [p.strip() for p in csv.split(",") if p.strip()]
    out = []
    for p in parts:
        p = p.lstrip("@").split("/")[-1]
        if p:
            out.append(p)
    return out

def strip_html(x: str) -> str:
    if not x: return ""
    x = html.unescape(x)
    x = re.sub(r"<br\s*/?>", "\n", x, flags=re.I)
    x = re.sub(r"<[^>]+>", "", x)
    return re.sub(r"\s+\n", "\n", x).strip()

def parse_date(s: str | None) -> str:
    # приводим к YYYY-MM-DD HH:MM:SS (UTC)
    try:
        if not s:
            return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        # feedparser уже парсит published_parsed
        return dt.datetime(*feedparser._parse_date(s)[:6]).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# ------------ Sheets ------------
def gs_open():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GCP_JSON, scope)
    gc = gspread.authorize(creds)
    sh = gc.open(GSHEET_TITLE)
    try:
        ws = sh.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(SHEET_NAME, rows=2000, cols=5)
        ws.append_row(["Date","Channel","Title","Link","Text"])
    try:
        st = sh.worksheet(STATE_SHEET)
    except gspread.WorksheetNotFound:
        st = sh.add_worksheet(STATE_SHEET, rows=200, cols=2)
        st.append_row(["Channel","LastLink"])
    return ws, st

def load_state(st):
    # читаем узко: сразу оба столбца, без get_all_records
    ch = st.col_values(1)[1:]  # без заголовка
    lk = st.col_values(2)[1:]
    return {c: l for c, l in zip(ch, lk) if c}

def save_state(st, channel, last_link):
    cells = st.findall(channel, in_column=1)
    if cells:
        st.update_cell(cells[-1].row, 2, last_link)
    else:
        st.append_row([channel, last_link])

# ------------ Источники данных ------------
def rss_entries(username: str):
    for base in RSS_BASES:
        url = f"{base}/telegram/channel/{username}"
        try:
            f = feedparser.parse(url)
            if f.bozo and not getattr(f, "entries", None):
                print(f"[warn] RSS bozo via {base} for {username}: {getattr(f,'bozo_exception',None)}")
            if f.entries:
                out = []
                for e in f.entries:
                    out.append({
                        "date": parse_date(getattr(e, "published", "")),
                        "title": getattr(e, "title", "") or "",
                        "link":  getattr(e, "link", "") or getattr(e, "id", "") or "",
                        "text":  strip_html(getattr(e, "summary", ""))[:45000],
                    })
                print(f"[info] RSS OK via {base} for {username}: {len(out)}")
                return out
            else:
                print(f"[warn] empty RSS via {base} for {username}")
        except Exception as ex:
            print(f"[warn] RSS error via {base} for {username}: {ex}")
    return []

def html_entries(username: str, limit=100):
    # fallback: публичная страница t.me/s/<username>
    url = f"https://t.me/s/{username}"
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=25)
        if r.status_code != 200:
            print(f"[warn] t.me/s status {r.status_code} for {username}")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        items = []
        for msg in soup.select(".tgme_widget_message_wrap"):
            a = msg.select_one("a.tgme_widget_message_date")
            if not a or not a.get("href"):
                continue
            link = a["href"]
            text_el = msg.select_one(".tgme_widget_message_text")
            text = text_el.get_text("\n", strip=True) if text_el else ""
            time_el = msg.select_one("time")
            date = time_el.get("datetime") if time_el else ""
            items.append({
                "date": parse_date(date),
                "title": "",
                "link": link,
                "text": text[:45000]
            })
            if len(items) >= limit:
                break
        if not items:
            print(f"[warn] t.me/s has no items for {username}")
        else:
            print(f"[info] Fallback t.me/s used for {username}: {len(items)}")
        return items
    except Exception as ex:
        print(f"[warn] t.me/s error for {username}: {ex}")
        return []

def fetch_entries(username: str):
    # 1) пробуем RSS
    e = rss_entries(username)
    if e:
        return e
    # 2) фолбэк на HTML
    return html_entries(username)

# ------------ Основной цикл ------------
def main():
    channels = norm_channels(os.getenv("CHANNELS_CSV"))
    ws, st = gs_open()
    state = load_state(st)

    for ch in channels:
        entries = fetch_entries(ch)
        if not entries:
            continue

        last_link = state.get(ch, "")
        fresh = []

        # oldest -> newest
        for e in reversed(entries):
            link = e["link"] or (e["title"] + "|" + e["date"])
            if not last_link:
                fresh.append(e)
                if len(fresh) > INITIAL_LIMIT:
                    fresh = fresh[-INITIAL_LIMIT:]
            else:
                fresh.append(e)
                if link == last_link:
                    fresh = fresh[:-1]
                    break

        if not fresh:
            print(f"[ok] {ch}: no new items")
            continue

        # нормальная запись: одним батчем
        rows = [[e["date"], ch, e["title"], e["link"], e["text"]] for e in fresh]
        ws.append_rows(rows, value_input_option="RAW")
        state[ch] = rows[-1][3]  # last link
        save_state(st, ch, state[ch])
        print(f"[append] {ch}: {len(rows)} rows")

if __name__ == "__main__":
    main()
