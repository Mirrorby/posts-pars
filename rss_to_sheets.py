import os, datetime as dt, re, html
import feedparser, gspread, requests
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

# ---------- Настройки ----------
DEFAULT_CHANNELS = ["MELOCHOV", "ABKS07", "jjsbossj", "toolsSADA"]
RSS_BASES = [b.strip() for b in os.getenv("RSS_BASES", os.getenv("RSS_BASE","https://rsshub.app")).split(",") if b.strip()]
INITIAL_LIMIT = int(os.getenv("INITIAL_LIMIT", "50"))

GSHEET_TITLE = os.getenv("GSHEET_TITLE", "Telegram Posts Inbox")
SHEET_NAME   = os.getenv("SHEET_NAME", "Posts")
STATE_SHEET  = os.getenv("STATE_SHEET", "State")
GCP_JSON     = os.getenv("GCP_JSON_PATH", "gcp_sa.json")

UA = "Mozilla/5.0 (compatible; tg-rss-to-sheets/1.3)"
MSK = ZoneInfo("Europe/Moscow")
UTC = ZoneInfo("UTC")

# ---------- Текст: очистка ----------
EMOJI_RE = re.compile(
    "[" "\U0001F300-\U0001F6FF" "\U0001F900-\U0001F9FF" "\U0001FA70-\U0001FAFF"
    "\U00002700-\U000027BF" "\U00002600-\U000026FF" "]+", flags=re.UNICODE
)
def strip_emoji(s:str)->str:
    return EMOJI_RE.sub("", s)

def strip_html(x:str)->str:
    if not x: return ""
    x = html.unescape(x)
    x = re.sub(r"<br\s*/?>", "\n", x, flags=re.I)
    x = re.sub(r"<[^>]+>", "", x)
    return x

def normalize_text(raw:str, limit=2000)->str:
    t = strip_html(raw)
    t = t.replace("\r", "").replace("\t", " ")
    t = re.sub(r"[ \u200b\u2060]{2,}", " ", t)
    t = re.sub(r"\n\s*\n\s*", "\n", t)
    t = re.sub(r"[ \u00A0]{2,}", " ", t)
    t = strip_emoji(t)
    return t.strip()[:limit]

def make_title_and_text(clean:str, title_limit=120, text_limit=2000):
    lines = [l.strip() for l in clean.split("\n") if l.strip()]
    if not lines:
        return "", ""
    title = lines[0][:title_limit]
    rest  = " ".join(lines[1:]).strip()
    return title, rest[:text_limit]

# ---------- Дата/время ----------
def fmt_msk(dt_obj: dt.datetime) -> str:
    """Формат YYYY-MM-DD HH:MM:SS в часовом поясе Москвы."""
    return dt_obj.astimezone(MSK).strftime("%Y-%m-%d %H:%M:%S")

def to_utc(dt_obj: dt.datetime | None) -> dt.datetime:
    if dt_obj is None:
        return dt.datetime.now(tz=UTC)
    if dt_obj.tzinfo is None:
        return dt_obj.replace(tzinfo=UTC)
    return dt_obj.astimezone(UTC)

def parse_any_datetime_to_utc(s: str | None) -> dt.datetime:
    # 1) пробуем feedparser (RFC822 и пр.)
    try:
        if s:
            tup = feedparser._parse_date(s)
            if tup:
                return dt.datetime(*tup[:6], tzinfo=UTC)
    except Exception:
        pass
    # 2) ISO 8601 (в том числе t.me time@datetime)
    try:
        if s:
            iso = s.replace("Z", "+00:00")
            d = dt.datetime.fromisoformat(iso)
            return to_utc(d)
    except Exception:
        pass
    # fallback: сейчас
    return dt.datetime.now(tz=UTC)

# ---------- Sheets ----------
def gs_open():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GCP_JSON, scope)
    gc = gspread.authorize(creds)
    sh = gc.open(GSHEET_TITLE)
    try:
        ws = sh.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(SHEET_NAME, rows=2000, cols=6)
        # ВАЖНО: новый порядок колонок
        ws.append_row(["PublishedAt (MSK)","AddedAt (MSK)","Channel","Link","Title","Text"])
    try:
        st = sh.worksheet(STATE_SHEET)
    except gspread.WorksheetNotFound:
        st = sh.add_worksheet(STATE_SHEET, rows=200, cols=2)
        st.append_row(["Channel","LastLink"])
    # Лёгкое форматирование (ширины + wrap clip)
    try:
        sh.batch_update({
            "requests":[
                {"updateDimensionProperties":{
                    "range":{"sheetId": ws.id, "dimension":"COLUMNS","startIndex":0,"endIndex":6},
                    "properties":{"pixelSize": 140}, "fields":"pixelSize"}},
                {"updateDimensionProperties":{
                    "range":{"sheetId": ws.id, "dimension":"COLUMNS","startIndex":2,"endIndex":3},
                    "properties":{"pixelSize": 110}, "fields":"pixelSize"}},
                {"updateDimensionProperties":{
                    "range":{"sheetId": ws.id, "dimension":"COLUMNS","startIndex":3,"endIndex":4},
                    "properties":{"pixelSize": 260}, "fields":"pixelSize"}},
                {"updateDimensionProperties":{
                    "range":{"sheetId": ws.id, "dimension":"COLUMNS","startIndex":4,"endIndex":5},
                    "properties":{"pixelSize": 280}, "fields":"pixelSize"}},
                {"updateDimensionProperties":{
                    "range":{"sheetId": ws.id, "dimension":"COLUMNS","startIndex":5,"endIndex":6},
                    "properties":{"pixelSize": 600}, "fields":"pixelSize"}},
                {"repeatCell":{
                    "range":{"sheetId": ws.id},
                    "cell":{"userEnteredFormat":{
                        "wrapStrategy":"CLIP",
                        "verticalAlignment":"TOP",
                        "horizontalAlignment":"LEFT"}},
                    "fields":"userEnteredFormat(wrapStrategy,verticalAlignment,horizontalAlignment)"}},
                {"updateSheetProperties":{
                    "properties":{"sheetId": ws.id, "gridProperties":{"frozenRowCount":1}},
                    "fields":"gridProperties.frozenRowCount"}}
            ]})
    except Exception:
        pass
    return ws, st

def load_state(st):
    ch = st.col_values(1)[1:]
    lk = st.col_values(2)[1:]
    return {c: l for c, l in zip(ch, lk) if c}

def save_state(st, channel, last_link):
    cells = st.findall(channel, in_column=1)
    if cells:
        st.update_cell(cells[-1].row, 2, last_link)
    else:
        st.append_row([channel, last_link])

# ---------- Источники ----------
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

def rss_entries(username: str):
    for base in RSS_BASES:
        url = f"{base}/telegram/channel/{username}"
        try:
            f = feedparser.parse(url)
            if f.entries:
                out = []
                for e in f.entries:
                    text_raw = getattr(e, "summary", "") or ""
                    text = normalize_text(text_raw)
                    title, text2 = make_title_and_text(text)
                    pub_utc = parse_any_datetime_to_utc(getattr(e, "published", ""))
                    out.append({
                        "published_msk": fmt_msk(pub_utc),
                        "link": (getattr(e, "link", "") or getattr(e, "id", "") or ""),
                        "title": title,
                        "text":  text2 if text2 else text,
                    })
                print(f"[info] RSS OK via {base} for {username}: {len(out)}")
                return out
            else:
                print(f"[warn] empty RSS via {base} for {username}")
        except Exception as ex:
            print(f"[warn] RSS error via {base} for {username}: {ex}")
    return []

def html_entries(username: str, limit=100):
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
            clean = normalize_text(text_el.get_text("\n", strip=True) if text_el else "")
            title, rest = make_title_and_text(clean)
            time_el = msg.select_one("time")
            pub_utc = parse_any_datetime_to_utc(time_el.get("datetime") if time_el else None)
            items.append({
                "published_msk": fmt_msk(pub_utc),
                "link": link,
                "title": title,
                "text": rest if rest else clean
            })
            if len(items) >= limit:
                break
        if items:
            print(f"[info] Fallback t.me/s used for {username}: {len(items)}")
        else:
            print(f"[warn] t.me/s has no items for {username}")
        return items
    except Exception as ex:
        print(f"[warn] t.me/s error for {username}: {ex}")
        return []

def fetch_entries(username: str):
    e = rss_entries(username)
    return e if e else html_entries(username)

# ---------- Основной запуск ----------
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
            link = e["link"] or (e["title"] + "|" + e["published_msk"])
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

        now_msk = fmt_msk(dt.datetime.now(tz=UTC))
        rows = [[e["published_msk"], now_msk, ch, e["link"], e["title"], e["text"]] for e in fresh]
        ws.append_rows(rows, value_input_option="RAW")
        save_state(st, ch, rows[-1][2+1])  # last link = column D (index 3 overall)
        print(f"[append] {ch}: {len(rows)} rows")

if __name__ == "__main__":
    main()
