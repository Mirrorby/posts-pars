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

UA  = "Mozilla/5.0 (compatible; tg-rss-to-sheets/1.4)"
MSK = ZoneInfo("Europe/Moscow")
UTC = ZoneInfo("UTC")

# ---------- Текст: очистка ----------
EMOJI_RE = re.compile("[" "\U0001F300-\U0001F6FF" "\U0001F900-\U0001F9FF" "\U0001FA70-\U0001FAFF" "\U00002700-\U000027BF" "\U00002600-\U000026FF" "]+")
def strip_emoji(s:str)->str: return EMOJI_RE.sub("", s)

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
    if not lines: return "", ""
    title = lines[0][:title_limit]
    rest  = " ".join(lines[1:]).strip()
    return title, rest[:text_limit]

# ---------- Дата/время ----------
def fmt_msk(dt_obj: dt.datetime) -> str:
    return dt_obj.astimezone(MSK).strftime("%Y-%m-%d %H:%M:%S")

def to_utc(dt_obj: dt.datetime | None) -> dt.datetime:
    if dt_obj is None: return dt.datetime.now(tz=UTC)
    if dt_obj.tzinfo is None: return dt_obj.replace(tzinfo=UTC)
    return dt_obj.astimezone(UTC)

def parse_any_datetime_to_utc(s: str | None) -> dt.datetime:
    try:
        if s:
            tup = feedparser._parse_date(s)
            if tup: return dt.datetime(*tup[:6], tzinfo=UTC)
    except Exception: pass
    try:
        if s:
            iso = s.replace("Z", "+00:00")
            d = dt.datetime.fromisoformat(iso)
            return to_utc(d)
    except Exception: pass
    return dt.datetime.now(tz=UTC)

# ---------- Ссылки/ключи (анти-дубль) ----------
TME_RE = re.compile(r"https?://t\.me/(?:s/)?([^/]+)/(\d+)", re.I)

def canonical_link(link: str, channel_hint: str | None = None) -> str:
    """Канонизируем ссылку на пост: https://t.me/<username>/<id>; убираем параметры."""
    if not link: return ""
    m = TME_RE.search(link)
    if m:
        user, mid = m.group(1), m.group(2)
        return f"https://t.me/{user}/{mid}"
    # обрубим параметры и хвосты
    core = link.split("?")[0].rstrip("/")
    return core.lower()

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
        ws.append_row(["PublishedAt (MSK)","AddedAt (MSK)","Channel","Link","Title","Text"])
    try:
        st = sh.worksheet(STATE_SHEET)
    except gspread.WorksheetNotFound:
        st = sh.add_worksheet(STATE_SHEET, rows=200, cols=2)
        st.append_row(["Channel","LastLink"])
    return ws, st

def load_state(st):
    ch = st.col_values(1)[1:]
    lk = st.col_values(2)[1:]
    return {c: l for c, l in zip(ch, lk) if c}

def save_state(st, channel, last_link):
    cells = st.findall(channel, in_column=1)
    if cells: st.update_cell(cells[-1].row, 2, last_link)
    else:     st.append_row([channel, last_link])

def known_links_set(ws, channel: str, window: int = 400) -> set[str]:
    """Берём последние ~window строк и собираем ссылки этого канала для анти-дублей."""
    # дешево и сердито: получим весь диапазон D (Link) и C (Channel), возьмём хвост.
    values = ws.get_all_values()
    if len(values) <= 1: return set()
    tail = values[-min(window, len(values)-1):]  # без заголовка
    out = set()
    for row in tail:
        if len(row) < 4:  # должны быть хотя бы C и D
            continue
        ch = row[2].strip()
        ln = row[3].strip()
        if ch == channel and ln:
            out.add(canonical_link(ln, ch))
    return out

# ---------- Источники ----------
def norm_channels(csv: str | None):
    if not csv: return DEFAULT_CHANNELS
    parts = [p.strip() for p in csv.split(",") if p.strip()]
    return [p.lstrip("@").split("/")[-1] for p in parts if p]

def rss_entries(username: str):
    for base in RSS_BASES:
        url = f"{base}/telegram/channel/{username}"
        try:
            f = feedparser.parse(url)
            if f.entries:
                out = []
                for e in f.entries:
                    text = normalize_text(getattr(e, "summary", "") or "")
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

        # анти-дубль: считаем известные ссылки этого канала из таблицы (последние ~400 строк)
        known = known_links_set(ws, ch, window=600)

        last_link = canonical_link(state.get(ch, ""), ch)
        fresh = []

        # oldest -> newest
        for e in reversed(entries):
            raw_link = e["link"] or (e["title"] + "|" + e["published_msk"])
            key = canonical_link(raw_link, ch)

            # пропускаем, если уже есть в таблице (надёжнее, чем только "последняя ссылка")
            if key and key in known:
                continue

            if not last_link:
                fresh.append((key, e))
                if len(fresh) > INITIAL_LIMIT:
                    fresh = fresh[-INITIAL_LIMIT:]
            else:
                fresh.append((key, e))
                if key == last_link:
                    fresh = fresh[:-1]
                    break

        if not fresh:
            print(f"[ok] {ch}: no new items")
            continue

        now_msk = fmt_msk(dt.datetime.now(tz=UTC))
        rows = [[item["published_msk"], now_msk, ch, item["link"], item["title"], item["text"]] for (key, item) in fresh]
        ws.append_rows(rows, value_input_option="RAW")

        # обновим state последней канонической ссылкой
        last_key = fresh[-1][0]
        save_state(st, ch, last_key)
        print(f"[append] {ch}: {len(rows)} rows (dedup ok)")

if __name__ == "__main__":
    main()
