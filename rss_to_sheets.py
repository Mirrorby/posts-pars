import os, datetime as dt, re, html, time
import feedparser, gspread, requests
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup  # <-- добавим в requirements

DEFAULT_CHANNELS = ["MELOCHOV", "ABKS07", "jjsbossj", "toolsSADA"]
# можно указать несколько баз через запятую (будем пробовать по очереди)
RSS_BASES = [b.strip() for b in os.getenv("RSS_BASES", os.getenv("RSS_BASE","https://rsshub.app")).split(",") if b.strip()]
INITIAL_LIMIT = int(os.getenv("INITIAL_LIMIT", "20"))

GSHEET_TITLE = os.getenv("GSHEET_TITLE", "Telegram Posts Inbox")
SHEET_NAME   = os.getenv("SHEET_NAME", "Posts")
GCP_JSON     = os.getenv("GCP_JSON_PATH", "gcp_sa.json")

UA = "Mozilla/5.0 (compatible; tg-rss-to-sheets/1.0)"

def parse_channels_from_env():
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
    try:
        ws = sh.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(SHEET_NAME, rows=2000, cols=5)
        ws.append_row(["Date","Channel","Title","Link","Text"])
    try:
        st = sh.worksheet("State")
    except gspread.WorksheetNotFound:
        st = sh.add_worksheet("State", rows=100, cols=2)
        st.append_row(["Channel","LastLink"])
    return ws, st

def state_map(st):
    return { (r.get("Channel") or "").strip(): (r.get("LastLink") or "").strip()
             for r in st.get_all_records() if r.get("Channel") }

def set_state(st, channel, last_link):
    cells = st.findall(channel, in_column=1)
    if cells:
        st.update_cell(cells[-1].row, 2, last_link)
    else:
        st.append_row([channel, last_link])

def strip_html(x):
    if not x: return ""
    x = html.unescape(x)
    return re.sub(r"<[^>]+>", "", x)

def feed_url(base, username):
    return f"{base}/telegram/channel/{username}"

def parse_tme_s(username, limit=50):
    """Запасной источник: HTML https://t.me/s/<username>"""
    url = f"https://t.me/s/{username}"
    r = requests.get(url, headers={"User-Agent": UA}, timeout=20)
    if r.status_code != 200:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    items = []
    for msg in soup.select(".tgme_widget_message_wrap"):
        # ссылка на пост
        a = msg.select_one("a.tgme_widget_message_date")
        if not a or not a.get("href"): 
            continue
        link = a["href"]
        # заголовок/текст
        text_el = msg.select_one(".tgme_widget_message_text")
        text = text_el.get_text("\n", strip=True) if text_el else ""
        # дата
        time_el = msg.select_one("time")
        date = time_el.get("datetime") if time_el else ""
        items.append({
            "date": date or dt.datetime.utcnow().isoformat(),
            "title": "",
            "link": link,
            "text": text
        })
        if len(items) >= limit:
            break
    return items

def fetch_entries(username):
    # 1) пробуем по очереди базы RSSHub
    for base in RSS_BASES:
        try:
            f = feedparser.parse(feed_url(base, username))
            if f.entries:
                out = []
                for e in f.entries:
                    out.append({
                        "date": getattr(e, "published", "") or dt.datetime.utcnow().isoformat(),
                        "title": getattr(e, "title", ""),
                        "link":  getattr(e, "link", "") or getattr(e, "id", ""),
                        "text":  strip_html(getattr(e, "summary", "")),
                    })
                print(f"[info] RSS OK via {base} for {username}: {len(out)}")
                return out
            else:
                print(f"[warn] empty RSS via {base} for {username}")
        except Exception as ex:
            print(f"[warn] RSS error via {base} for {username}: {ex}")
    # 2) fallback: HTML со страницы t.me/s/<channel>
    fallback = parse_tme_s(username, limit=100)
    if fallback:
        print(f"[info] Fallback t.me/s used for {username}: {len(fallback)}")
    else:
        print(f"[warn] Fallback t.me/s failed for {username}")
    return fallback

def main():
    channels = parse_channels_from_env()
    ws, st = gs_sheet()
    smap = state_map(st)

    for ch in channels:
        entries = fetch_entries(ch)
        if not entries:
            continue

        last_link = smap.get(ch, "")
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

        for e in fresh:
            ws.append_row([e["date"], ch, e["title"], e["link"], e["text"][:45000]])
            last_link = e["link"] or (e["title"] + "|" + e["date"])

        set_state(st, ch, last_link)
        print(f"[append] {ch}: {len(fresh)} rows")

if __name__ == "__main__":
    main()
