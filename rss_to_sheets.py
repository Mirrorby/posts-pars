import os, datetime as dt, re, html, time
import feedparser, gspread, requests
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

# ============ Конфигурация ============
DEFAULT_CHANNELS = ["MELOCHOV", "ABKS07", "jjsbossj", "toolsSADA"]
RSS_BASES = [b.strip() for b in os.getenv("RSS_BASES", os.getenv("RSS_BASE","https://rsshub.app")).split(",") if b.strip()]
INITIAL_LIMIT = int(os.getenv("INITIAL_LIMIT", "50"))

GSHEET_TITLE = os.getenv("GSHEET_TITLE", "Telegram Posts Inbox")
SHEET_NAME   = os.getenv("SHEET_NAME", "Posts")
STATE_SHEET  = os.getenv("STATE_SHEET", "State")
CHANNELS_SHEET = os.getenv("CHANNELS_SHEET", "Channels")
BOTSTATE_SHEET = os.getenv("BOTSTATE_SHEET", "BotState")
GCP_JSON     = os.getenv("GCP_JSON_PATH", "gcp_sa.json")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_IDS  = [c.strip() for c in os.getenv("CHAT_ID", "").split(",") if c.strip()]
ADMIN_IDS = [a.strip() for a in os.getenv("ADMIN_IDS", "").split(",") if a.strip()]  # user_id разрешённых отправителей

UA  = "Mozilla/5.0 (compatible; tg-rss-to-sheets/1.6)"
MSK = ZoneInfo("Europe/Moscow")
UTC = ZoneInfo("UTC")

# ============ Утилиты текста и ссылок ============
EMOJI_RE = re.compile("[" "\U0001F300-\U0001F6FF" "\U0001F900-\U0001F9FF" "\U0001FA70-\U0001FAFF" "\U00002700-\U000027BF" "\U00002600-\U000026FF" "]+")
TME_RE = re.compile(r"https?://t\.me/(?:s/)?([^/]+)/(\d+)", re.I)

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

def canonical_link(link: str) -> str:
    if not link: return ""
    m = TME_RE.search(link)
    if m:
        user, mid = m.group(1), m.group(2)
        return f"https://t.me/{user}/{mid}"
    return link.split("?")[0].rstrip("/").lower()

# ============ Дата/время ============
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

# ============ Google Sheets ============
def gs_open():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GCP_JSON, scope)
    gc = gspread.authorize(creds)
    sh = gc.open(GSHEET_TITLE)
    # Posts
    try: ws = sh.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(SHEET_NAME, rows=2000, cols=6)
        ws.append_row(["PublishedAt (MSK)","AddedAt (MSK)","Channel","Link","Title","Text"])
    # State
    try: st = sh.worksheet(STATE_SHEET)
    except gspread.WorksheetNotFound:
        st = sh.add_worksheet(STATE_SHEET, rows=200, cols=2)
        st.append_row(["Channel","LastLink"])
    # Channels
    try: chs = sh.worksheet(CHANNELS_SHEET)
    except gspread.WorksheetNotFound:
        chs = sh.add_worksheet(CHANNELS_SHEET, rows=200, cols=1)
        chs.update("A1", "Channel")
        # инициализация дефолтами
        rows = [[c] for c in DEFAULT_CHANNELS]
        if rows: chs.append_rows(rows, value_input_option="RAW")
    # BotState (last_update_id)
    try: bs = sh.worksheet(BOTSTATE_SHEET)
    except gspread.WorksheetNotFound:
        bs = sh.add_worksheet(BOTSTATE_SHEET, rows=5, cols=2)
        bs.append_row(["Key","Value"])
        bs.append_row(["last_update_id","0"])
    return sh, ws, st, chs, bs

def load_state(st):
    ch = st.col_values(1)[1:]
    lk = st.col_values(2)[1:]
    return {c: l for c, l in zip(ch, lk) if c}

def save_state(st, channel, last_link):
    cells = st.findall(channel, in_column=1)
    if cells: st.update_cell(cells[-1].row, 2, last_link)
    else:     st.append_row([channel, last_link])

def get_channels(chs):
    vals = chs.col_values(1)[1:]  # без заголовка
    # нормализуем @/t.me/
    out = []
    for v in vals:
        v = (v or "").strip()
        if not v: continue
        v = v.lstrip("@").split("/")[-1]
        if v: out.append(v)
    return sorted(list(dict.fromkeys(out)))

def add_channel(chs, name: str) -> bool:
    name = (name or "").strip()
    if not name: return False
    name = name.lstrip("@").split("/")[-1]
    if not name: return False
    existing = set(get_channels(chs))
    if name in existing: return False
    chs.append_row([name])
    return True

def remove_channel(chs, name: str) -> bool:
    name = (name or "").strip()
    if not name: return False
    name = name.lstrip("@").split("/")[-1]
    vals = chs.col_values(1)
    for idx, val in enumerate(vals, start=1):
        if idx == 1: continue
        if (val or "").strip().lstrip("@").split("/")[-1] == name:
            chs.delete_rows(idx)
            return True
    return False

def get_kv(bs, key: str, default: str="0") -> str:
    vals = bs.get_all_values()
    for row in vals:
        if row and row[0] == key:
            return row[1] if len(row) > 1 else default
    bs.append_row([key, default])
    return default

def set_kv(bs, key: str, value: str):
    cells = bs.findall(key, in_column=1)
    if cells:
        bs.update_cell(cells[-1].row, 2, value)
    else:
        bs.append_row([key, value])

# ============ Источники ============
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
            if not a or not a.get("href"): continue
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
            if len(items) >= limit: break
        if items: print(f"[info] Fallback t.me/s used for {username}: {len(items)}")
        else:     print(f"[warn] t.me/s has no items for {username}")
        return items
    except Exception as ex:
        print(f"[warn] t.me/s error for {username}: {ex}")
        return []

def fetch_entries(username: str):
    e = rss_entries(username)
    return e if e else html_entries(username)

# ============ Bot API ============
def tg_send_message(token: str, chat_id: str, html_text: str, disable_preview=False):
    if not token or not chat_id: return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    # режем до лимита 4096
    parts = [html_text[i:i+4000] for i in range(0, len(html_text), 4000)] or [html_text]
    for i, chunk in enumerate(parts):
        data = {
            "chat_id": chat_id,
            "text": chunk,
            "parse_mode": "HTML",
            "disable_web_page_preview": disable_preview if i==len(parts)-1 else True
        }
        r = requests.post(url, json=data, timeout=20)
        if r.status_code == 429:
            time.sleep(1.2)
            r = requests.post(url, json=data, timeout=20)
        if not r.ok:
            print("[warn] sendMessage failed:", r.status_code, r.text)
        time.sleep(0.15)

def build_post_message(ch: str, pub_msk: str, link: str, title: str, text: str) -> str:
    title = html.escape(title)
    text  = html.escape(text)
    link  = html.escape(link)
    return f"<b>{ch}</b> • <i>{pub_msk}</i>\n<a href=\"{link}\">Пост</a>\n\n<b>{title}</b>\n{text}"

def process_bot_updates(bs, chs):
    """Обрабатывает команды: /list, /add <channel>, /remove <channel>.
       Хранит offset в BotState (last_update_id).
    """
    if not BOT_TOKEN:
        return

    last = int(get_kv(bs, "last_update_id", "0"))
    url  = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    try:
        resp = requests.get(url, params={"timeout": 0, "offset": last+1}, timeout=20)
        data = resp.json()
        if not data.get("ok"):
            print("[warn] getUpdates:", data)
            return
        updates = data.get("result", [])
    except Exception as ex:
        print("[warn] getUpdates error:", ex)
        return

    for upd in updates:
        last = max(last, upd.get("update_id", 0))
        msg = upd.get("message") or {}
        text = (msg.get("text") or "").strip()
        chat = msg.get("chat") or {}
        from_user = msg.get("from") or {}
        sender_id = str(from_user.get("id", ""))

        # авторизация отправителя (если задан ADMIN_IDS)
        if ADMIN_IDS and sender_id not in ADMIN_IDS:
            continue

        if not text.startswith("/"):
            continue

        def reply(s: str):
            try:
                tg_send_message(BOT_TOKEN, str(chat.get("id")), s, disable_preview=True)
            except Exception as e:
                print("[warn] reply failed:", e)

        cmd, *args = text.split(maxsplit=1)
        cmd = cmd.lower()

        if cmd == "/list":
            lst = get_channels(chs)
            if lst:
                reply("📡 Отслеживаемые каналы:\n" + "\n".join(f"• @{c}" for c in lst))
            else:
                reply("Список каналов пуст. Добавьте через: /add <ссылка или @username>")
        elif cmd == "/add":
            if not args:
                reply("Пришлите так: /add https://t.me/username (или @username)")
            else:
                cand = args[0].strip()
                ok = add_channel(chs, cand)
                reply("✅ Канал добавлен" if ok else "ℹ️ Канал уже есть или некорректный")
        elif cmd == "/remove":
            if not args:
                reply("Пришлите так: /remove https://t.me/username (или @username)")
            else:
                cand = args[0].strip()
                ok = remove_channel(chs, cand)
                reply("🗑️ Канал удалён" if ok else "ℹ️ Канал не найден")

    set_kv(bs, "last_update_id", str(last))

# ============ Основной запуск ============
def known_links_set(ws, channel: str, window: int = 600) -> set[str]:
    values = ws.get_all_values()
    if len(values) <= 1: return set()
    tail = values[-min(window, len(values)-1):]
    out = set()
    for row in tail:
        if len(row) < 4: continue
        ch = row[2].strip(); ln = row[3].strip()
        if ch == channel and ln:
            out.add(canonical_link(ln))
    return out

def main():
    sh, ws, st, chs, bs = gs_open()

    # 0) обработать команды бота (если есть)
    process_bot_updates(bs, chs)

    # 1) собрать список каналов из листа Channels
    channels = get_channels(chs)
    if not channels:
        print("[warn] Channels sheet empty")
        return

    # 2) стейты и антидубль
    state = load_state(st)

    for ch in channels:
        # сбор источников
        entries = fetch_entries(ch)
        if not entries:
            continue

        known = known_links_set(ws, ch, window=800)
        last_link_key = canonical_link(state.get(ch, ""))

        # от старых к новым
        fresh = []
        for e in reversed(entries):
            key = canonical_link(e["link"] or (e["title"] + "|" + e["published_msk"]))
            if key and key in known:
                continue
            if not last_link_key:
                fresh.append((key, e))
                if len(fresh) > INITIAL_LIMIT:
                    fresh = fresh[-INITIAL_LIMIT:]
            else:
                fresh.append((key, e))
                if key == last_link_key:
                    fresh = fresh[:-1]
                    break

        if not fresh:
            print(f"[ok] {ch}: no new items")
            continue

        now_msk = fmt_msk(dt.datetime.now(tz=UTC))
        rows = [[e["published_msk"], now_msk, ch, canonical_link(e["link"]), e["title"], e["text"]] for (key, e) in fresh]
        # 3) запись в таблицу
        ws.append_rows(rows, value_input_option="RAW")
        save_state(st, ch, rows[-1][3])

        # 4) отправка в бота
        if BOT_TOKEN and CHAT_IDS:
            for r in rows:
                pub, _, chan, link, title, text = r
                msg = build_post_message(chan, pub, link, title, text)
                for cid in CHAT_IDS:
                    tg_send_message(BOT_TOKEN, cid, msg, disable_preview=False)

        print(f"[append+send] {ch}: {len(rows)} rows")

if __name__ == "__main__":
    main()
