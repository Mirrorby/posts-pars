# Telegram RSS → Google Sheets

Парсит **публичные** Telegram-каналы через RSSHub и каждые 5 минут добавляет новые посты в Google Таблицу.

## Быстрый старт
1. Создайте Google Таблицу (например `Telegram Posts Inbox`) и лист `Posts`.
2. Создайте сервис-аккаунт (включите *Google Sheets API* и *Google Drive API*), скачайте JSON-ключ.
3. Поделитесь таблицей с `client_email` сервис-аккаунта (Editor).
4. В GitHub репозитории создайте Secrets:
   - `GSHEET_TITLE` — название таблицы
   - `SHEET_NAME` — название листа (например, `Posts`)
   - `GCP_JSON` — **полный** JSON-ключ (вставьте весь текст)
5. Запустите workflow вручную (Actions → **RSS to Sheets** → Run workflow). Дальше будет работать каждые 5 минут.

## Настройки
- Каналы по умолчанию: `MELOCHOV, ABKS07, jjsbossj, toolsSADA`.
- Можно задать свои через `CHANNELS_CSV` (в workflow или как Secret), формат: `name1,name2,...` (без `@`, можно с t.me/…).
- `RSS_BASE` — адрес инстанса RSSHub (по умолчанию публичный `https://rsshub.app`).
- `INITIAL_LIMIT` — сколько последних записей брать при **самом первом** запуске (по каждому каналу).

## Формат таблицы
Авто-создаются листы:
- `Posts`: `Date | Channel | Title | Link | Text`
- `State`: `Channel | LastLink` (техническое хранение «последней» ссылки для дедупа)

## Траблы
- `PERMISSION_DENIED` → не выдали сервис-аккаунту доступ к таблице (Share → Editor).
- Пустой фид/ошибки → публичный RSSHub перегружен, поставьте свой и укажите `RSS_BASE`.
- Хотите «перечитать с нуля» канал → очистите его строку в листе `State` (столбец LastLink).
