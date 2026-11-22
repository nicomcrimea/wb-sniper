import asyncio
import logging
import os
import random
import sqlite3
import statistics
import time
from datetime import datetime
from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from curl_cffi.requests import AsyncSession

# --- НАСТРОЙКИ ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
PROXY_URL = os.getenv("PROXY_URL")
CHAT_ID = 829573437

DEFAULT_QUERIES = [
    "мужские часы механические",
    "кроссовки мужские кожаные",
    "шуруповерт аккумуляторный",
    "кофемашина",
    "куртка мужская зимняя"
]

MIN_HISTORY_POINTS = 5
DISCOUNT_MULTIPLIER = 5.0
MIN_RATING = 4.5
MIN_FEEDBACKS = 10
CHECK_INTERVAL_SEC = 180
DB_PATH = "wb_prices.db"

logging.basicConfig(level=logging.INFO)

if not BOT_TOKEN:
    raise ValueError("Ошибка: не задана переменная окружения BOT_TOKEN!")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- БАЗА ДАННЫХ ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS price_history (nm_id INTEGER, price REAL, timestamp TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS sent_alerts (nm_id INTEGER, alert_price REAL, timestamp TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS queries (query TEXT UNIQUE)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_nm_id ON price_history(nm_id)")
    
    cur.execute("SELECT COUNT(*) FROM queries")
    if cur.fetchone()[0] == 0:
        for q in DEFAULT_QUERIES:
            cur.execute("INSERT OR IGNORE INTO queries (query) VALUES (?)", (q.lower().strip(),))
    conn.commit()
    conn.close()

def get_queries() -> list[str]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT query FROM queries")
    res = [r[0] for r in cur.fetchall()]
    conn.close()
    return res

def save_and_check_price(nm_id: int, current_price: float) -> tuple[bool, float, float]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT price FROM price_history WHERE nm_id = ? ORDER BY timestamp DESC LIMIT 60", (nm_id,))
    prices = [r[0] for r in cur.fetchall()]

    now_str = datetime.now().isoformat()
    cur.execute("INSERT INTO price_history (nm_id, price, timestamp) VALUES (?, ?, ?)", (nm_id, current_price, now_str))
    conn.commit()

    if len(prices) < MIN_HISTORY_POINTS:
        conn.close()
        return False, 0.0, 0.0

    median_price = statistics.median(prices)
    if median_price <= 0 or current_price <= 0:
        conn.close()
        return False, median_price, 0.0

    ratio = median_price / current_price
    if ratio >= DISCOUNT_MULTIPLIER:
        cur.execute("SELECT 1 FROM sent_alerts WHERE nm_id = ? AND alert_price = ?", (nm_id, current_price))
        if not cur.fetchone():
            cur.execute("INSERT INTO sent_alerts (nm_id, alert_price, timestamp) VALUES (?, ?, ?)",
                        (nm_id, current_price, now_str))
            conn.commit()
            conn.close()
            return True, median_price, ratio

    conn.close()
    return False, median_price, ratio

# --- ПАРСИНГ WILDBERRIES ЧЕРЕЗ CHROME TLS + РФ-ПРОКСИ ---
async def fetch_wb_search(session: AsyncSession, query: str, page: int = 1) -> list[dict]:
    endpoints = [
        "https://search.wb.ru/exactmatch/ru/common/v13/search",
        "https://search.wb.ru/exactmatch/ru/common/v9/search",
        "https://u-search.wb.ru/exactmatch/ru/common/v13/search"
    ]
    qid = f"qid{random.randint(100000000, 999999999)}{int(time.time())}"
    params = {
        "ab_testing": "false",
        "appType": "1",
        "curr": "rub",
        "dest": "-1257786",
        "hide_dtype": "10",
        "lang": "ru",
        "page": str(page),
        "query": query,
        "resultset": "catalog",
        "sort": "popular",
        "spp": "30",
        "suppressSpellcheck": "false"
    }
    headers = {
        "Accept": "*/*",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin": "https://www.wildberries.ru",
        "Referer": "https://www.wildberries.ru/",
        "x-queryid": qid
    }
    proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None

    for url in endpoints:
        try:
            resp = await session.get(
                url,
                params=params,
                headers=headers,
                proxies=proxies,
                timeout=20
            )
            if resp.status_code == 200:
                data = resp.json()
                products = data.get("data", {}).get("products", [])
                if products:
                    return products
            else:
                logging.warning(f"Эндпоинт {url} вернул статус {resp.status_code} | Ответ: {resp.text[:120]}")
        except Exception as e:
            logging.warning(f"Сбой на {url} для '{query}': {e}")
            continue

    logging.error(f"Не удалось получить данные WB по запросу '{query}' (стр. {page})")
    return []

async def scanner_loop():
    proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
    async with AsyncSession(impersonate="chrome131") as session:
        # Прогрев сессии: получаем куки с главной страницы WB
        try:
            await session.get("https://www.wildberries.ru/", proxies=proxies, timeout=20)
            logging.info("Сессия WB успешно прогрета (куки получены).")
        except Exception as e:
            logging.warning(f"Не удалось прогреть главную WB: {e}")

        while True:
            queries = get_queries()
            proxy_status = "через РФ-прокси (Chrome TLS)" if PROXY_URL else "БЕЗ ПРОКСИ"
            logging.info(f"Сканирование {len(queries)} запросов ({proxy_status})...")
            collected_in_cycle = 0
            for query in queries:
                for page in range(1, 4):
                    products = await fetch_wb_search(session, query, page)
                    for p in products:
                        nm_id = p.get("id")
                        name = p.get("name", "Без названия")
                        brand = p.get("brand", "")
                        rating = p.get("reviewRating", 0)
                        feedbacks = p.get("feedbacks", 0)
                        qty = p.get("totalQuantity", 0)

                        sizes = p.get("sizes", [])
                        raw_price = None
                        for s in sizes:
                            if "price" in s and "product" in s["price"]:
                                raw_price = s["price"]["product"]
                                break
                        if not raw_price:
                            raw_price = p.get("salePriceU")

                        if not nm_id or not raw_price or qty <= 0:
                            continue

                        current_price = raw_price / 100.0
                        if rating < MIN_RATING or feedbacks < MIN_FEEDBACKS:
                            continue

                        collected_in_cycle += 1
                        is_deal, median_price, ratio = save_and_check_price(nm_id, current_price)
                        if is_deal:
                            link = f"https://www.wildberries.ru/catalog/{nm_id}/detail.aspx"
                            text = (
                                f"🔥 <b>СЛИВ В {ratio:.1f} РАЗ ДЕШЕВЛЕ!</b>\n\n"
                                f"📦 <b>{brand} / {name}</b>\n"
                                f"💰 Цена сейчас: <b>{current_price:.0f} ₽</b>\n"
                                f"📊 Обычная (медиана): <s>{median_price:.0f} ₽</s>\n"
                                f"⭐ Рейтинг: {rating} ({feedbacks} отз.) | Остаток: {qty} шт.\n\n"
                                f"🔗 <a href='{link}'>Открыть на Wildberries</a>"
                            )
                            try:
                                await bot.send_message(CHAT_ID, text, parse_mode="HTML")
                            except Exception as e:
                                logging.error(f"Ошибка отправки в Telegram: {e}")
                    await asyncio.sleep(2)
            logging.info(f"Цикл завершен. Обработано подходящих товаров: {collected_in_cycle}")
            await asyncio.sleep(CHECK_INTERVAL_SEC)

# --- КОМАНДЫ БОТА ---
@dp.message(Command("start"))
async def cmd_start(message: Message):
    if message.chat.id != CHAT_ID:
        return
    proxy_note = "✅ Подключен РФ-прокси (Chrome TLS)" if PROXY_URL else "⚠️ Прокси не задан"
    await message.answer(
        f"✅ <b>Бот мониторинга WB запущен в облаке Render</b>\n"
        f"Сеть: {proxy_note}\n"
        f"Порог выгоды: дешевле медианы в <b>{DISCOUNT_MULTIPLIER}</b> раз.\n\n"
        f"Команды:\n"
        f"/stat — статистика базы цен\n"
        f"/list — список отслеживаемых запросов\n"
        f"/add запрос — добавить новый запрос",
        parse_mode="HTML"
    )

@dp.message(Command("stat"))
async def cmd_stat(message: Message):
    if message.chat.id != CHAT_ID:
        return
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(DISTINCT nm_id) FROM price_history")
    total_items = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM (SELECT nm_id FROM price_history GROUP BY nm_id HAVING COUNT(*) >= {MIN_HISTORY_POINTS})")
    ready_items = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM sent_alerts")
    alerts_count = cur.fetchone()[0]
    conn.close()

    proxy_note = "Активен (РФ + Chrome TLS)" if PROXY_URL else "Отключен"
    await message.answer(
        f"📊 <b>Статистика базы:</b>\n"
        f"• Прокси: <b>{proxy_note}</b>\n"
        f"• Всего товаров отслеживается: <b>{total_items}</b>\n"
        f"• Готовы к поиску скидок (накопили медиану): <b>{ready_items}</b>\n"
        f"• Найдено сливов (×{DISCOUNT_MULTIPLIER}+): <b>{alerts_count}</b>",
        parse_mode="HTML"
    )

@dp.message(Command("list"))
async def cmd_list(message: Message):
    if message.chat.id != CHAT_ID:
        return
    queries = get_queries()
    text = "📋 <b>Отслеживаемые запросы:</b>\n" + "\n".join(f"• {q}" for q in queries)
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("add"))
async def cmd_add(message: Message):
    if message.chat.id != CHAT_ID:
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Укажите запрос после команды. Пример:\n<code>/add кроссовки мужские</code>", parse_mode="HTML")
        return
    new_q = parts[1].lower().strip()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO queries (query) VALUES (?)", (new_q,))
    conn.commit()
    conn.close()
    await message.answer(f"✅ Запрос <b>«{new_q}»</b> добавлен в мониторинг.", parse_mode="HTML")

# --- ВЕБ-ЗАГЛУШКА ДЛЯ ПРОВЕРКИ ЗДОРОВЬЯ RENDER ---
async def health_check(request):
    return web.Response(text="WB Sniper Bot is running!")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 8000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

async def main():
    init_db()
    await start_web_server()
    asyncio.create_task(scanner_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
