from io import StringIO
import logging
import pandas as pd
import requests
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ================= НАСТРОЙКИ =================

# ВАЖНО: подставь новый токен!
TELEGRAM_BOT_TOKEN = "8258549580:AAFR0NAxUssOVIvL6CFflol3MEA2PrmZ5h4"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

CBONDS_OIS_1M_URL = "https://cbonds.ru/indexes/174337/"


# ================= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================

def _parse_percent(value: str) -> float:
    """
    Преобразует строки вида '16,3 %' → 16.3
    """
    s = str(value)
    s = s.replace("\xa0", " ")
    s = s.replace("%", "").strip()
    s = s.replace(" ", "")
    s = s.replace(",", ".")
    return float(s)


def fetch_rusfar_term():
    """
    Получаем данные OIS RUSFAR 1M и 6M с Cbonds.

    Возвращает:
        (date_str, value_1m, value_6m)
        либо None в случае ошибки
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; TelegramBot/1.0)"
        }
        resp = requests.get(CBONDS_OIS_1M_URL, headers=headers, timeout=15)
        resp.raise_for_status()

        tables = pd.read_html(StringIO(resp.text), decimal=",", thousands=" ")

        # Ищем таблицу "Индексы подгруппы"
        target_df = None
        for t in tables:
            cols = {str(c).strip() for c in t.columns}
            if {"Индекс", "Последнее значение", "Дата"}.issubset(cols):
                target_df = t
                break

        if target_df is None:
            raise ValueError("Не найдена таблица индексов на странице")

        df = target_df.copy()
        df["Индекс"] = df["Индекс"].astype(str)
        df["Дата"] = pd.to_datetime(df["Дата"], dayfirst=True, errors="coerce")

        row_1m = df[df["Индекс"].str.contains("OIS 1 M RUSFAR RUB", na=False)]
        row_6m = df[df["Индекс"].str.contains("OIS 6 M RUSFAR RUB", na=False)]

        if row_1m.empty or row_6m.empty:
            raise ValueError("Не удалось найти строки OIS 1M или 6M")

        val_1m = _parse_percent(row_1m["Последнее значение"].iloc[0])
        val_6m = _parse_percent(row_6m["Последнее значение"].iloc[0])

        date_1m = row_1m["Дата"].iloc[0]
        date_6m = row_6m["Дата"].iloc[0]

        # Обычно одинаковые, но возьмём максимальную
        date = max(d for d in [date_1m, date_6m] if pd.notna(d))
        date_str = date.strftime("%d.%m.%Y")

        return date_str, val_1m, val_6m

    except Exception as e:
        logger.exception("Ошибка получения данных RUSFAR: %s", e)
        return None


# ================= ХЕНДЛЕРЫ =================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я бот c индексами OIS RUSFAR.\n\n"
        "/rusfar — показать OIS RUSFAR 1M и 6M (последние данные Cbonds)"
    )


async def rusfar_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Получаю свежие данные OIS RUSFAR...")

    data = fetch_rusfar_term()
    if data is None:
        await update.message.reply_text("Ошибка. Не удалось получить данные с Cbonds 😔")
        return

    date, r1m, r6m = data

    await update.message.reply_text(
        f"OIS RUSFAR (МБ СПФИ OTC)\n"
        f"Дата: {date}\n\n"
        f"• OIS 1M RUSFAR: {r1m:.2f} %\n"
        f"• OIS 6M RUSFAR: {r6m:.2f} %"
    )


# ================= ЗАПУСК БОТА =================

def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("rusfar", rusfar_command))

    logger.info("Бот запущен.")
    app.run_polling()


if __name__ == "__main__":
    main()
