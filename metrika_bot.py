"""
Telegram-бот для анализа корреляций целей Яндекс.Метрики.
Автоопределение периода, автосохранение настроек, метрика ср. срабатываний/неделя.
"""
import os
import sys
import json
import re
import logging
import traceback
from datetime import datetime, date, timedelta

import requests
import pandas as pd
from dotenv import load_dotenv
import telebot
from telebot import types

# ─── Настройки ────────────────────────────────────────────
load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
METRIKA_TOKEN = os.getenv("METRIKA_OAUTH_TOKEN", "")
ALLOWED_USER_ID = os.getenv("ALLOWED_USER_ID", "").strip()

if not BOT_TOKEN or not METRIKA_TOKEN:
    raise SystemExit("Нет TELEGRAM_BOT_TOKEN или METRIKA_OAUTH_TOKEN в .env / env vars")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
HEADERS = {"Authorization": f"OAuth {METRIKA_TOKEN}"}

THRESHOLD = 0.4  # умеренная корреляция и выше

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

# ─── Persistent State (JSON) ──────────────────────────────
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state.json")


def _load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_state(state: dict):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"Не удалось сохранить state: {e}")


STATE = _load_state()


def st(chat_id) -> dict:
    key = str(chat_id)
    if key not in STATE:
        STATE[key] = {
            "counter_id": None,
            "date1": None,   # может быть None → будет вычислен автоматически
            "date2": None,   # может быть None → будет today
            "main_goal_ids": [],
        }
        _save_state(STATE)
    return STATE[key]


def save():
    _save_state(STATE)


# ─── Guard ─────────────────────────────────────────────────
def guard(message):
    if not ALLOWED_USER_ID:
        return True
    return str(message.from_user.id) == ALLOWED_USER_ID


def guard_cb(call):
    if not ALLOWED_USER_ID:
        return True
    return str(call.from_user.id) == ALLOWED_USER_ID


# ─── Метрика API ──────────────────────────────────────────
def api_get(url, params=None, timeout=120):
    """GET с обработкой ошибок."""
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json(), None
    except requests.exceptions.HTTPError as e:
        body = ""
        try:
            body = e.response.text[:500]
        except Exception:
            pass
        return None, f"HTTP {e.response.status_code}: {body}"
    except requests.exceptions.RequestException as e:
        return None, f"Ошибка запроса: {e}"


def get_goals(counter_id: str):
    """Получить dict {goal_id: goal_name}."""
    url = f"https://api-metrika.yandex.net/management/v1/counter/{counter_id}/goals"
    data, err = api_get(url, timeout=60)
    if err:
        return None, err
    goals = data.get("goals", [])
    return {int(g["id"]): g.get("name", f"goal_{g['id']}") for g in goals}, None


def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def fetch_daily_reaches(counter_id: str, date1: str, date2: str, goal_ids: list):
    """Получить DataFrame: index=date, columns=goal_id, values=reaches."""
    metrics = [f"ym:s:goal{gid}reaches" for gid in goal_ids]
    out = None

    for metrics_part in chunked(metrics, 20):
        params = {
            "id": counter_id,
            "metrics": ",".join(metrics_part),
            "dimensions": "ym:s:date",
            "date1": date1,
            "date2": date2,
            "limit": 100000,
        }
        data, err = api_get(
            "https://api-metrika.yandex.net/stat/v1/data",
            params=params, timeout=120
        )
        if err:
            return None, err

        rows = []
        for item in data.get("data", []):
            dt = item["dimensions"][0]["name"]
            values = item["metrics"]
            row = {"date": dt}
            for m, v in zip(metrics_part, values):
                gid = int(re.findall(r"goal(\d+)reaches", m)[0])
                row[gid] = v
            rows.append(row)

        if rows:
            part_df = pd.DataFrame(rows).set_index("date")
        else:
            part_df = pd.DataFrame()

        if out is None:
            out = part_df
        else:
            out = out.join(part_df, how="outer")

    if out is None or out.empty:
        return pd.DataFrame(), None

    return out.fillna(0), None


def detect_first_trigger_date(counter_id: str, goal_ids: list) -> str | None:
    """
    Определить дату первого срабатывания любой из указанных целей.
    Ищем по годам (2015..сегодня), чтобы не упереться в лимит
    "Query is too complicated" при запросе за весь период сразу.
    Для каждой цели берём самую раннюю дату.
    """
    today_d = date.today()
    earliest_date = None

    for gid in goal_ids:
        found_for_goal = None
        for year in range(2015, today_d.year + 1):
            d1 = f"{year}-01-01"
            d2 = f"{year}-12-31" if year < today_d.year else today_d.isoformat()

            params = {
                "id": counter_id,
                "metrics": f"ym:s:goal{gid}reaches",
                "dimensions": "ym:s:date",
                "date1": d1,
                "date2": d2,
                "sort": "ym:s:date",
                "limit": 1000,
            }
            data, err = api_get(
                "https://api-metrika.yandex.net/stat/v1/data",
                params=params, timeout=60
            )
            if err or not data:
                log.warning(f"detect_first_trigger goal={gid} year={year}: {err}")
                continue

            for item in data.get("data", []):
                reaches = item["metrics"][0]
                if reaches and reaches > 0:
                    found_for_goal = item["dimensions"][0]["name"]
                    break  # нашли первый день в этом году

            if found_for_goal:
                break  # нашли год — дальше не ищем

        if found_for_goal:
            log.info(f"Goal {gid}: first trigger = {found_for_goal}")
            if earliest_date is None or found_for_goal < earliest_date:
                earliest_date = found_for_goal

    return earliest_date


# ─── Корреляции ────────────────────────────────────────────
def compute_correlations(df: pd.DataFrame, main_goal_ids: list):
    """
    Считаем корреляцию каждой цели с объединённой главной целью.
    Возвращает (dict {goal_id: corr_value}, error_string).
    """
    present_main = [gid for gid in main_goal_ids if gid in df.columns]
    if not present_main:
        return None, "Главные цели не найдены в данных за период."

    # Объединённая главная цель: сумма срабатываний выбранных целей за день
    main_series = df[present_main].sum(axis=1)
    if main_series.sum() == 0:
        return None, "По главным целям за период нет достижений — корреляция невозможна."

    results = {}
    for gid in df.columns:
        if gid in present_main:
            continue
        if df[gid].sum() == 0:
            continue
        c = df[gid].corr(main_series)
        if pd.notna(c) and abs(c) >= THRESHOLD:
            results[int(gid)] = float(c)

    return results, None


def compute_weekly_rate(df: pd.DataFrame, goal_ids: list) -> dict:
    """
    Среднее количество срабатываний каждой цели в неделю.
    Формула: total_reaches / (кол-во дней в периоде / 7).
    """
    if df.empty:
        return {}

    num_days = len(df)
    num_weeks = max(num_days / 7.0, 1.0)

    rates = {}
    for gid in goal_ids:
        if gid in df.columns:
            total = df[gid].sum()
            rates[int(gid)] = round(total / num_weeks, 2)

    return rates


# ─── Форматирование результата ─────────────────────────────
def strength_label(c: float) -> str:
    a = abs(c)
    if a >= 0.9:
        return "🔴 очень сильная"
    if a >= 0.7:
        return "🟠 сильная"
    if a >= 0.5:
        return "🟡 заметная"
    return "🔵 умеренная"


def format_results(results: dict, weekly_rates: dict, goals_map: dict,
                   main_goal_ids: list, date1: str, date2: str) -> str:
    """Красиво форматирует результат для Telegram."""
    main_names = [goals_map.get(gid, str(gid)) for gid in main_goal_ids if gid in goals_map]
    header = (
        f"<b>📊 Корреляции целей Яндекс.Метрики</b>\n"
        f"<b>Главные цели:</b> {', '.join(main_names)}\n"
        f"<b>Период:</b> {date1} — {date2}\n"
        f"<b>Порог:</b> |r| ≥ {THRESHOLD}\n"
        f"{'─' * 30}\n"
    )

    # Средние срабатывания главных целей
    main_rate_lines = []
    for gid in main_goal_ids:
        rate = weekly_rates.get(gid, 0)
        name = goals_map.get(gid, str(gid))
        main_rate_lines.append(f"  • {name}: <b>{rate}</b>/нед")

    header += "<b>📈 Ср. срабатыв. главных целей в неделю:</b>\n"
    header += "\n".join(main_rate_lines) + "\n"
    header += f"{'─' * 30}\n"

    if not results:
        return header + "\n⚠️ Корреляций умеренной силы и выше — не найдено."

    ranked = sorted(results.items(), key=lambda x: abs(x[1]), reverse=True)
    lines = []
    for i, (gid, c) in enumerate(ranked, 1):
        name = goals_map.get(gid, str(gid))
        direction = "↗️" if c > 0 else "↘️"
        rate = weekly_rates.get(gid, 0)
        label = strength_label(c)
        lines.append(
            f"<b>{i}.</b> {name}\n"
            f"   r = <b>{c:+.3f}</b> {direction} {label}\n"
            f"   📊 ср. {rate}/нед"
        )

    body = "\n\n".join(lines)
    return header + "\n" + body


# ─── Telegram handlers ────────────────────────────────────

@bot.message_handler(commands=["start", "help"])
def cmd_start(message):
    if not guard(message):
        return
    s = st(message.chat.id)
    status_lines = []
    if s["counter_id"]:
        status_lines.append(f"📍 Счётчик: <b>{s['counter_id']}</b>")
    if s["main_goal_ids"]:
        status_lines.append(f"🎯 Главные цели: <code>{', '.join(map(str, s['main_goal_ids']))}</code>")
    if s["date1"] and s["date2"]:
        status_lines.append(f"📅 Период: {s['date1']} — {s['date2']}")
    else:
        status_lines.append("📅 Период: <i>авто (с первого срабатывания)</i>")

    status = "\n".join(status_lines) if status_lines else "—"

    bot.send_message(
        message.chat.id,
        f"<b>🤖 Бот корреляций Яндекс.Метрики</b>\n\n"
        f"<b>Текущие настройки:</b>\n{status}\n\n"
        f"<b>Команды:</b>\n"
        f"/counter <code>&lt;id&gt;</code> — установить счётчик\n"
        f"/goals — показать список целей счётчика\n"
        f"/main <code>&lt;id,id,...&gt;</code> — задать главные цели\n"
        f"/period <code>&lt;YYYY-MM-DD&gt; &lt;YYYY-MM-DD&gt;</code> — задать период вручную\n"
        f"/autoperiod — сбросить период на авто\n"
        f"/run — запустить анализ\n"
        f"/status — текущие настройки\n"
        f"/reset — сбросить все настройки\n\n"
        f"<i>Период определяется автоматически: от первого срабатывания главной цели до сегодня. "
        f"Можно переопределить командой /period.</i>",
    )


@bot.message_handler(commands=["counter"])
def cmd_counter(message):
    if not guard(message):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().isdigit():
        bot.send_message(message.chat.id, "Пример: <code>/counter 44147844</code>")
        return
    cid = args[1].strip()
    st(message.chat.id)["counter_id"] = cid
    # сбрасываем цели при смене счётчика
    st(message.chat.id)["main_goal_ids"] = []
    st(message.chat.id)["date1"] = None
    st(message.chat.id)["date2"] = None
    save()
    bot.send_message(message.chat.id, f"✅ Счётчик установлен: <b>{cid}</b>\n\nТеперь используйте /goals чтобы увидеть цели.")


@bot.message_handler(commands=["goals"])
def cmd_goals(message):
    if not guard(message):
        return
    s = st(message.chat.id)
    if not s["counter_id"]:
        bot.send_message(message.chat.id, "⚠️ Сначала задайте счётчик: /counter <id>")
        return

    bot.send_message(message.chat.id, "⏳ Загружаю список целей…")
    goals_map, err = get_goals(s["counter_id"])
    if err:
        bot.send_message(message.chat.id, f"❌ Ошибка: {err}")
        return
    if not goals_map:
        bot.send_message(message.chat.id, "⚠️ У этого счётчика нет целей.")
        return

    lines = [f"<b>Цели счётчика {s['counter_id']}:</b>\n"]
    for gid, name in sorted(goals_map.items()):
        marker = " ⭐" if gid in s.get("main_goal_ids", []) else ""
        lines.append(f"<code>{gid}</code> — {name}{marker}")

    text = "\n".join(lines)
    # Telegram ограничивает 4096 символов
    if len(text) > 4000:
        for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
            bot.send_message(message.chat.id, chunk)
    else:
        bot.send_message(message.chat.id, text)


@bot.message_handler(commands=["main"])
def cmd_main(message):
    if not guard(message):
        return
    s = st(message.chat.id)
    if not s["counter_id"]:
        bot.send_message(message.chat.id, "⚠️ Сначала задайте счётчик: /counter <id>")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        bot.send_message(message.chat.id, "Пример: <code>/main 255302693,257143059</code>\n\nИспользуйте /goals чтобы увидеть ID целей.")
        return
    ids = [int(x.strip()) for x in args[1].split(",") if x.strip().isdigit()]
    if not ids:
        bot.send_message(message.chat.id, "⚠️ Не распознаны ID целей. Пример: <code>/main 123,456</code>")
        return
    # Проверяем что цели принадлежат этому счётчику
    goals_map, err = get_goals(s["counter_id"])
    if err:
        bot.send_message(message.chat.id, f"❌ Ошибка проверки целей: {err}")
        return
    bad_ids = [gid for gid in ids if gid not in goals_map]
    if bad_ids:
        bot.send_message(
            message.chat.id,
            f"⚠️ Цели {', '.join(map(str, bad_ids))} <b>не найдены</b> в счётчике {s['counter_id']}.\n"
            f"Проверьте ID через /goals"
        )
        return
    name_list = [f"{gid} ({goals_map[gid]})" for gid in ids]
    s["main_goal_ids"] = ids
    # Сбросить ручной период → автоопределение
    s["date1"] = None
    s["date2"] = None
    save()
    bot.send_message(
        message.chat.id,
        f"✅ Главные цели:\n" +
        "\n".join(f"  • {n}" for n in name_list) + "\n\n"
        f"📅 Период будет определён автоматически при /run.\n"
        f"Или задайте вручную: /period"
    )


@bot.message_handler(commands=["period"])
def cmd_period(message):
    if not guard(message):
        return
    args = message.text.split()
    if len(args) != 3:
        bot.send_message(message.chat.id, "Пример: <code>/period 2025-01-01 2025-12-31</code>")
        return
    # Валидация формата дат
    for d in [args[1], args[2]]:
        try:
            datetime.strptime(d, "%Y-%m-%d")
        except ValueError:
            bot.send_message(message.chat.id, f"⚠️ Неверный формат даты: {d}. Нужен YYYY-MM-DD.")
            return

    st(message.chat.id)["date1"] = args[1]
    st(message.chat.id)["date2"] = args[2]
    save()
    bot.send_message(message.chat.id, f"✅ Период установлен: <b>{args[1]} — {args[2]}</b>")


@bot.message_handler(commands=["autoperiod"])
def cmd_autoperiod(message):
    if not guard(message):
        return
    st(message.chat.id)["date1"] = None
    st(message.chat.id)["date2"] = None
    save()
    bot.send_message(message.chat.id, "✅ Период сброшен на <b>автоопределение</b> (от первого срабатывания до сегодня).")


@bot.message_handler(commands=["status"])
def cmd_status(message):
    if not guard(message):
        return
    s = st(message.chat.id)
    lines = ["<b>📋 Текущие настройки:</b>\n"]
    lines.append(f"📍 Счётчик: <b>{s.get('counter_id', '—') or '—'}</b>")
    if s.get("main_goal_ids"):
        lines.append(f"🎯 Главные цели: <code>{', '.join(map(str, s['main_goal_ids']))}</code>")
    else:
        lines.append("🎯 Главные цели: <i>не заданы</i>")
    if s.get("date1") and s.get("date2"):
        lines.append(f"📅 Период: {s['date1']} — {s['date2']} (ручной)")
    else:
        lines.append("📅 Период: <i>авто (от первого срабатывания до сегодня)</i>")
    lines.append(f"\n🔧 Порог корреляции: |r| ≥ {THRESHOLD}")
    bot.send_message(message.chat.id, "\n".join(lines))


@bot.message_handler(commands=["reset"])
def cmd_reset(message):
    if not guard(message):
        return
    key = str(message.chat.id)
    if key in STATE:
        del STATE[key]
    save()
    bot.send_message(message.chat.id, "✅ Все настройки сброшены. Начните с /counter.")


@bot.message_handler(commands=["run"])
def cmd_run(message):
    if not guard(message):
        return
    s = st(message.chat.id)

    if not s["counter_id"]:
        bot.send_message(message.chat.id, "⚠️ Сначала задайте счётчик: /counter <id>")
        return
    if not s["main_goal_ids"]:
        bot.send_message(message.chat.id, "⚠️ Сначала задайте главные цели: /main <id,id,...>")
        return

    try:
        # 1. Определяем период
        date2 = s.get("date2") or date.today().isoformat()
        date1 = s.get("date1")

        if not date1:
            bot.send_message(message.chat.id, "⏳ Определяю дату первого срабатывания главных целей…")
            date1 = detect_first_trigger_date(s["counter_id"], s["main_goal_ids"])
            if not date1:
                bot.send_message(
                    message.chat.id,
                    "⚠️ Не удалось определить первое срабатывание. "
                    "Задайте период вручную: /period YYYY-MM-DD YYYY-MM-DD"
                )
                return
            bot.send_message(
                message.chat.id,
                f"📅 Автопериод: <b>{date1} — {date2}</b>\n"
                f"<i>(от первого срабатывания до сегодня)</i>"
            )

        # 2. Получаем список целей
        bot.send_message(message.chat.id, "⏳ Загружаю цели и данные…")
        goals_map, err = get_goals(s["counter_id"])
        if err:
            bot.send_message(message.chat.id, f"❌ Ошибка получения целей: {err}")
            return

        all_goal_ids = sorted(goals_map.keys())

        # 3. Получаем данные
        df, err = fetch_daily_reaches(s["counter_id"], date1, date2, all_goal_ids)
        if err:
            bot.send_message(message.chat.id, f"❌ Ошибка получения данных: {err}")
            return

        if df.empty:
            bot.send_message(message.chat.id, "⚠️ Нет данных за указанный период.")
            return

        # 4. Считаем корреляции
        results, err = compute_correlations(df, s["main_goal_ids"])
        if err:
            bot.send_message(message.chat.id, f"⚠️ {err}")
            return

        # 5. Считаем средние срабатывания в неделю
        weekly_rates = compute_weekly_rate(df, all_goal_ids)

        # 6. Форматируем и отправляем
        text = format_results(
            results or {}, weekly_rates, goals_map,
            s["main_goal_ids"], date1, date2
        )

        # Telegram ограничивает 4096 символов
        if len(text) > 4000:
            parts = []
            current = ""
            for line in text.split("\n"):
                if len(current) + len(line) + 1 > 3900:
                    parts.append(current)
                    current = line
                else:
                    current += "\n" + line if current else line
            if current:
                parts.append(current)
            for part in parts:
                bot.send_message(message.chat.id, part)
        else:
            bot.send_message(message.chat.id, text)

        log.info(f"Анализ завершён для chat_id={message.chat.id}, "
                 f"найдено {len(results or {})} корреляций.")

    except Exception as e:
        log.error(f"Ошибка при выполнении /run: {traceback.format_exc()}")
        bot.send_message(
            message.chat.id,
            f"❌ Произошла ошибка:\n<code>{str(e)[:500]}</code>\n\n"
            f"Попробуйте позже или проверьте настройки."
        )


# ─── Запуск ────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Бот запущен, ожидаю сообщения…")
    bot.infinity_polling(timeout=60, long_polling_timeout=60)
