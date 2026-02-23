"""
Telegram-бот для анализа корреляций целей Яндекс.Метрики.
Интерактивный UX: списки счётчиков и целей через inline-кнопки.
"""
import os
import sys
import json
import re
import logging
import traceback
from datetime import datetime, date

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

MSG_NO_COUNTER = "⚠️ Сначала выберите счётчик: /counters"

# ─── Persistent State ─────────────────────────────────────
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
            "counter_name": None,
            "date1": None,
            "date2": None,
            "main_goal_ids": [],
            # временный буфер: цели, выбранные через чекбоксы (до подтверждения)
            "pending_goal_ids": [],
        }
        _save_state(STATE)
    return STATE[key]


def save():
    _save_state(STATE)


# ─── Guard ────────────────────────────────────────────────
def guard(message):
    if not ALLOWED_USER_ID:
        log.error("ALLOWED_USER_ID не задан в окружении! Доступ запрещён для всех.")
        return False
    return str(message.from_user.id) == ALLOWED_USER_ID


def guard_cb(call):
    if not ALLOWED_USER_ID:
        return False
    return str(call.from_user.id) == ALLOWED_USER_ID


# ─── Metrika API ──────────────────────────────────────────
def api_get(url, params=None, timeout=120):
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


def get_counters():
    """Список счётчиков: [{id, name, site}]"""
    data, err = api_get(
        "https://api-metrika.yandex.net/management/v1/counters",
        params={"per_page": 100}, timeout=30
    )
    if err:
        return None, err
    counters = data.get("counters", [])
    return [
        {
            "id": str(c["id"]),
            "name": c.get("name") or c.get("site", str(c["id"])),
            "site": c.get("site", ""),
        }
        for c in counters
    ], None


def get_goals(counter_id: str):
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

        part_df = pd.DataFrame(rows).set_index("date") if rows else pd.DataFrame()
        out = part_df if out is None else out.join(part_df, how="outer")

    if out is None or out.empty:
        return pd.DataFrame(), None

    return out.fillna(0), None


def detect_first_trigger_date(counter_id: str, goal_ids: list) -> str | None:
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
                    break
            if found_for_goal:
                break

        if found_for_goal:
            log.info(f"Goal {gid}: first trigger = {found_for_goal}")
            if earliest_date is None or found_for_goal < earliest_date:
                earliest_date = found_for_goal

    return earliest_date


# ─── Аналитика ────────────────────────────────────────────
def compute_correlations(df: pd.DataFrame, main_goal_ids: list):
    present_main = [gid for gid in main_goal_ids if gid in df.columns]
    if not present_main:
        return None, "Главные цели не найдены в данных за период."
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
    if df.empty:
        return {}
    num_weeks = max(len(df) / 7.0, 1.0)
    return {
        int(gid): round(df[gid].sum() / num_weeks, 2)
        for gid in goal_ids if gid in df.columns
    }


def strength_label(c: float) -> str:
    a = abs(c)
    if a >= 0.9:
        return "🔴 очень сильная"
    if a >= 0.7:
        return "🟠 сильная"
    return "🔵 умеренная"


def format_results(results: dict, weekly_rates: dict, goals_map: dict,
                   main_goal_ids: list, date1: str, date2: str,
                   counter_name: str = "") -> str:
    main_names = [goals_map.get(gid, str(gid)) for gid in main_goal_ids if gid in goals_map]
    total_main_rate = sum(weekly_rates.get(gid, 0) for gid in main_goal_ids)

    header = (
        f"<b>📊 Анализ корреляций (|r| ≥ {THRESHOLD})</b>\n"
        f"<b>Счётчик:</b> {counter_name}\n"
        f"<b>Главные цели:</b> {', '.join(main_names)}\n"
        f"<b>Период:</b> {date1} — {date2}\n"
        f"{'─' * 30}\n"
        f"<b>📈 Суммарно главных: {total_main_rate:.2f}/нед</b>\n"
    )
    for gid in main_goal_ids:
        rate = weekly_rates.get(gid, 0)
        name = goals_map.get(gid, str(gid))
        header += f"  • {name}: <b>{rate}</b>/нед\n"
    header += f"{'─' * 30}\n"

    filtered = {gid: c for gid, c in results.items() if abs(c) >= THRESHOLD}

    if not filtered:
        return header + f"\n⚠️ Корреляций (|r| ≥ {THRESHOLD}) не найдено."

    # Сортировка:
    # 1. Приоритет (True/False): среднее кол-во в неделю больше суммы главных целей
    # 2. Сила корреляции |r| по убыванию
    ranked = sorted(
        filtered.items(),
        key=lambda x: (weekly_rates.get(x[0], 0) > total_main_rate, abs(x[1])),
        reverse=True
    )
    
    lines = []
    for i, (gid, c) in enumerate(ranked, 1):
        name = goals_map.get(gid, str(gid))
        direction = "↗️" if c > 0 else "↘️"
        rate = weekly_rates.get(gid, 0)
        label = strength_label(c)
        highlight = " 🔥 <b>(ВЫШЕ ГЛАВНОЙ)</b>" if rate > total_main_rate else ""
        lines.append(
            f"<b>{i}. {name}</b>{highlight}\n"
            f"   r = <b>{c:+.3f}</b> {direction} {label}\n"
            f"   📊 ср. <b>{rate}</b>/нед"
        )

    return header + "\n" + "\n\n".join(lines)


# ─── Inline keyboard helpers ──────────────────────────────

def _counters_keyboard(counters: list) -> types.InlineKeyboardMarkup:
    """Кнопки счётчиков — по одному в строку."""
    kb = types.InlineKeyboardMarkup(row_width=1)
    for c in counters:
        label = f"📍 {c['name']} ({c['site'] or c['id']})"
        kb.add(types.InlineKeyboardButton(label, callback_data=f"ctr:{c['id']}"))
    return kb


def _goals_keyboard(goals_map: dict, selected: list) -> types.InlineKeyboardMarkup:
    """
    Кнопки целей с чекбоксами (✅/☑️).
    Внизу кнопка «Запустить анализ» если хоть одна выбрана.
    """
    kb = types.InlineKeyboardMarkup(row_width=1)
    for gid, name in sorted(goals_map.items(), key=lambda x: x[0]):
        check = "✅" if gid in selected else "☑️"
        short_name = name[:40] + "…" if len(name) > 40 else name
        kb.add(types.InlineKeyboardButton(
            f"{check} {short_name}",
            callback_data=f"goal:{gid}"
        ))
    if selected:
        kb.add(types.InlineKeyboardButton(
            "🚀 Запустить анализ", callback_data="run_goals"
        ))
    kb.add(types.InlineKeyboardButton(
        "❌ Отмена", callback_data="cancel_goals"
    ))
    return kb


# ─── Основное меню ────────────────────────────────────────

def send_main_menu(chat_id):
    s = st(chat_id)

    counter_str = f"📍 <b>{s.get('counter_name') or s.get('counter_id') or 'не выбран'}</b>"
    main_ids = s.get("main_goal_ids", [])
    goals_str = f"🎯 Главных целей: <b>{len(main_ids)}</b>" if main_ids else "🎯 Цели: <i>не выбраны</i>"
    if s.get("date1") and s.get("date2"):
        period_str = f"📅 Период: {s['date1']} — {s['date2']} (ручной)"
    else:
        period_str = "📅 Период: <i>авто (с первого срабатывания)</i>"

    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("📋 Выбрать счётчик", callback_data="open_counters"),
        types.InlineKeyboardButton("🎯 Выбрать цели и запустить", callback_data="open_goals"),
        types.InlineKeyboardButton("🚀 Запустить анализ (текущие цели)", callback_data="run_now"),
        types.InlineKeyboardButton("📅 Статус и настройки", callback_data="show_status"),
    )

    bot.send_message(
        chat_id,
        f"<b>🤖 Бот корреляций Яндекс.Метрики</b>\n\n"
        f"{counter_str}\n{goals_str}\n{period_str}\n\n"
        f"Выберите действие:",
        reply_markup=kb
    )


# ─── Команды ──────────────────────────────────────────────

@bot.message_handler(commands=["start", "help"])
def cmd_start(message):
    if not guard(message):
        return
    send_main_menu(message.chat.id)


@bot.message_handler(commands=["counters"])
def cmd_counters(message):
    if not guard(message):
        return
    _show_counters(message.chat.id)


def _show_counters(chat_id):
    msg = bot.send_message(chat_id, "⏳ Загружаю список счётчиков…")
    counters, err = get_counters()
    if err:
        bot.edit_message_text(f"❌ Ошибка: {err}", chat_id, msg.message_id)
        return
    if not counters:
        bot.edit_message_text("⚠️ Счётчики не найдены.", chat_id, msg.message_id)
        return
    kb = _counters_keyboard(counters)
    bot.edit_message_text(
        f"<b>Выберите счётчик</b> ({len(counters)} шт.):",
        chat_id, msg.message_id,
        reply_markup=kb
    )


@bot.message_handler(commands=["goals"])
def cmd_goals(message):
    if not guard(message):
        return
    s = st(message.chat.id)
    if not s["counter_id"]:
        bot.send_message(message.chat.id, MSG_NO_COUNTER)
        return
    _show_goals(message.chat.id)


def _show_goals(chat_id):
    s = st(chat_id)
    if not s["counter_id"]:
        bot.send_message(chat_id, MSG_NO_COUNTER)
        return
    msg = bot.send_message(chat_id, "⏳ Загружаю цели…")
    goals_map, err = get_goals(s["counter_id"])
    if err:
        bot.edit_message_text(f"❌ Ошибка: {err}", chat_id, msg.message_id)
        return
    if not goals_map:
        bot.edit_message_text("⚠️ У этого счётчика нет целей.", chat_id, msg.message_id)
        return

    # Сохраняем карту целей в state для последующих callback
    s["_goals_cache"] = {str(k): v for k, v in goals_map.items()}
    s["_goals_msg_id"] = msg.message_id
    # pending = уже выбранные main_goal_ids (продолжаем с них)
    s["pending_goal_ids"] = list(s.get("main_goal_ids", []))
    save()

    name = s.get("counter_name") or s["counter_id"]
    kb = _goals_keyboard(goals_map, s["pending_goal_ids"])
    bot.edit_message_text(
        f"<b>Цели счётчика «{name}»</b>\n"
        f"✅ = выбрана как главная. Нажимайте чтобы выбрать/снять.\n"
        f"Затем нажмите «🚀 Запустить анализ».",
        chat_id, msg.message_id,
        reply_markup=kb
    )


@bot.message_handler(commands=["run"])
def cmd_run_cmd(message):
    if not guard(message):
        return
    _run_analysis(message.chat.id)


@bot.message_handler(commands=["status"])
def cmd_status(message):
    if not guard(message):
        return
    _show_status(message.chat.id)


def _show_status(chat_id):
    s = st(chat_id)
    lines = ["<b>📋 Текущие настройки:</b>\n"]
    lines.append(f"📍 Счётчик: <b>{s.get('counter_name') or s.get('counter_id') or '—'}</b>")
    if s.get("main_goal_ids"):
        goals_cache = {int(k): v for k, v in s.get("_goals_cache", {}).items()}
        goal_strs = []
        for gid in s["main_goal_ids"]:
            name = goals_cache.get(gid, str(gid))
            goal_strs.append(f"  • {name} (id {gid})")
        lines.append("🎯 Главные цели:\n" + "\n".join(goal_strs))
    else:
        lines.append("🎯 Главные цели: <i>не заданы</i>")
    if s.get("date1") and s.get("date2"):
        lines.append(f"📅 Период: {s['date1']} — {s['date2']} (ручной)")
    else:
        lines.append("📅 Период: <i>авто</i>")
    lines.append(f"\n🔧 Порог: |r| ≥ {THRESHOLD}")
    bot.send_message(chat_id, "\n".join(lines))


@bot.message_handler(commands=["period"])
def cmd_period(message):
    if not guard(message):
        return
    args = message.text.split()
    if len(args) != 3:
        bot.send_message(message.chat.id, "Пример: <code>/period 2025-01-01 2025-12-31</code>")
        return
    for d in [args[1], args[2]]:
        try:
            datetime.strptime(d, "%Y-%m-%d")
        except ValueError:
            bot.send_message(message.chat.id, f"⚠️ Неверный формат даты: {d}")
            return
    st(message.chat.id)["date1"] = args[1]
    st(message.chat.id)["date2"] = args[2]
    save()
    bot.send_message(message.chat.id, f"✅ Период: <b>{args[1]} — {args[2]}</b>")


@bot.message_handler(commands=["autoperiod"])
def cmd_autoperiod(message):
    if not guard(message):
        return
    st(message.chat.id)["date1"] = None
    st(message.chat.id)["date2"] = None
    save()
    bot.send_message(message.chat.id, "✅ Период сброшен на <b>автоопределение</b>.")


@bot.message_handler(commands=["reset"])
def cmd_reset(message):
    if not guard(message):
        return
    key = str(message.chat.id)
    if key in STATE:
        del STATE[key]
    save()
    bot.send_message(message.chat.id, "✅ Все настройки сброшены. Нажмите /counters чтобы начать.")


# ─── Callback-обработчики (inline кнопки) ─────────────────

@bot.callback_query_handler(func=lambda c: c.data == "open_counters")
def cb_open_counters(call):
    if not guard_cb(call):
        return
    bot.answer_callback_query(call.id)
    _show_counters(call.message.chat.id)


@bot.callback_query_handler(func=lambda c: c.data == "open_goals")
def cb_open_goals(call):
    if not guard_cb(call):
        return
    bot.answer_callback_query(call.id)
    s = st(call.message.chat.id)
    if not s["counter_id"]:
        bot.send_message(call.message.chat.id, MSG_NO_COUNTER)
        return
    _show_goals(call.message.chat.id)


@bot.callback_query_handler(func=lambda c: c.data == "run_now")
def cb_run_now(call):
    if not guard_cb(call):
        return
    bot.answer_callback_query(call.id)
    _run_analysis(call.message.chat.id)


@bot.callback_query_handler(func=lambda c: c.data == "show_status")
def cb_show_status(call):
    if not guard_cb(call):
        return
    bot.answer_callback_query(call.id)
    _show_status(call.message.chat.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("ctr:"))
def cb_select_counter(call):
    if not guard_cb(call):
        return
    counter_id = call.data.split(":", 1)[1]

    # Достаём название счётчика из текста кнопки
    counter_name = counter_id
    for row in call.message.reply_markup.keyboard:
        for btn in row:
            if btn.callback_data == call.data:
                # Убираем эмодзи "📍 " из начала
                counter_name = btn.text.replace("📍 ", "").strip()

    s = st(call.message.chat.id)
    s["counter_id"] = counter_id
    s["counter_name"] = counter_name
    s["main_goal_ids"] = []
    s["pending_goal_ids"] = []
    s["date1"] = None
    s["date2"] = None
    save()

    bot.answer_callback_query(call.id, f"✅ Выбран: {counter_name}")
    bot.edit_message_text(
        f"✅ Счётчик: <b>{counter_name}</b>\n\n⏳ Загружаю цели…",
        call.message.chat.id, call.message.message_id
    )
    # Сразу показываем цели
    _show_goals_inline(call.message.chat.id, call.message.message_id)


def _show_goals_inline(chat_id, msg_id):
    """Показать цели, редактируя существующее сообщение."""
    s = st(chat_id)
    goals_map, err = get_goals(s["counter_id"])
    if err:
        bot.edit_message_text(f"❌ Ошибка загрузки целей: {err}", chat_id, msg_id)
        return
    if not goals_map:
        bot.edit_message_text("⚠️ У этого счётчика нет целей.", chat_id, msg_id)
        return

    s["_goals_cache"] = {str(k): v for k, v in goals_map.items()}
    s["_goals_msg_id"] = msg_id
    s["pending_goal_ids"] = []
    save()

    name = s.get("counter_name") or s["counter_id"]
    kb = _goals_keyboard(goals_map, [])
    bot.edit_message_text(
        f"<b>Цели счётчика «{name}»</b> ({len(goals_map)} шт.)\n\n"
        f"Нажимайте на цели чтобы выбрать главные.\n"
        f"Затем нажмите «🚀 Запустить анализ».",
        chat_id, msg_id,
        reply_markup=kb
    )


@bot.callback_query_handler(func=lambda c: c.data.startswith("goal:"))
def cb_toggle_goal(call):
    if not guard_cb(call):
        return
    gid = int(call.data.split(":", 1)[1])
    s = st(call.message.chat.id)

    pending = list(s.get("pending_goal_ids", []))
    if gid in pending:
        pending.remove(gid)
        bot.answer_callback_query(call.id, "☑️ Снято")
    else:
        pending.append(gid)
        bot.answer_callback_query(call.id, "✅ Выбрано")

    s["pending_goal_ids"] = pending
    save()

    # Перестраиваем клавиатуру
    goals_map = {int(k): v for k, v in s.get("_goals_cache", {}).items()}
    if not goals_map:
        bot.answer_callback_query(call.id, "⚠️ Нет кэша целей, откройте /goals заново.")
        return

    kb = _goals_keyboard(goals_map, pending)
    try:
        bot.edit_message_reply_markup(
            call.message.chat.id, call.message.message_id, reply_markup=kb
        )
    except Exception:
        pass  # Если ничего не изменилось — Telegram вернёт ошибку "message not modified"


@bot.callback_query_handler(func=lambda c: c.data == "run_goals")
def cb_run_goals(call):
    if not guard_cb(call):
        return
    bot.answer_callback_query(call.id, "🚀 Запускаю анализ…")
    s = st(call.message.chat.id)

    pending = s.get("pending_goal_ids", [])
    if not pending:
        bot.send_message(call.message.chat.id, "⚠️ Не выбрано ни одной цели.")
        return

    # Фиксируем выбор
    s["main_goal_ids"] = pending
    s["pending_goal_ids"] = []
    s["date1"] = None  # сбрасываем период → автоопределение
    s["date2"] = None
    save()

    # Убираем кнопки из сообщения с целями
    try:
        goals_map = {int(k): v for k, v in s.get("_goals_cache", {}).items()}
        selected_names = [goals_map.get(g, str(g)) for g in pending]
        bot.edit_message_text(
            f"✅ Главные цели сохранены:\n" +
            "\n".join(f"  • {n}" for n in selected_names),
            call.message.chat.id, call.message.message_id
        )
    except Exception:
        pass

    _run_analysis(call.message.chat.id)


@bot.callback_query_handler(func=lambda c: c.data == "cancel_goals")
def cb_cancel_goals(call):
    if not guard_cb(call):
        return
    bot.answer_callback_query(call.id, "Отменено")
    s = st(call.message.chat.id)
    s["pending_goal_ids"] = []
    save()
    try:
        bot.edit_message_text("❌ Выбор целей отменён.", call.message.chat.id, call.message.message_id)
    except Exception:
        pass


# ─── Основной анализ ──────────────────────────────────────

def _run_analysis(chat_id):
    s = st(chat_id)

    if not s.get("counter_id"):
        bot.send_message(chat_id, MSG_NO_COUNTER)
        return
    if not s.get("main_goal_ids"):
        bot.send_message(chat_id, "⚠️ Не выбраны главные цели. Используйте /goals чтобы выбрать.")
        return

    try:
        date2 = s.get("date2") or date.today().isoformat()
        date1 = s.get("date1")

        if not date1:
            msg = bot.send_message(chat_id, "⏳ Определяю дату первого срабатывания…")
            date1 = detect_first_trigger_date(s["counter_id"], s["main_goal_ids"])
            if not date1:
                bot.edit_message_text(
                    "⚠️ Не удалось определить первое срабатывание.\n"
                    "Задайте период вручную: <code>/period ГГГГ-ММ-ДД ГГГГ-ММ-ДД</code>",
                    chat_id, msg.message_id
                )
                return
            bot.edit_message_text(
                f"📅 Автопериод: <b>{date1} — {date2}</b>",
                chat_id, msg.message_id
            )

        msg2 = bot.send_message(chat_id, "⏳ Загружаю данные по целям…")
        goals_map, err = get_goals(s["counter_id"])
        if err:
            bot.edit_message_text(f"❌ Ошибка получения целей: {err}", chat_id, msg2.message_id)
            return

        all_goal_ids = sorted(goals_map.keys())
        df, err = fetch_daily_reaches(s["counter_id"], date1, date2, all_goal_ids)
        if err:
            bot.edit_message_text(f"❌ Ошибка данных: {err}", chat_id, msg2.message_id)
            return
        if df.empty:
            bot.edit_message_text("⚠️ Нет данных за период.", chat_id, msg2.message_id)
            return

        bot.edit_message_text("⏳ Считаю корреляции…", chat_id, msg2.message_id)

        results, err = compute_correlations(df, s["main_goal_ids"])
        if err:
            bot.edit_message_text(f"⚠️ {err}", chat_id, msg2.message_id)
            return

        weekly_rates = compute_weekly_rate(df, all_goal_ids)
        counter_name = s.get("counter_name") or s["counter_id"]
        text = format_results(
            results or {}, weekly_rates, goals_map,
            s["main_goal_ids"], date1, date2, counter_name
        )

        # Добавляем кнопку «повторный запуск»
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("🔄 Обновить", callback_data="run_now"))
        kb.add(types.InlineKeyboardButton("🎯 Сменить цели", callback_data="open_goals"))
        kb.add(types.InlineKeyboardButton("📋 Сменить счётчик", callback_data="open_counters"))

        bot.delete_message(chat_id, msg2.message_id)

        if len(text) > 4000:
            chunks = [text[i:i+3900] for i in range(0, len(text), 3900)]
            for chunk in chunks[:-1]:
                bot.send_message(chat_id, chunk)
            bot.send_message(chat_id, chunks[-1], reply_markup=kb)
        else:
            bot.send_message(chat_id, text, reply_markup=kb)

        log.info(f"Анализ chat={chat_id}: {len(results or {})} корреляций.")

    except Exception as e:
        log.error(f"Ошибка /run: {traceback.format_exc()}")
        bot.send_message(
            chat_id,
            f"❌ Ошибка:\n<code>{str(e)[:400]}</code>\n\nПопробуйте позже."
        )


# ─── Запуск ───────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Установка команд меню…")
    try:
        commands = [
            types.BotCommand("start",      "🏠 Главное меню"),
            types.BotCommand("counters",   "📋 Выбрать счётчик"),
            types.BotCommand("goals",      "🎯 Выбрать главные цели"),
            types.BotCommand("run",        "🚀 Запустить анализ"),
            types.BotCommand("status",     "📊 Текущие настройки"),
            types.BotCommand("period",     "📅 Задать период вручную"),
            types.BotCommand("autoperiod", "🔄 Авто-период"),
            types.BotCommand("reset",      "🗑 Сбросить всё"),
        ]
        bot.set_my_commands(commands)
        log.info("Команды меню установлены.")
    except Exception as e:
        log.error(f"Ошибка установки команд: {e}")

    log.info("Бот запущен, жду сообщений…")
    bot.infinity_polling(timeout=60, long_polling_timeout=60)
