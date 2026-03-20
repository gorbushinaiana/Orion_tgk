import telebot
import sqlite3
import re
import time
import threading
import datetime
import os
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler

# ----- Токен (временное решение, позже вернём через переменную окружения) -----
TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]

# ----- Инициализация бота с увеличенным таймаутом -----
bot = telebot.TeleBot(TOKEN)

# ----- Проверка соединения с Telegram перед стартом -----
print("Проверяю соединение с Telegram API...")
try:
    me = bot.get_me()
    print(f"✅ Бот подключён: @{me.username}")
except Exception as e:
    print(f"❌ Не удалось подключиться к Telegram API: {e}")
    sys.exit(1)

# ----- Остальной код без изменений (база данных, обработчики, планировщик) -----
conn = sqlite3.connect("db.db", check_same_thread=False)
cursor = conn.cursor()
db_lock = threading.Lock()

with db_lock:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER,
            chat_id INTEGER,
            username TEXT,
            last_active INTEGER,
            PRIMARY KEY(id, chat_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            author INTEGER,
            author_name TEXT,
            link TEXT,
            activity TEXT,
            created INTEGER,
            message_id INTEGER
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS completions (
            task_id INTEGER,
            chat_id INTEGER,
            user_id INTEGER,
            username TEXT,
            time INTEGER,
            verified INTEGER DEFAULT 0
        )
    """)
    conn.commit()

link_pattern = r"https://t.me/\S+"

MSK = datetime.timezone(datetime.timedelta(hours=3))

def msk_now():
    return datetime.datetime.now(MSK)

def is_work_time(post_time):
    dt = datetime.datetime.fromtimestamp(post_time, tz=MSK)
    weekday = dt.weekday()
    hour = dt.hour
    if weekday == 0:        # понедельник
        return hour >= 7
    elif 1 <= weekday <= 3: # вторник–четверг
        return True
    elif weekday == 4:      # пятница
        return hour < 23
    else:                   # суббота, воскресенье
        return False

def is_admin(chat_id, user_id):
    try:
        status = bot.get_chat_member(chat_id, user_id).status
        return status in ["administrator", "creator"]
    except:
        return False

def task_link(chat_id, message_id):
    if message_id and chat_id < 0:
        cid = str(abs(chat_id))[3:]  # убираем -100
        return f"https://t.me/c/{cid}/{message_id}"
    return None

def keyboard(task_id):
    markup = telebot.types.InlineKeyboardMarkup()
    markup.add(telebot.types.InlineKeyboardButton(
        "✅ Актив выполнен", callback_data=f"done_{task_id}"
    ))
    return markup

@bot.message_handler(func=lambda m: True)
def new_task(message):
    if not message.text:
        return

    chat_id = message.chat.id
    match = re.search(link_pattern, message.text)
    if not match:
        if not is_admin(chat_id, message.from_user.id):
            try:
                bot.delete_message(chat_id, message.message_id)
            except:
                pass
        return

    if not is_work_time(message.date):
        return

    link = match.group()
    activity = message.text.replace(link, "").strip() or "лайк"
    now = int(time.time())

    with db_lock:
        cursor.execute(
            "SELECT COUNT(*) FROM tasks WHERE author=? AND chat_id=? AND created>?",
            (message.from_user.id, chat_id, now - 86400)
        )
        if cursor.fetchone()[0] >= 2:
            bot.send_message(chat_id, "❗ Лимит 2 задания в сутки")
            return

    with db_lock:
        cursor.execute(
            "INSERT INTO tasks (chat_id, author, author_name, link, activity, created) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (chat_id, message.from_user.id, message.from_user.username, link, activity, now)
        )
        task_id = cursor.lastrowid
        conn.commit()

    sent = bot.send_message(
        chat_id,
        f"📢 Новое задание\n\n@{message.from_user.username}\n{link}\n{activity}",
        reply_markup=keyboard(task_id)
    )

    with db_lock:
        cursor.execute("UPDATE tasks SET message_id=? WHERE id=?", (sent.message_id, task_id))
        conn.commit()

    if not is_admin(chat_id, message.from_user.id):
        try:
            bot.delete_message(chat_id, message.message_id)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("done_"))
def done(call):
    task_id = int(call.data.split("_")[1])
    now = int(time.time())

    with db_lock:
        cursor.execute("SELECT created, chat_id, author FROM tasks WHERE id=?", (task_id,))
        task = cursor.fetchone()

    if not task:
        bot.answer_callback_query(call.id, "Задание не найдено")
        return

    created, chat_id, author_id = task

    if call.from_user.id == author_id:
        bot.answer_callback_query(call.id, "⚠️ Нельзя выполнить своё задание")
        return

    if now - created < 10:
        bot.answer_callback_query(call.id, "⚠️ Подождите 10 секунд")
        return

    with db_lock:
        cursor.execute(
            "SELECT * FROM completions WHERE task_id=? AND user_id=? AND chat_id=?",
            (task_id, call.from_user.id, chat_id)
        )
        if cursor.fetchone():
            bot.answer_callback_query(call.id, "Уже отмечено")
            return

    bot.answer_callback_query(call.id, "Засчитано ✅")

    with db_lock:
        cursor.execute(
            "INSERT INTO completions (task_id, chat_id, user_id, username, time, verified) "
            "VALUES (?, ?, ?, ?, ?, 1)",
            (task_id, chat_id, call.from_user.id, call.from_user.username, now)
        )
        cursor.execute(
            "INSERT OR REPLACE INTO users (id, chat_id, username, last_active) "
            "VALUES (?, ?, ?, ?)",
            (call.from_user.id, chat_id, call.from_user.username, now)
        )
        conn.commit()

    try:
        bot.send_message(
            chat_id,
            f"✅ Пользователь @{call.from_user.username} выполнил задание."
        )
    except:
        pass

# ---------- Планировщик (без изменений) ----------
def scheduler():
    weekly_reported = set()
    friday_notified = set()
    monday_notified = set()

    while True:
        now = int(time.time())
        now_dt = msk_now()
        day = now_dt.weekday()
        hour = now_dt.hour
        week_num = now_dt.isocalendar()[1]

        with db_lock:
            cursor.execute("SELECT DISTINCT chat_id FROM users")
            chats = {r[0] for r in cursor.fetchall()}
            cursor.execute("SELECT DISTINCT chat_id FROM tasks")
            chats |= {r[0] for r in cursor.fetchall()}

        for chat_id in chats:
            # --- Пятница 23:00 ---
            fri_key = (chat_id, week_num)
            if day == 4 and hour == 23 and fri_key not in friday_notified:
                try:
                    bot.send_message(chat_id, "🌙 Пост-чат ушел на выходные! Актив по желанию")
                except:
                    pass
                friday_notified.add(fri_key)

            # --- Понедельник 7:00 ---
            mon_key = (chat_id, week_num)
            if day == 0 and hour == 7 and mon_key not in monday_notified:
                try:
                    bot.send_message(chat_id, "☀️ Доброе утро, пост-чат работает в нормальном режиме")
                except:
                    pass
                monday_notified.add(mon_key)

            # --- Истекшие задания (24 часа) ---
            with db_lock:
                cursor.execute(
                    "SELECT id, created, author, author_name, message_id, link FROM tasks WHERE chat_id=?",
                    (chat_id,)
                )
                tasks = cursor.fetchall()

            for task in tasks:
                task_id, created, author_id, author_name, msg_id, link = task
                if now - created > 86400:
                    with db_lock:
                        cursor.execute(
                            "SELECT username FROM completions WHERE task_id=? AND chat_id=?",
                            (task_id, chat_id)
                        )
                        done_users = {x[0] for x in cursor.fetchall()}
                        cursor.execute(
                            "SELECT username FROM users WHERE chat_id=?",
                            (chat_id,)
                        )
                        all_users = {x[0] for x in cursor.fetchall()}

                    # Исключаем автора и админов
                    admins = set()
                    for u in all_users:
                        with db_lock:
                            cursor.execute("SELECT id FROM users WHERE username=? AND chat_id=?", (u, chat_id))
                            row = cursor.fetchone()
                        if row and is_admin(chat_id, row[0]):
                            admins.add(u)

                    not_done = (all_users - done_users) - {author_name} - admins

                    link_msg = task_link(chat_id, msg_id) or link
                    if not_done:
                        text = "❌ Не выполнили задание"
                        if link_msg:
                            text += f" ({link_msg})"
                        text += ":\n\n" + "\n".join([f"@{u}" for u in not_done if u])
                    else:
                        text = "✅ Все выполнили задание"
                        if link_msg:
                            text += f" ({link_msg})"

                    try:
                        bot.send_message(chat_id, text)
                    except Exception as e:
                        print(f"Ошибка отправки отчёта: {e}")

                    with db_lock:
                        cursor.execute("DELETE FROM tasks WHERE id=?", (task_id,))
                        conn.commit()

            # --- Недельный отчёт (воскресенье 12:00) ---
            week_key = (chat_id, week_num)
            if day == 6 and hour == 12 and week_key not in weekly_reported:
                week_ago = now - 604800
                with db_lock:
                    cursor.execute(
                        "SELECT username FROM users WHERE chat_id=? AND last_active<?",
                        (chat_id, week_ago)
                    )
                    inactive = [f"@{x[0]}" for x in cursor.fetchall() if x[0]]
                    cursor.execute(
                        "SELECT username, COUNT(*) as c FROM completions WHERE chat_id=? "
                        "GROUP BY user_id ORDER BY c DESC LIMIT 5",
                        (chat_id,)
                    )
                    top = cursor.fetchall()

                text = "📊 **Недельный отчёт**\n\n"
                if inactive:
                    text += "❌ Неактивные:\n" + "\n".join(inactive) + "\n\n"
                else:
                    text += "✅ Все активны!\n\n"
                if top:
                    text += "🏆 **Топ по выполнениям:**\n"
                    for t in top:
                        text += f"@{t[0]} — {t[1]}\n"

                try:
                    bot.send_message(chat_id, text, parse_mode="Markdown")
                except:
                    pass
                weekly_reported.add(week_key)

        time.sleep(60)

# ---------- Health-сервер ----------
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        pass

def run_health_server():
    port = int(os.environ.get("PORT", 8000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    server.serve_forever()

threading.Thread(target=run_health_server, daemon=True).start()
threading.Thread(target=scheduler, daemon=True).start()

print("Бот запущен...")
bot.infinity_polling()
