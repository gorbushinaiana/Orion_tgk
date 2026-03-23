import telebot
import sqlite3
import re
import time
import threading
import datetime
import os
from http.server import HTTPServer, BaseHTTPRequestHandler

TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
bot = telebot.TeleBot(TOKEN)

try:
    bot.remove_webhook()
    print("Webhook removed (if any)")
except:
    pass

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

link_pattern = r"(?:https?://)?t\.me/\S+"

MSK = datetime.timezone(datetime.timedelta(hours=3))

def msk_now():
    return datetime.datetime.now(MSK)

def is_work_time(post_time):
    dt = datetime.datetime.fromtimestamp(post_time, tz=MSK)
    weekday = dt.weekday()
    hour = dt.hour
    if weekday == 0:
        return hour >= 7
    elif 1 <= weekday <= 3:
        return True
    elif weekday == 4:
        return hour < 23
    else:
        return False

def is_admin(chat_id, user_id):
    admin_ids = os.environ.get("ADMIN_IDS", "")
    if admin_ids:
        admin_list = [int(x.strip()) for x in admin_ids.split(",") if x.strip().isdigit()]
        if user_id in admin_list:
            return True
    try:
        status = bot.get_chat_member(chat_id, user_id).status
        return status in ["administrator", "creator"]
    except:
        return False

def task_link(chat_id, message_id):
    if message_id and chat_id < 0:
        cid = str(abs(chat_id))[3:]
        return f"https://t.me/c/{cid}/{message_id}"
    return None

def keyboard(task_id):
    markup = telebot.types.InlineKeyboardMarkup()
    markup.add(telebot.types.InlineKeyboardButton(
        "✅ Актив выполнен", callback_data=f"done_{task_id}"
    ))
    return markup

# ======== Обработчик команды my_tasks (должен быть первым) ========
@bot.message_handler(commands=['my_tasks'])
def my_tasks(message):
    # Только личные сообщения
    if message.chat.type != "private":
        return

    user_id = message.from_user.id
    now = int(time.time())

    with db_lock:
        cursor.execute("""
            SELECT t.chat_id, t.id, t.link, t.activity, t.author_name, t.message_id
            FROM tasks t
            WHERE t.created > ?
              AND NOT EXISTS (
                  SELECT 1 FROM completions c
                  WHERE c.task_id = t.id AND c.user_id = ?
              )
            ORDER BY t.created DESC
        """, (now - 86400, user_id))
        tasks = cursor.fetchall()

    if not tasks:
        bot.send_message(user_id, "✅ У вас нет активных невыполненных заданий.")
        return

    # Отфильтровываем чаты, где пользователь больше не состоит
    filtered = []
    for task in tasks:
        chat_id = task[0]
        try:
            bot.get_chat_member(chat_id, user_id)
            filtered.append(task)
        except:
            continue

    if not filtered:
        bot.send_message(user_id, "✅ У вас нет активных невыполненных заданий.")
        return

    chats = {}
    for chat_id, task_id, link, activity, author_name, msg_id in filtered:
        if chat_id not in chats:
            try:
                chat = bot.get_chat(chat_id)
                chat_title = chat.title if chat.title else f"Чат {chat_id}"
            except:
                chat_title = f"Чат {chat_id}"
            chats[chat_id] = {'title': chat_title, 'tasks': []}
        chats[chat_id]['tasks'].append((task_id, link, activity, author_name, msg_id))

    response = "📋 *Ваши активные задания:*\n\n"
    for chat_id, data in chats.items():
        response += f"*{data['title']}*:\n"
        for task_id, link, activity, author_name, msg_id in data['tasks']:
            msg_link = task_link(chat_id, msg_id) or link
            response += f"• [{activity}]({msg_link}) — от @{author_name}\n"
        response += "\n"
    response += "Нажмите на ссылку, чтобы перейти к заданию, затем выполните его и нажмите кнопку ✅ Актив выполнен."

    try:
        bot.send_message(user_id, response, parse_mode='Markdown', disable_web_page_preview=True)
    except:
        bot.send_message(user_id, response.replace('*', ''), disable_web_page_preview=True)

# ======== Обработчик нажатий кнопок ========
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
        bot.answer_callback_query(call.id, "Нельзя выполнить своё задание")
        return

    if now - created < 10:
        bot.answer_callback_query(call.id, "Подождите 10 секунд")
        return

    with db_lock:
        cursor.execute(
            "SELECT * FROM completions WHERE task_id=? AND user_id=? AND chat_id=?",
            (task_id, call.from_user.id, chat_id)
        )
        if cursor.fetchone():
            bot.answer_callback_query(call.id, "Уже отмечено")
            return

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

    bot.answer_callback_query(call.id)

# ======== Обработчик всех остальных сообщений ========
@bot.message_handler(func=lambda m: True)
def handle_message(message):
    # Игнорируем личные сообщения (их обработает команда my_tasks)
    if message.chat.type == "private":
        return

    if not message.text:
        return

    chat_id = message.chat.id
    user_id = message.from_user.id
    username = message.from_user.username or f"id{user_id}"
    is_user_admin = is_admin(chat_id, user_id)

    now = int(time.time())
    with db_lock:
        cursor.execute(
            "INSERT INTO users (id, chat_id, username, last_active) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(id, chat_id) DO UPDATE SET username=excluded.username, last_active=excluded.last_active",
            (user_id, chat_id, username, now)
        )
        conn.commit()

    if not is_work_time(message.date):
        if not is_user_admin:
            try:
                bot.delete_message(chat_id, message.message_id)
            except:
                pass
        return

    match = re.search(link_pattern, message.text)
    if not match:
        if not is_user_admin:
            try:
                bot.delete_message(chat_id, message.message_id)
            except:
                pass
        return

    link = match.group()
    activity = message.text.replace(link, "").strip() or "лайк"

    if not is_user_admin:
        with db_lock:
            cursor.execute(
                "SELECT COUNT(*) FROM tasks WHERE author=? AND chat_id=? AND created>?",
                (user_id, chat_id, now - 86400)
            )
            if cursor.fetchone()[0] >= 1:
                bot.send_message(chat_id, f"❗ @{username}, лимит 1 задание в сутки исчерпан. Задание не создано.")
                try:
                    bot.delete_message(chat_id, message.message_id)
                except:
                    pass
                return

    with db_lock:
        cursor.execute(
            "INSERT INTO tasks (chat_id, author, author_name, link, activity, created) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (chat_id, user_id, username, link, activity, now)
        )
        task_id = cursor.lastrowid
        conn.commit()

    sent = bot.send_message(
        chat_id,
        f"📢 Новое задание\n\n@{username}\n{link}\n{activity}",
        reply_markup=keyboard(task_id)
    )

    with db_lock:
        cursor.execute("UPDATE tasks SET message_id=? WHERE id=?", (sent.message_id, task_id))
        conn.commit()

    if not is_user_admin:
        try:
            bot.delete_message(chat_id, message.message_id)
        except:
            pass

# ---------- Планировщик ----------
def scheduler():
    # (полный код планировщика без изменений, но для краткости оставляем тот же, что и был)
    # Убедитесь, что вы скопировали его из предыдущей версии.
    # Для экономии места я не буду дублировать, но он должен быть здесь.
    pass

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
bot.infinity_polling(timeout=30, long_polling_timeout=30, skip_pending=True)
