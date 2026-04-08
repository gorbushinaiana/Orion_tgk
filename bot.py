import telebot
import sqlite3
import re
import time
import threading
import datetime
import os
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler
from collections import defaultdict
import logging
import signal
import sys

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== КОНФИГУРАЦИЯ ====================
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN environment variable not set")
    sys.exit(1)

bot = telebot.TeleBot(TOKEN)

# Время жизни задания (48 часов, чтобы пользователи успевали)
TASK_LIFETIME = 48 * 3600      # 48 часов
USER_TASK_LIMIT_PERIOD = 12 * 3600
MAX_TASKS_PER_USER = 2

MSK = datetime.timezone(datetime.timedelta(hours=3))
link_pattern = r"(?:https?://)?t\.me/\S+"

# ==================== БАЗА ДАННЫХ ====================
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
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bot_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_completions_task_user ON completions(task_id, user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_completions_chat ON completions(chat_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_chat_id ON users(chat_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tasks_chat_id ON tasks(chat_id)")
    conn.commit()

# ==================== КЭШ СТАТУСОВ УЧАСТНИКОВ ====================
members_cache = {}
CACHE_TTL = 300

def get_cached_member(chat_id, user_id, force_refresh=False):
    cache_key = (chat_id, user_id)
    now = time.time()
    if not force_refresh and cache_key in members_cache:
        status, ts = members_cache[cache_key]
        if now - ts < CACHE_TTL:
            return status
    try:
        member = bot.get_chat_member(chat_id, user_id)
        status = member.status
        members_cache[cache_key] = (status, now)
        return status
    except Exception as e:
        logger.warning(f"Failed to get member {user_id} in chat {chat_id}: {e}")
        return None

def is_admin_cached(chat_id, user_id):
    admin_ids = os.environ.get("ADMIN_IDS", "")
    if admin_ids:
        admin_list = [int(x.strip()) for x in admin_ids.split(",") if x.strip().isdigit()]
        if user_id in admin_list:
            return True
    if chat_id is None:
        return False
    status = get_cached_member(chat_id, user_id)
    return status in ["administrator", "creator"]

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
def msk_now():
    return datetime.datetime.now(MSK)

def is_weekend_period(now_ts=None):
    if now_ts is None:
        now_ts = int(time.time())
    dt = datetime.datetime.fromtimestamp(now_ts, tz=MSK)
    weekday = dt.weekday()
    hour = dt.hour
    if weekday == 4 and hour >= 23:
        return True
    if weekday == 5 or weekday == 6:
        return True
    if weekday == 0 and hour < 7:
        return True
    return False

def is_work_time(post_time):
    if is_weekend_period(post_time):
        return False
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

def reset_telegram_webhook(token):
    try:
        resp = requests.post(f"https://api.telegram.org/bot{token}/deleteWebhook", json={"drop_pending_updates": True})
        if resp.status_code == 200:
            logger.info("Webhook reset successfully")
        else:
            logger.error(f"Webhook reset failed: {resp.text}")
    except Exception as e:
        logger.error(f"Error resetting webhook: {e}")

# ==================== ОБРАБОТКА ИСТЕКШИХ ЗАДАНИЙ ====================
def process_expired_task(task_id, chat_id, author_id, author_name, msg_id, link):
    logger.info(f"Processing expired task {task_id} in chat {chat_id}")
    with db_lock:
        cursor.execute("SELECT username FROM completions WHERE task_id=? AND chat_id=?", (task_id, chat_id))
        done_users = {row[0] for row in cursor.fetchall()}
        cursor.execute("SELECT username, id FROM users WHERE chat_id=?", (chat_id,))
        all_users = {row[0]: row[1] for row in cursor.fetchall()}

    not_done = []
    for uname, uid in all_users.items():
        if uname == author_name or is_admin_cached(chat_id, uid):
            continue
        if uname in done_users:
            continue
        status = get_cached_member(chat_id, uid)
        if status in ["member", "administrator", "creator"]:
            mention = f"@{uname}" if uname else f"id{uid}"
            not_done.append(mention)

    link_msg = task_link(chat_id, msg_id) or link

    if not_done:
        chunk = []
        chunk_len = 0
        for mention in not_done:
            mention_with_newline = mention + "\n"
            if chunk_len + len(mention_with_newline) > 4000:
                text = "❌ Не выполнили задание"
                if link_msg:
                    text += f" ({link_msg})"
                text += ":\n\n" + "".join(chunk)
                try:
                    bot.send_message(chat_id, text)
                except Exception as e:
                    logger.error(f"Failed to send chunk: {e}")
                chunk = [mention_with_newline]
                chunk_len = len(mention_with_newline)
            else:
                chunk.append(mention_with_newline)
                chunk_len += len(mention_with_newline)
        if chunk:
            text = "❌ Не выполнили задание"
            if link_msg:
                text += f" ({link_msg})"
            text += ":\n\n" + "".join(chunk)
            try:
                bot.send_message(chat_id, text)
            except Exception as e:
                logger.error(f"Failed to send final chunk: {e}")
    else:
        text = "✅ Все выполнили задание"
        if link_msg:
            text += f" ({link_msg})"
        try:
            bot.send_message(chat_id, text)
        except Exception as e:
            logger.error(f"Failed to send completion report: {e}")

# ==================== КОМАНДЫ ====================
@bot.message_handler(commands=['stats'])
def stats(message):
    if message.chat.type != "private":
        return
    if not is_admin_cached(None, message.from_user.id):
        bot.send_message(message.chat.id, "У вас нет прав.")
        return
    with db_lock:
        cursor.execute("SELECT COUNT(*) FROM tasks WHERE created > ?", (int(time.time()) - TASK_LIFETIME,))
        active_tasks = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM completions WHERE time > ?", (int(time.time()) - USER_TASK_LIMIT_PERIOD,))
        completions_last_period = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM tasks")
        total_tasks = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM users")
        total_users = cursor.fetchone()[0]
    text = (f"📊 Статистика бота:\n\n"
            f"Активных заданий (последние {TASK_LIFETIME//3600} ч): {active_tasks}\n"
            f"Выполнено за последние {USER_TASK_LIMIT_PERIOD//3600} ч: {completions_last_period}\n"
            f"Всего заданий: {total_tasks}\n"
            f"Всего пользователей: {total_users}")
    bot.send_message(message.chat.id, text)

@bot.message_handler(commands=['my_tasks'])
def my_tasks(message):
    if message.chat.type != "private":
        return
    user_id = message.from_user.id
    now = int(time.time())
    with db_lock:
        cursor.execute("""
            SELECT t.chat_id, t.id, t.link, t.activity, t.author_name, t.message_id
            FROM tasks t
            WHERE t.created > ?
              AND t.author != ?
              AND NOT EXISTS (
                  SELECT 1 FROM completions c
                  WHERE c.task_id = t.id AND c.user_id = ?
              )
            ORDER BY t.created DESC
        """, (now - TASK_LIFETIME, user_id, user_id))
        tasks = cursor.fetchall()
    if not tasks:
        bot.send_message(user_id, "✅ У вас нет активных невыполненных заданий.")
        return
    filtered = []
    for task in tasks:
        chat_id = task[0]
        status = get_cached_member(chat_id, user_id)
        if status in ["left", "kicked", None]:
            # Если статус не определён, пробуем обновить кеш
            if status is None:
                status = get_cached_member(chat_id, user_id, force_refresh=True)
            if status in ["left", "kicked", None]:
                continue
        if status in ["administrator", "creator"]:
            continue
        filtered.append(task)
    if not filtered:
        bot.send_message(user_id, "✅ У вас нет активных невыполненных заданий.")
        return
    tasks_by_chat = defaultdict(list)
    for chat_id, task_id, link, activity, author_name, msg_id in filtered:
        tasks_by_chat[chat_id].append((task_id, link, activity, author_name, msg_id))
    response = "📋 *Ваши активные задания:*\n\n"
    for chat_id, tasks_list in tasks_by_chat.items():
        try:
            chat_info = bot.get_chat(chat_id)
            chat_title = chat_info.title if chat_info else f"Чат {chat_id}"
        except:
            chat_title = f"Чат {chat_id}"
        response += f"*{chat_title}*:\n"
        for task_id, link, activity, author_name, msg_id in tasks_list:
            msg_link = task_link(chat_id, msg_id) or link
            response += f"• [Задание]({msg_link})\n"
        response += "\n"
    response += "Нажмите на ссылку, чтобы перейти к заданию, затем выполните его и нажмите кнопку ✅ Актив выполнен."
    try:
        bot.send_message(user_id, response, parse_mode='Markdown', disable_web_page_preview=True)
    except Exception as e:
        bot.send_message(user_id, response.replace('*', ''), disable_web_page_preview=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("done_"))
def done(call):
    task_id = int(call.data.split("_")[1])
    user_id = call.from_user.id
    username = call.from_user.username or f"id{user_id}"
    now = int(time.time())

    logger.info(f"Callback done: task_id={task_id}, user_id={user_id}, username={username}")

    # Блокируем на всё время обработки, чтобы избежать гонок
    with db_lock:
        # 1. Проверяем, существует ли задание и не истекло ли оно
        cursor.execute("SELECT created, chat_id, author FROM tasks WHERE id=?", (task_id,))
        task = cursor.fetchone()

        if not task:
            logger.warning(f"Task {task_id} not found in tasks table (expired or deleted)")
            bot.answer_callback_query(call.id, f"❌ Задание недоступно. Возможно, прошло более {TASK_LIFETIME//3600} часов.")
            return

        created, chat_id, author_id = task
        logger.info(f"Task found: created={created}, chat_id={chat_id}, author={author_id}")

        # 2. Проверка на авторство
        if user_id == author_id:
            bot.answer_callback_query(call.id, "❌ Нельзя выполнить своё собственное задание.")
            return

        # 3. Защита от нажатия сразу после создания (10 секунд)
        if now - created < 10:
            bot.answer_callback_query(call.id, "⏳ Подождите 10 секунд после создания задания.")
            return

        # 4. Проверка членства в чате (с принудительным обновлением кеша)
        status = get_cached_member(chat_id, user_id)
        if status not in ["member", "administrator", "creator"]:
            # Пробуем обновить кеш
            status = get_cached_member(chat_id, user_id, force_refresh=True)
            if status not in ["member", "administrator", "creator"]:
                logger.info(f"User {user_id} not a member of chat {chat_id}, status={status}")
                bot.answer_callback_query(call.id, "❌ Вы не состоите в этом чате. Вступите и попробуйте снова.")
                return

        # 5. Проверяем, не выполнял ли пользователь это задание ранее
        cursor.execute("SELECT time FROM completions WHERE task_id=? AND user_id=? AND chat_id=?", (task_id, user_id, chat_id))
        existing = cursor.fetchone()
        if existing:
            done_time = existing[0]
            done_dt = datetime.datetime.fromtimestamp(done_time, tz=MSK).strftime("%Y-%m-%d %H:%M")
            logger.info(f"User {user_id} already completed task {task_id} at {done_time}")
            bot.answer_callback_query(call.id, f"✅ Вы уже отмечали это задание {done_dt} (МСК). Повторно не требуется.")
            return

        # 6. Всё хорошо, отмечаем выполнение
        cursor.execute(
            "INSERT INTO completions (task_id, chat_id, user_id, username, time, verified) VALUES (?, ?, ?, ?, ?, 1)",
            (task_id, chat_id, user_id, username, now)
        )
        cursor.execute(
            "INSERT OR REPLACE INTO users (id, chat_id, username, last_active) VALUES (?, ?, ?, ?)",
            (user_id, chat_id, username, now)
        )
        conn.commit()
        logger.info(f"Completion recorded: task_id={task_id}, user_id={user_id}")

    bot.answer_callback_query(call.id, "✅ Задание выполнено! Спасибо.")
    # Дополнительно можно отредактировать сообщение, убрав кнопку
    try:
        bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
    except Exception as e:
        logger.warning(f"Could not remove inline keyboard: {e}")

@bot.message_handler(func=lambda m: True)
def handle_message(message):
    if message.chat.type == "private":
        return
    if not message.text:
        return
    if message.from_user.is_bot:
        return
    chat_id = message.chat.id
    user_id = message.from_user.id
    username = message.from_user.username or f"id{user_id}"
    is_user_admin = is_admin_cached(chat_id, user_id)
    now = int(time.time())

    with db_lock:
        cursor.execute(
            "INSERT INTO users (id, chat_id, username, last_active) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(id, chat_id) DO UPDATE SET username=excluded.username, last_active=excluded.last_active",
            (user_id, chat_id, username, now)
        )
        conn.commit()

    # Проверка рабочего времени
    if not is_work_time(message.date):
        if not is_user_admin:
            try:
                bot.delete_message(chat_id, message.message_id)
            except Exception as e:
                logger.warning(f"Failed to delete message (out of work time): {e}")
        return

    # Поиск ссылки t.me
    match = re.search(link_pattern, message.text)
    if not match:
        if not is_user_admin:
            try:
                bot.delete_message(chat_id, message.message_id)
            except Exception as e:
                logger.warning(f"Failed to delete message (no link): {e}")
        return

    link = match.group()
    activity = message.text.replace(link, "").strip() or "лайк"

    # Лимит заданий для не-админов
    if not is_user_admin:
        with db_lock:
            cursor.execute("SELECT COUNT(*) FROM tasks WHERE author=? AND chat_id=? AND created>?", (user_id, chat_id, now - USER_TASK_LIMIT_PERIOD))
            count = cursor.fetchone()[0]
            if count >= MAX_TASKS_PER_USER:
                bot.send_message(chat_id, f"❗ @{username}, лимит {MAX_TASKS_PER_USER} задания за {USER_TASK_LIMIT_PERIOD//3600} часов исчерпан. Подождите.")
                try:
                    bot.delete_message(chat_id, message.message_id)
                except Exception as e:
                    logger.warning(f"Failed to delete message (limit exceeded): {e}")
                return

    # Создаём задание
    with db_lock:
        cursor.execute(
            "INSERT INTO tasks (chat_id, author, author_name, link, activity, created) VALUES (?, ?, ?, ?, ?, ?)",
            (chat_id, user_id, username, link, activity, now)
        )
        task_id = cursor.lastrowid
        conn.commit()

    sent = bot.send_message(chat_id, f"📢 Новое задание\n\n@{username}\n{link}\n{activity}", reply_markup=keyboard(task_id))

    with db_lock:
        cursor.execute("UPDATE tasks SET message_id=? WHERE id=?", (sent.message_id, task_id))
        conn.commit()

    if not is_user_admin:
        try:
            bot.delete_message(chat_id, message.message_id)
        except Exception as e:
            logger.warning(f"Failed to delete original message: {e}")

# ==================== ПЛАНИРОВЩИК ====================
def get_state(key, default=None):
    with db_lock:
        cursor.execute("SELECT value FROM bot_state WHERE key=?", (key,))
        row = cursor.fetchone()
        return row[0] if row else default

def set_state(key, value):
    with db_lock:
        cursor.execute("INSERT OR REPLACE INTO bot_state (key, value) VALUES (?, ?)", (key, str(value)))
        conn.commit()

def scheduler():
    logger.info("Scheduler started")
    while True:
        now = int(time.time())
        now_dt = msk_now()
        day = now_dt.weekday()
        hour = now_dt.hour
        week_num = now_dt.isocalendar()[1]

        # Пятница 23:00
        if day == 4 and hour == 23:
            last_friday = get_state("last_friday_week")
            if last_friday != str(week_num):
                with db_lock:
                    cursor.execute("SELECT DISTINCT chat_id FROM tasks")
                    chats = [row[0] for row in cursor.fetchall()]
                for chat_id in chats:
                    try:
                        bot.send_message(chat_id, "🌙 Пост-чат ушел на выходные! Актив по желанию")
                    except Exception as e:
                        logger.error(f"Failed to send friday message in {chat_id}: {e}")
                set_state("last_friday_week", week_num)

        # Понедельник 7:00
        if day == 0 and hour == 7:
            last_monday = get_state("last_monday_week")
            if last_monday != str(week_num):
                with db_lock:
                    cursor.execute("SELECT DISTINCT chat_id FROM tasks")
                    chats = [row[0] for row in cursor.fetchall()]
                for chat_id in chats:
                    try:
                        bot.send_message(chat_id, "☀️ Доброе утро, пост-чат работает в нормальном режиме")
                    except Exception as e:
                        logger.error(f"Failed to send monday message in {chat_id}: {e}")
                set_state("last_monday_week", week_num)

        # Удаление истекших заданий (только в рабочее время, чтобы не мешать)
        if not is_weekend_period(now):
            with db_lock:
                cursor.execute("SELECT id, chat_id, author, author_name, message_id, link, created FROM tasks WHERE created <= ?", (now - TASK_LIFETIME,))
                expired = cursor.fetchall()
            for task in expired:
                task_id, chat_id, author_id, author_name, msg_id, link, created = task
                logger.info(f"Task {task_id} expired, processing...")
                process_expired_task(task_id, chat_id, author_id, author_name, msg_id, link)
                with db_lock:
                    cursor.execute("DELETE FROM tasks WHERE id=?", (task_id,))
                    conn.commit()

        # Еженедельный отчёт по субботам в 12:00
        if day == 5 and hour == 12:   # суббота
            last_report = get_state("last_report_week")
            if last_report != str(week_num):
                week_ago = now - 604800
                with db_lock:
                    cursor.execute("SELECT DISTINCT chat_id FROM users")
                    chats = [row[0] for row in cursor.fetchall()]
                for chat_id in chats:
                    with db_lock:
                        cursor.execute("SELECT username FROM users WHERE chat_id=? AND last_active<?", (chat_id, week_ago))
                        inactive = [f"@{row[0]}" for row in cursor.fetchall() if row[0]]
                        cursor.execute("""
                            SELECT username, COUNT(*) as c FROM completions
                            WHERE chat_id=? GROUP BY user_id ORDER BY c DESC LIMIT 5
                        """, (chat_id,))
                        top = cursor.fetchall()
                    text = "📊 **Недельный отчёт**\n\n"
                    if inactive:
                        text += "❌ Неактивные:\n" + ("\n".join(inactive[:50]) + ("\n... и ещё" if len(inactive)>50 else "")) + "\n\n"
                    else:
                        text += "✅ Все активны!\n\n"
                    if top:
                        text += "🏆 **Топ по выполнениям:**\n"
                        for username, count in top:
                            text += f"@{username} — {count}\n"
                    try:
                        bot.send_message(chat_id, text, parse_mode="Markdown")
                    except:
                        bot.send_message(chat_id, text.replace('*', ''))
                set_state("last_report_week", week_num)

        time.sleep(60)

# ==================== HEALTH-СЕРВЕР ====================
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

def signal_handler(signum, frame):
    logger.info("Shutting down gracefully...")
    bot.stop_polling()
    conn.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    reset_telegram_webhook(TOKEN)
    time.sleep(5)
    threading.Thread(target=run_health_server, daemon=True).start()
    threading.Thread(target=scheduler, daemon=True).start()
    logger.info("Бот запущен...")
    while True:
        try:
            bot.infinity_polling(timeout=60, long_polling_timeout=60, skip_pending=True)
        except telebot.apihelper.ApiTelegramException as e:
            logger.error(f"Polling error: {e}")
            if "409" in str(e):
                reset_telegram_webhook(TOKEN)
                time.sleep(10)
            else:
                time.sleep(30)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            time.sleep(30)
