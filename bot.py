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

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
bot = telebot.TeleBot(TOKEN)

# Время жизни задания (24 часа)
TASK_LIFETIME = 24 * 3600
# Период для лимита создания заданий (12 часов)
USER_TASK_LIMIT_PERIOD = 12 * 3600
# Максимум заданий за период для обычных пользователей
MAX_TASKS_PER_USER = 2

# --- Принудительный сброс webhook и pending updates ---
def reset_telegram_webhook(token):
    try:
        resp = requests.post(f"https://api.telegram.org/bot{token}/deleteWebhook", json={"drop_pending_updates": True})
        if resp.status_code == 200:
            logger.info("Webhook reset successfully (drop_pending_updates=True)")
        else:
            logger.error(f"Webhook reset failed: {resp.text}")
    except Exception as e:
        logger.error(f"Error resetting webhook: {e}")

# --- База данных ---
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
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_completions_task_user ON completions(task_id, user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_completions_chat ON completions(chat_id)")
    conn.commit()

link_pattern = r"(?:https?://)?t\.me/\S+"
MSK = datetime.timezone(datetime.timedelta(hours=3))

def msk_now():
    return datetime.datetime.now(MSK)

def is_work_time(post_time):
    dt = datetime.datetime.fromtimestamp(post_time, tz=MSK)
    weekday = dt.weekday()
    hour = dt.hour
    if weekday == 0:          # понедельник
        return hour >= 7
    elif 1 <= weekday <= 3:   # вторник-четверг
        return True
    elif weekday == 4:        # пятница
        return hour < 23
    else:                     # суббота, воскресенье
        return False

def is_admin(chat_id, user_id):
    # Проверка глобальных администраторов из переменной окружения
    admin_ids = os.environ.get("ADMIN_IDS", "")
    if admin_ids:
        admin_list = [int(x.strip()) for x in admin_ids.split(",") if x.strip().isdigit()]
        if user_id in admin_list:
            return True

    # Если передан конкретный чат, проверяем статус в нём
    if chat_id is not None:
        try:
            status = bot.get_chat_member(chat_id, user_id).status
            return status in ["administrator", "creator"]
        except Exception as e:
            logger.warning(f"Failed to check admin status for user {user_id} in chat {chat_id}: {e}")
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

# --- Команда /stats (только глобальные админы) ---
@bot.message_handler(commands=['stats'])
def stats(message):
    if message.chat.type != "private":
        return
    user_id = message.from_user.id
    if not is_admin(None, user_id):
        bot.send_message(user_id, "У вас нет прав.")
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
    bot.send_message(user_id, text)

# --- Команда /debug_tasks (только глобальные админы) ---
@bot.message_handler(commands=['debug_tasks'])
def debug_tasks(message):
    if message.chat.type != "private":
        return
    user_id = message.from_user.id
    if not is_admin(None, user_id):
        bot.send_message(user_id, "У вас нет прав.")
        return

    with db_lock:
        cursor.execute("""
            SELECT id, chat_id, author_name, link, created, message_id
            FROM tasks
            ORDER BY created DESC
        """)
        tasks = cursor.fetchall()

    if not tasks:
        bot.send_message(user_id, "Нет заданий в базе.")
        return

    now = int(time.time())
    text = "📋 *Все задания в базе:*\n\n"
    for task in tasks:
        task_id, chat_id, author, link, created, msg_id = task
        age_hours = (now - created) / 3600
        status = "✅ истекло" if now - created > TASK_LIFETIME else f"⏳ {age_hours:.1f} ч"
        text += f"ID {task_id}: чат {chat_id}, автор @{author}, {status}\n"
        text += f"   ссылка: {link}\n"
    try:
        bot.send_message(user_id, text, parse_mode='Markdown', disable_web_page_preview=True)
    except Exception as e:
        bot.send_message(user_id, text.replace('*', ''), disable_web_page_preview=True)

# --- Команда /force_report (только глобальные админы) ---
@bot.message_handler(commands=['force_report'])
def force_report(message):
    if message.chat.type != "private":
        return
    user_id = message.from_user.id
    if not is_admin(None, user_id):
        bot.send_message(user_id, "У вас нет прав.")
        return

    bot.send_message(user_id, "Запускаю принудительный отчёт по истекшим заданиям...")
    now = int(time.time())
    with db_lock:
        cursor.execute("SELECT id, chat_id, author, author_name, message_id, link, created FROM tasks")
        tasks = cursor.fetchall()

    if not tasks:
        bot.send_message(user_id, "Нет заданий.")
        return

    processed = 0
    for task in tasks:
        task_id, chat_id, author_id, author_name, msg_id, link, created = task
        if now - created > TASK_LIFETIME:
            process_expired_task(task_id, chat_id, author_id, author_name, msg_id, link)
            with db_lock:
                cursor.execute("DELETE FROM tasks WHERE id=?", (task_id,))
                conn.commit()
            processed += 1
    bot.send_message(user_id, f"Отчёты отправлены для {processed} заданий.")

def process_expired_task(task_id, chat_id, author_id, author_name, msg_id, link):
    logger.info(f"Processing expired task {task_id} in chat {chat_id}")
    with db_lock:
        cursor.execute("SELECT username FROM completions WHERE task_id=? AND chat_id=?", (task_id, chat_id))
        done_users = {row[0] for row in cursor.fetchall()}
        cursor.execute("SELECT username, id FROM users WHERE chat_id=?", (chat_id,))
        all_users = {row[0]: row[1] for row in cursor.fetchall()}

    # Определяем администраторов чата и проверяем, кто реально в чате
    not_done = []
    for uname, uid in all_users.items():
        # Пропускаем автора и администраторов
        if uname == author_name or is_admin(chat_id, uid):
            continue
        # Пропускаем тех, кто уже выполнил задание
        if uname in done_users:
            continue

        # Проверяем, состоит ли пользователь в чате в данный момент
        try:
            member = bot.get_chat_member(chat_id, uid)
            if member.status in ["member", "administrator", "creator"]:
                # Пользователь в чате – добавляем в отчёт
                mention = f"@{uname}" if uname else f"id{uid}"
                not_done.append(mention)
            # Если статус "left" или "kicked", просто пропускаем
        except Exception as e:
            logger.warning(f"Failed to get member {uid} in chat {chat_id}: {e}")
            # Если ошибка (например, пользователь не в чате), пропускаем
            continue

    link_msg = task_link(chat_id, msg_id) or link

    if not_done:
        chunk_size = 100
        for i in range(0, len(not_done), chunk_size):
            chunk = not_done[i:i+chunk_size]
            text = "❌ Не выполнили задание"
            if link_msg:
                text += f" ({link_msg})"
            text += ":\n\n" + "\n".join(chunk)
            try:
                bot.send_message(chat_id, text)
                logger.info(f"Sent expiration report for task {task_id} to chat {chat_id} (chunk {i//chunk_size+1})")
            except Exception as e:
                logger.error(f"Failed to send expiration report for task {task_id}: {e}")
    else:
        text = "✅ Все выполнили задание"
        if link_msg:
            text += f" ({link_msg})"
        try:
            bot.send_message(chat_id, text)
            logger.info(f"Sent completion report for task {task_id} to chat {chat_id}")
        except Exception as e:
            logger.error(f"Failed to send completion report for task {task_id}: {e}")

# --- Обработчик команды /my_tasks ---
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
        try:
            member = bot.get_chat_member(chat_id, user_id)
            if member.status in ["left", "kicked"]:
                continue
            if member.status in ["administrator", "creator"]:
                continue
            filtered.append(task)
        except Exception as e:
            logger.warning(f"Failed to get member for {user_id} in {chat_id}: {e}")
            continue

    if not filtered:
        bot.send_message(user_id, "✅ У вас нет активных невыполненных заданий.")
        return

    tasks_by_chat = defaultdict(list)
    for chat_id, task_id, link, activity, author_name, msg_id in filtered:
        tasks_by_chat[chat_id].append((task_id, link, activity, author_name, msg_id))

    response = "📋 *Ваши активные задания:*\n\n"
    for chat_id, tasks in tasks_by_chat.items():
        try:
            chat_info = bot.get_chat(chat_id)
            chat_title = chat_info.title if chat_info else f"Чат {chat_id}"
        except:
            chat_title = f"Чат {chat_id}"
        response += f"*{chat_title}*:\n"
        for task_id, link, activity, author_name, msg_id in tasks:
            msg_link = task_link(chat_id, msg_id) or link
            response += f"• [Задание]({msg_link})\n"
        response += "\n"
    response += "Нажмите на ссылку, чтобы перейти к заданию, затем выполните его и нажмите кнопку ✅ Актив выполнен."

    try:
        bot.send_message(user_id, response, parse_mode='Markdown', disable_web_page_preview=True)
    except Exception as e:
        logger.warning(f"Markdown failed for /my_tasks: {e}")
        bot.send_message(user_id, response.replace('*', ''), disable_web_page_preview=True)

# --- Обработчик кнопок ---
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

    bot.answer_callback_query(call.id, "Задание выполнено!")

# --- Обработчик всех остальных сообщений ---
@bot.message_handler(func=lambda m: True)
def handle_message(message):
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
            except Exception as e:
                logger.warning(f"Failed to delete message in chat {chat_id}: {e}")
        return

    match = re.search(link_pattern, message.text)
    if not match:
        if not is_user_admin:
            try:
                bot.delete_message(chat_id, message.message_id)
            except Exception as e:
                logger.warning(f"Failed to delete message in chat {chat_id}: {e}")
        return

    link = match.group()
    activity = message.text.replace(link, "").strip() or "лайк"

    if not is_user_admin:
        with db_lock:
            cursor.execute(
                "SELECT COUNT(*) FROM tasks WHERE author=? AND chat_id=? AND created>?",
                (user_id, chat_id, now - USER_TASK_LIMIT_PERIOD)
            )
            if cursor.fetchone()[0] >= MAX_TASKS_PER_USER:
                bot.send_message(chat_id, f"❗ @{username}, лимит {MAX_TASKS_PER_USER} задания за {USER_TASK_LIMIT_PERIOD//3600} часов исчерпан. Задание не создано.")
                try:
                    bot.delete_message(chat_id, message.message_id)
                except Exception as e:
                    logger.warning(f"Failed to delete message in chat {chat_id}: {e}")
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
        except Exception as e:
            logger.warning(f"Failed to delete message in chat {chat_id}: {e}")

# --- Планировщик ---
def scheduler():
    logger.info("Scheduler started")
    friday_notified_week = None
    monday_notified_week = None
    weekly_reported_week = None
    tick = 0
    while True:
        now = int(time.time())
        now_dt = msk_now()
        day = now_dt.weekday()
        hour = now_dt.hour
        week_num = now_dt.isocalendar()[1]
        tick += 1
        if tick % 10 == 0:   # раз в 10 минут пишем в лог
            logger.info(f"Scheduler tick #{tick}, day={day}, hour={hour}")

        with db_lock:
            cursor.execute("SELECT DISTINCT chat_id FROM users")
            chats = {r[0] for r in cursor.fetchall()}
            cursor.execute("SELECT DISTINCT chat_id FROM tasks")
            chats |= {r[0] for r in cursor.fetchall()}

        for chat_id in chats:
            # Пятничное напоминание (23:00)
            if day == 4 and hour == 23 and friday_notified_week != week_num:
                try:
                    bot.send_message(chat_id, "🌙 Пост-чат ушел на выходные! Актив по желанию")
                    logger.info(f"Sent friday message to chat {chat_id}")
                except Exception as e:
                    logger.error(f"Failed to send friday message in {chat_id}: {e}")
                friday_notified_week = week_num

            # Понедельничное приветствие (7:00)
            if day == 0 and hour == 7 and monday_notified_week != week_num:
                try:
                    bot.send_message(chat_id, "☀️ Доброе утро, пост-чат работает в нормальном режиме")
                    logger.info(f"Sent monday message to chat {chat_id}")
                except Exception as e:
                    logger.error(f"Failed to send monday message in {chat_id}: {e}")
                monday_notified_week = week_num

            # Обработка истекших заданий (24 часа)
            with db_lock:
                cursor.execute("SELECT id, created, author, author_name, message_id, link FROM tasks WHERE chat_id=?", (chat_id,))
                tasks = cursor.fetchall()

            for task in tasks:
                task_id, created, author_id, author_name, msg_id, link = task
                if now - created > TASK_LIFETIME:
                    logger.info(f"Task {task_id} expired in chat {chat_id}")
                    process_expired_task(task_id, chat_id, author_id, author_name, msg_id, link)
                    with db_lock:
                        cursor.execute("DELETE FROM tasks WHERE id=?", (task_id,))
                        conn.commit()

            # Недельный отчёт (суббота, 12:00)
            if day == 5 and hour == 12 and weekly_reported_week != week_num:
                week_ago = now - 604800
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
                    if len(inactive) > 50:
                        text += "❌ Неактивные (список большой, показываю первых 50):\n" + "\n".join(inactive[:50])
                    else:
                        text += "❌ Неактивные:\n" + "\n".join(inactive)
                    text += "\n\n"
                else:
                    text += "✅ Все активны!\n\n"
                if top:
                    text += "🏆 **Топ по выполнениям:**\n"
                    for username, count in top:
                        text += f"@{username} — {count}\n"

                try:
                    bot.send_message(chat_id, text, parse_mode="Markdown")
                    logger.info(f"Sent weekly report to chat {chat_id}")
                except Exception as e:
                    logger.warning(f"Markdown failed for weekly report in chat {chat_id}: {e}")
                    try:
                        bot.send_message(chat_id, text.replace('*', ''))
                    except Exception as e2:
                        logger.error(f"Failed to send weekly report in chat {chat_id}: {e2}")
                weekly_reported_week = week_num

        time.sleep(60)

# --- Health-сервер ---
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

if __name__ == "__main__":
    reset_telegram_webhook(TOKEN)
    time.sleep(5)
    logger.info("Бот запущен...")
    while True:
        try:
            bot.infinity_polling(timeout=30, long_polling_timeout=30, skip_pending=True)
        except telebot.apihelper.ApiTelegramException as e:
            logger.error(f"Polling error: {e}")
            if "409" in str(e):
                logger.warning("Conflict detected (409). Resetting webhook and retrying...")
                reset_telegram_webhook(TOKEN)
                time.sleep(10)
            else:
                time.sleep(30)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            time.sleep(30)
