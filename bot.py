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
import csv
import io

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

TASK_LIFETIME = 24 * 3600
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
    return not is_weekend_period(post_time)

def task_link(chat_id, message_id):
    if message_id and chat_id < 0:
        cid = str(abs(chat_id))[3:]
        return f"https://t.me/c/{cid}/{message_id}"
    return None

def get_task_keyboard(task_id, original_link):
    markup = telebot.types.InlineKeyboardMarkup(row_width=1)
    # Кнопка-ссылка (открывает оригинальный пост)
    markup.add(telebot.types.InlineKeyboardButton("🔗 Открыть задание", url=original_link))
    # Кнопка подтверждения
    markup.add(telebot.types.InlineKeyboardButton("✅ Я выполнил", callback_data=f"done_{task_id}"))
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

def is_admin(chat_id, user_id):
    admin_ids = os.environ.get("ADMIN_IDS", "")
    if admin_ids:
        admin_list = [int(x.strip()) for x in admin_ids.split(",") if x.strip().isdigit()]
        if user_id in admin_list:
            return True
    try:
        member = bot.get_chat_member(chat_id, user_id)
        return member.status in ["administrator", "creator"]
    except Exception as e:
        logger.warning(f"Failed to check admin status for {user_id} in {chat_id}: {e}")
        return False

def get_non_completers(chat_id, task_id, author_id, author_name):
    with db_lock:
        cursor.execute("SELECT user_id FROM completions WHERE task_id=? AND chat_id=?", (task_id, chat_id))
        done_user_ids = {row[0] for row in cursor.fetchall()}
        cursor.execute("SELECT id, username FROM users WHERE chat_id=?", (chat_id,))
        users_in_db = {row[0]: row[1] for row in cursor.fetchall()}

    try:
        members = bot.get_chat_members(chat_id, limit=200)
        real_member_ids = {member.user.id for member in members if not member.user.is_bot}
        all_user_ids = set(users_in_db.keys()) | real_member_ids
    except Exception as e:
        logger.warning(f"Failed to get chat members for {chat_id}, fallback to users table: {e}")
        all_user_ids = set(users_in_db.keys())

    all_user_ids.discard(author_id)

    admin_ids = set()
    for uid in list(all_user_ids):
        if is_admin(chat_id, uid):
            admin_ids.add(uid)
    all_user_ids -= admin_ids

    not_done_ids = all_user_ids - done_user_ids

    not_done_list = []
    for uid in not_done_ids:
        uname = users_in_db.get(uid)
        if not uname:
            try:
                user = bot.get_chat_member(chat_id, uid).user
                uname = user.username if user.username else f"id{uid}"
            except:
                uname = f"id{uid}"
        else:
            uname = f"@{uname}" if uname else f"id{uid}"
        not_done_list.append(uname)

    return ", ".join(not_done_list) if not_done_list else "Все выполнили"

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
        if uname == author_name or is_admin(chat_id, uid):
            continue
        if uname in done_users:
            continue
        try:
            member = bot.get_chat_member(chat_id, uid)
            if member.status in ["member", "administrator", "creator"]:
                mention = f"@{uname}" if uname else f"id{uid}"
                not_done.append(mention)
        except:
            continue

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
@bot.message_handler(commands=['start'])
def start_cmd(message):
    bot.send_message(message.chat.id, "Привет! Я бот взаимной активности. Просто нажимайте кнопки под заданиями.")

@bot.message_handler(commands=['stats'])
def stats(message):
    if message.chat.type != "private":
        return
    if not is_admin(None, message.from_user.id):
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
        try:
            member = bot.get_chat_member(chat_id, user_id)
            if member.status in ["left", "kicked"]:
                continue
            if member.status in ["administrator", "creator"]:
                continue
            filtered.append(task)
        except:
            continue
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
    response += "Откройте задание по ссылке, выполните актив и нажмите кнопку «Я выполнил» под сообщением."
    try:
        bot.send_message(user_id, response, parse_mode='Markdown', disable_web_page_preview=True)
    except Exception as e:
        bot.send_message(user_id, response.replace('*', ''), disable_web_page_preview=True)

@bot.message_handler(commands=['howto'])
def howto(message):
    user_id = message.from_user.id
    instruction = (
        "📖 *Инструкция по выполнению заданий*\n\n"
        "1️⃣ Бот публикует задание с двумя кнопками:\n"
        "   • «🔗 Открыть задание» – ведёт на пост автора.\n"
        "   • «✅ Я выполнил» – подтверждение выполнения.\n\n"
        "2️⃣ Перейдите по ссылке, выполните актив (лайк, репост, подписка).\n\n"
        "3️⃣ Вернитесь в чат и нажмите «✅ Я выполнил» – задание будет засчитано.\n\n"
        "⚠️ Нажимайте кнопку подтверждения *только после реального выполнения*.\n\n"
        "📌 Если вы случайно нажали подтверждение – ничего страшного, задание зачтётся один раз."
    )
    try:
        bot.send_message(user_id, instruction, parse_mode='Markdown')
        if message.chat.type != "private":
            bot.send_message(message.chat.id, "📩 Инструкция отправлена в личные сообщения.")
    except Exception as e:
        logger.error(f"Failed to send howto to {user_id}: {e}")
        if message.chat.type != "private":
            bot.send_message(message.chat.id, "❌ Не удалось отправить инструкцию в ЛС. Напишите боту /start и попробуйте снова.")

@bot.message_handler(commands=['broadcast'])
def broadcast(message):
    if message.chat.type != "private":
        return
    if not is_admin(None, message.from_user.id):
        bot.send_message(message.chat.id, "У вас нет прав.")
        return
    text = message.text.replace('/broadcast', '', 1).strip()
    if not text:
        bot.send_message(message.chat.id, "Укажите текст для рассылки. Пример: /broadcast Внимание, новое правило!")
        return
    with db_lock:
        cursor.execute("SELECT DISTINCT chat_id FROM users")
        chats = [row[0] for row in cursor.fetchall()]
    sent = 0
    for chat_id in chats:
        try:
            bot.send_message(chat_id, text)
            sent += 1
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"Failed to broadcast to {chat_id}: {e}")
    bot.send_message(message.chat.id, f"📢 Сообщение отправлено в {sent} чатов.")

@bot.message_handler(commands=['export_csv'])
def export_csv(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    is_private = message.chat.type == "private"

    if is_private:
        with db_lock:
            cursor.execute("SELECT DISTINCT chat_id FROM users WHERE id=?", (user_id,))
            all_chats = [row[0] for row in cursor.fetchall()]
        admin_chats = [cid for cid in all_chats if is_admin(cid, user_id)]
        if not admin_chats:
            bot.send_message(user_id, "❌ Вы не администратор ни в одном из чатов, где есть бот.")
            return
        target_chats = admin_chats
    else:
        if not is_admin(chat_id, user_id):
            bot.send_message(chat_id, "❌ Команда доступна только администраторам чата.")
            return
        target_chats = [chat_id]

    bot.send_message(user_id if is_private else chat_id, "📤 Формирую CSV-файл...")

    output = io.StringIO()
    writer = csv.writer(output, delimiter=';')
    writer.writerow(["Название чата", "Ссылка на задание (бот)", "Ссылка на пост автора", "Кто не выполнил"])

    now = int(time.time())
    total_rows = 0

    for cid in target_chats:
        try:
            chat_info = bot.get_chat(cid)
            chat_title = chat_info.title if chat_info else str(cid)
        except:
            chat_title = str(cid)

        with db_lock:
            cursor.execute("""
                SELECT id, author, author_name, link, message_id
                FROM tasks
                WHERE chat_id = ? AND created > ?
                ORDER BY created DESC
            """, (cid, now - TASK_LIFETIME))
            tasks = cursor.fetchall()

        if not tasks:
            writer.writerow([chat_title, "Нет активных заданий", "", ""])
            total_rows += 1
            continue

        for task in tasks:
            task_id, author_id, author_name, original_link, msg_id = task
            bot_link = task_link(cid, msg_id) or (f"https://t.me/c/{str(cid)[4:]}/{msg_id}" if msg_id else "Недоступно")
            non_completers = get_non_completers(cid, task_id, author_id, author_name)
            writer.writerow([chat_title, bot_link, original_link, non_completers])
            total_rows += 1

    output.seek(0)
    try:
        bot.send_document(
            user_id if is_private else chat_id,
            ('tasks_export.csv', output.getvalue().encode('utf-8-sig'))
        )
        bot.send_message(user_id if is_private else chat_id, f"✅ Выгрузка завершена. Записано {total_rows} строк.")
    except Exception as e:
        logger.error(f"Failed to send CSV: {e}")
        bot.send_message(user_id if is_private else chat_id, "❌ Ошибка при отправке файла.")

# ==================== ОСНОВНОЙ ОБРАБОТЧИК КНОПКИ (подтверждение) ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("done_"))
def handle_done(call):
    task_id = int(call.data.split("_")[1])
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    now = int(time.time())

    with db_lock:
        cursor.execute("SELECT author, created FROM tasks WHERE id=?", (task_id,))
        row = cursor.fetchone()
        if not row:
            bot.answer_callback_query(call.id, "❌ Задание устарело или было удалено.")
            return
        author_id, created = row

        if user_id == author_id:
            bot.answer_callback_query(call.id, "❌ Нельзя выполнить своё задание")
            return
        if now - created < 10:
            bot.answer_callback_query(call.id, "⏳ Подождите 10 секунд после создания задания")
            return

        # Проверяем, не выполнено ли уже
        cursor.execute("SELECT * FROM completions WHERE task_id=? AND user_id=? AND chat_id=?", (task_id, user_id, chat_id))
        if cursor.fetchone():
            bot.answer_callback_query(call.id, "ℹ️ Вы уже выполнили это задание")
            return

        # Засчитываем выполнение
        cursor.execute(
            "INSERT INTO completions (task_id, chat_id, user_id, username, time, verified) VALUES (?, ?, ?, ?, ?, 1)",
            (task_id, chat_id, user_id, call.from_user.username, now)
        )
        cursor.execute(
            "INSERT OR REPLACE INTO users (id, chat_id, username, last_active) VALUES (?, ?, ?, ?)",
            (user_id, chat_id, call.from_user.username, now)
        )
        conn.commit()

    bot.answer_callback_query(call.id, "✅ Задание выполнено! Спасибо за активность.")

# ==================== ОСНОВНОЙ ОБРАБОТЧИК СООБЩЕНИЙ ====================
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
        return

    match = re.search(link_pattern, message.text)
    if not match:
        if not is_user_admin:
            try:
                bot.delete_message(chat_id, message.message_id)
            except Exception as e:
                logger.warning(f"Failed to delete message: {e}")
        return

    link = match.group()
    activity = message.text.replace(link, "").strip() or "лайк"

    if not is_user_admin:
        with db_lock:
            cursor.execute("SELECT COUNT(*) FROM tasks WHERE author=? AND chat_id=? AND created>?", (user_id, chat_id, now - USER_TASK_LIMIT_PERIOD))
            if cursor.fetchone()[0] >= MAX_TASKS_PER_USER:
                bot.send_message(chat_id, f"❗ @{username}, лимит {MAX_TASKS_PER_USER} задания за {USER_TASK_LIMIT_PERIOD//3600} часов исчерпан.")
                try:
                    bot.delete_message(chat_id, message.message_id)
                except Exception as e:
                    logger.warning(f"Failed to delete message: {e}")
                return

    with db_lock:
        cursor.execute(
            "INSERT INTO tasks (chat_id, author, author_name, link, activity, created) VALUES (?, ?, ?, ?, ?, ?)",
            (chat_id, user_id, username, link, activity, now)
        )
        task_id = cursor.lastrowid
        conn.commit()

    # Отправляем сообщение с двумя кнопками
    sent = bot.send_message(
        chat_id,
        f"📢 Новое задание\n\n@{username}\n{activity}\n\n⬇️ Перейдите по ссылке, выполните актив и нажмите подтверждение.",
        reply_markup=get_task_keyboard(task_id, link)
    )
    with db_lock:
        cursor.execute("UPDATE tasks SET message_id=? WHERE id=?", (sent.message_id, task_id))
        conn.commit()

    if not is_user_admin:
        try:
            bot.delete_message(chat_id, message.message_id)
        except Exception as e:
            logger.warning(f"Failed to delete message: {e}")

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

def send_update_notification():
    with db_lock:
        cursor.execute("SELECT DISTINCT chat_id FROM users")
        chats = [row[0] for row in cursor.fetchall()]
    for chat_id in chats:
        key = f"update_notified_{chat_id}"
        if get_state(key) == "1":
            continue
        try:
            bot.send_message(
                chat_id,
                "🔄 *Внимание! Бот обновлён!*\n\n"
                "Теперь задания публикуются с двумя кнопками:\n"
                "• «🔗 Открыть задание» – ссылка на пост автора.\n"
                "• «✅ Я выполнил» – подтверждение выполнения.\n\n"
                "Порядок действий:\n"
                "1. Перейдите по ссылке, выполните актив.\n"
                "2. Вернитесь в чат и нажмите «✅ Я выполнил».\n\n"
                "Инструкция: /howto\n"
                "Спасибо за понимание!",
                parse_mode='Markdown'
            )
            set_state(key, "1")
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"Failed to send update notification to {chat_id}: {e}")

def scheduler():
    logger.info("Scheduler started")
    while True:
        now = int(time.time())
        now_dt = msk_now()
        day = now_dt.weekday()
        hour = now_dt.hour
        week_num = now_dt.isocalendar()[1]

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

        if is_work_time(now):
            with db_lock:
                cursor.execute("SELECT id, chat_id, author, author_name, message_id, link, created FROM tasks WHERE created <= ?", (now - TASK_LIFETIME,))
                expired = cursor.fetchall()
            for task in expired:
                task_id, chat_id, author_id, author_name, msg_id, link, created = task
                process_expired_task(task_id, chat_id, author_id, author_name, msg_id, link)
                with db_lock:
                    cursor.execute("DELETE FROM tasks WHERE id=?", (task_id,))
                    conn.commit()

        if day == 6 and hour == 12:
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

# ==================== ЗАПУСК ====================
if __name__ == "__main__":
    reset_telegram_webhook(TOKEN)
    time.sleep(5)
    threading.Thread(target=send_update_notification, daemon=True).start()
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
