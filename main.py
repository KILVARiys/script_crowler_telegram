from telethon import TelegramClient, events
from telethon.tl.functions.messages import GetHistoryRequest
import sqlite3
from datetime import datetime
import os

# Конфигурация Telegram API
API_ID = 'your API_ID'
API_HASH = 'your API_HASH'
PHONE_NUMBER = 'your NUMBER'

# Путь к файлу базы данных SQLite
DB_PATH = 'telegram_data.db'


# Создание таблицы в SQLite
def create_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS telegram_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        chat_title TEXT,
        message_id INTEGER,
        message_text TEXT,
        sender_id INTEGER,
        sender_name TEXT,
        date TEXT,
        media_link TEXT,
        UNIQUE(chat_id, message_id)  -- Уникальность сообщений
    )
    """)
    conn.commit()
    conn.close()


create_table()

# Подключение к Telegram
client = TelegramClient('bot_session', API_ID, API_HASH)


# Функция для сохранения данных в SQLite
def save_message(chat_id, chat_title, message_id, message_text, sender_id, sender_name, date, media_link):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
        INSERT OR IGNORE INTO telegram_data (chat_id, chat_title, message_id, message_text, sender_id, sender_name, date, media_link)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (chat_id, chat_title, message_id, message_text, sender_id, sender_name, date, media_link))
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        print(f"Error saving message: {e}")


# Функция для обработки медиафайлов
async def download_media(message):
    if message.media:
        folder = "media"
        os.makedirs(folder, exist_ok=True)
        file_path = await message.download_media(file=folder)
        return file_path
    return None


# Функция для сбора истории сообщений
async def fetch_chat_history(chat):
    history = await client(GetHistoryRequest(
        peer=chat,
        limit=100,  # Укажите лимит сообщений за раз
        offset_date=None,
        offset_id=0,
        max_id=0,
        min_id=0,
        add_offset=0,
        hash=0
    ))
    return history.messages


# Функция для обработки сообщений
async def process_messages(messages, chat):
    chat_id = chat.id

    # Определяем название чата
    if hasattr(chat, 'title'):  # Группы и каналы
        chat_title = chat.title
    elif hasattr(chat, 'username'):  # Личные чаты
        chat_title = chat.username or f"User {chat_id}"
    elif hasattr(chat, 'first_name') or hasattr(chat, 'last_name'):  # Пользователи
        chat_title = f"{chat.first_name or ''} {chat.last_name or ''}".strip()
    else:
        chat_title = "Unknown Chat"

    for message in messages:
        sender = await message.get_sender()
        sender_id = sender.id if sender else None
        sender_name = f"{sender.first_name or ''} {sender.last_name or ''}".strip() if sender else "Unknown"
        message_id = message.id
        message_text = message.message or ''
        date = message.date.isoformat()
        media_link = await download_media(message)

        save_message(chat_id, chat_title, message_id, message_text, sender_id, sender_name, date, media_link)


# Обработчик новых сообщений
@client.on(events.NewMessage)
async def handler(event):
    chat = await event.get_chat()
    await process_messages([event.message], chat)


# Основной процесс
async def main():
    await client.start(phone=PHONE_NUMBER)
    print("Fetching chats and history...")

    # Получение всех чатов
    dialogs = await client.get_dialogs()

    for dialog in dialogs:
        chat = dialog.entity
        print(f"Processing chat: {chat.title if hasattr(chat, 'title') else 'Private Chat'}")
        messages = await fetch_chat_history(chat)
        await process_messages(messages, chat)

    print("Bot is now listening for new messages...")
    await client.run_until_disconnected()


# Запуск скрипта
if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main())
