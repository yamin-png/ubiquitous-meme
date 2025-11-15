import os
import json
import asyncio
import logging
import re
import configparser
import sys
import threading
import time
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError

# --- Configuration and Setup ---

CONFIG_FILE = 'config.txt'
USERS_FILE = 'users.json'
json_lock = threading.Lock()

logging.basicConfig(filename='broadcast.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def load_config():
    """Load configuration from config.txt."""
    if not os.path.exists(CONFIG_FILE):
        logging.critical(f"{CONFIG_FILE} not found!")
        raise FileNotFoundError(f"{CONFIG_FILE} not found!")
        
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE, encoding='utf-8')
    return config['Settings']

def load_json_data(filepath, default_data):
    with json_lock:
        if not os.path.exists(filepath):
            return default_data
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return default_data

def html_escape(text):
    return str(text).replace('<', '&lt;').replace('>', '&gt;')

# --- Main Broadcast Logic ---

async def main_broadcast(message_text: str):
    """
    Connects to Telegram, loads users, and sends the broadcast message.
    """
    try:
        config = load_config()
        TOKEN = config.get('TELEGRAM_BOT_TOKEN', 'YOUR_BOT_TOKEN_HERE') # Fallback
        if TOKEN == 'YOUR_BOT_TOKEN_HERE':
             print("Error: TELEGRAM_BOT_TOKEN not found in config.txt")
             logging.error("TELEGRAM_BOT_TOKEN not found in config.txt")
             return
    except Exception as e:
        print(f"Error loading config: {e}")
        logging.error(f"Error loading config: {e}")
        return

    bot = Bot(token=TOKEN)
    
    # Format the message
    formatted_message = f"<blockquote>{html_escape(message_text)}</blockquote>"
    
    users_data = load_json_data(USERS_FILE, {})
    user_ids = list(users_data.keys())
    
    if not user_ids:
        print("No users found in users.json.")
        logging.warning("No users found in users.json.")
        return

    print(f"📢 Starting to broadcast to {len(user_ids)} users...")
    logging.info(f"Starting broadcast to {len(user_ids)} users.")
    
    success_count = 0
    fail_count = 0
    
    start_time = time.time()

    for user_id in user_ids:
        try:
            await bot.send_message(chat_id=user_id, text=formatted_message, parse_mode=ParseMode.HTML)
            success_count += 1
            print(f"Sent to {user_id} ({success_count}/{len(user_ids)})")
            await asyncio.sleep(0.1) # Avoid hitting rate limits
        except TelegramError as e:
            fail_count += 1
            logging.error(f"Failed to send broadcast to {user_id}: {e}")
            print(f"Failed for {user_id}: {e}")
            if e.message == "Forbidden: bot was blocked by the user":
                logging.warning(f"User {user_id} blocked the bot. Consider removing them.")
        except Exception as e:
            fail_count += 1
            logging.error(f"Unexpected error for {user_id}: {e}")
            print(f"Unexpected error for {user_id}: {e}")
            
    end_time = time.time()
    duration = end_time - start_time
    
    summary = (
        f"\n--- Broadcast Complete! ---\n"
        f"✅ Sent successfully to {success_count} users.\n"
        f"❌ Failed to send to {fail_count} users.\n"
        f"Total users: {len(user_ids)}\n"
        f"Duration: {duration:.2f} seconds\n"
    )
    
    print(summary)
    logging.info(summary)

if __name__ == "__main__":
    # Get message from command line arguments
    if len(sys.argv) < 2:
        print("Usage: python broadcast_tool.py [Your message here]")
        sys.exit(1)
        
    message = " ".join(sys.argv[1:])
    
    try:
        asyncio.run(main_broadcast(message))
    except KeyboardInterrupt:
        print("\nBroadcast interrupted by user.")
        logging.info("Broadcast interrupted by user.")