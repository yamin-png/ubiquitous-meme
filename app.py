import os
import json
import time
import asyncio
import threading
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup, error
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode, ChatMemberStatus
# GUI libraries removed for terminal-based operation
import logging
import re


def normalize_number(num: str) -> str:
    if not num:
        return ''
    return re.sub(r'\D', '', str(num)).lstrip('0')


def format_number_variants(phone: str) -> dict:
    if not phone:
        return {'international': '', 'plain': ''}
    raw = str(phone).strip()
    digits = re.sub(r'\D', '', raw)

    international = raw
    plain = digits
    if digits.startswith('00'):
        digits = digits.lstrip('0')
    if raw.startswith('+'):
        international = raw
    else:
        if len(digits) > 9:
            local = digits[-9:]
            cc = digits[:-9]
            international = f"+{cc}{local}" if cc else f"+{digits}"
            plain = local
        else:
            international = f"+{digits}"
            plain = digits

    return {'international': international, 'plain': plain}

    
# --- UPDATED CONFIGURATION ---
TELEGRAM_BOT_TOKEN = "8522208519:AAHwD_dI5pUY6lI8HYDmRKoaXStBuDVIapQ"
GROUP_ID = -1001367182443
CHANNEL_ID = -1001688406759
ADMIN_ID = 1319659809 
UPDATE_CHANNEL_LINK = "https://t.me/painite_1"
GROUP_LINK = "https://t.me/painite_club" # Fixed group link typo
EMAIL = "M.JAHIDHASSAN.K1@GMAIL.COM"
PASSWORD = "Alve2583Alve"
ADMIN_USERNAME = "JAHID_1" 
PHPSESSID = "ae3771mtkp2a0dcvel03v9jrtu"

# File Paths
USERS_FILE = 'users.json'
SMS_CACHE_FILE = 'sms.txt'
SENT_SMS_FILE = 'sent_sms.json'
NUMBERS_FILE = 'numbers.txt'

# --- Comprehensive Country Codes and Flags ---
def detect_country_from_phone(phone):
    """Detect country from phone number prefix"""
    if not phone:
        return "Unknown", "🌍"
    
    phone_str = str(phone).replace("+", "").replace(" ", "").replace("-", "")
    
    # Country code mappings (most common prefixes)
    country_codes = {
        "1": ("United States", "🇺🇸"),
        "7": ("Russia", "🇷🇺"),
        "20": ("Egypt", "🇪🇬"),
        "27": ("South Africa", "🇿🇦"),
        "30": ("Greece", "🇬🇷"),
        "31": ("Netherlands", "🇳🇱"),
        "32": ("Belgium", "🇧🇪"),
        "33": ("France", "🇫🇷"),
        "34": ("Spain", "🇪🇸"),
        "36": ("Hungary", "🇭🇺"),
        "39": ("Italy", "🇮🇹"),
        "40": ("Romania", "🇷🇴"),
        "41": ("Switzerland", "🇨🇭"),
        "43": ("Austria", "🇦🇹"),
        "44": ("United Kingdom", "🇬🇧"),
        "45": ("Denmark", "🇩🇰"),
        "46": ("Sweden", "🇸🇪"),
        "47": ("Norway", "🇳🇴"),
        "48": ("Poland", "🇵🇱"),
        "49": ("Germany", "🇩🇪"),
        "51": ("Peru", "🇵🇪"),
        "52": ("Mexico", "🇲🇽"),
        "53": ("Cuba", "🇨🇺"),
        "54": ("Argentina", "🇦🇷"),
        "55": ("Brazil", "🇧🇷"),
        "56": ("Chile", "🇨🇱"),
        "57": ("Colombia", "🇨🇴"),
        "58": ("Venezuela", "🇻🇪"),
        "60": ("Malaysia", "🇲🇾"),
        "61": ("Australia", "🇦🇺"),
        "62": ("Indonesia", "🇮🇩"),
        "63": ("Philippines", "🇵🇭"),
        "64": ("New Zealand", "🇳🇿"),
        "65": ("Singapore", "🇸🇬"),
        "66": ("Thailand", "🇹🇭"),
        "81": ("Japan", "🇯🇵"),
        "82": ("South Korea", "🇰🇷"),
        "84": ("Vietnam", "🇻🇳"),
        "86": ("China", "🇨🇳"),
        "90": ("Turkey", "🇹🇷"),
        "91": ("India", "🇮🇳"),
        "92": ("Pakistan", "🇵🇰"),
        "93": ("Afghanistan", "🇦🇫"),
        "94": ("Sri Lanka", "🇱🇰"),
        "95": ("Myanmar", "🇲🇲"),
        "98": ("Iran", "🇮🇷"),
        "212": ("Morocco", "🇲🇦"),
        "213": ("Algeria", "🇩🇿"),
        "216": ("Tunisia", "🇹🇳"),
        "218": ("Libya", "🇱🇾"),
        "220": ("Gambia", "🇬🇲"),
        "221": ("Senegal", "🇸🇳"),
        "222": ("Mauritania", "🇲🇷"),
        "223": ("Mali", "🇲🇱"),
        "224": ("Guinea", "🇬🇳"),
        "225": ("Ivory Coast", "🇨🇮"),
        "226": ("Burkina Faso", "🇧🇫"),
        "227": ("Niger", "🇳🇪"),
        "228": ("Togo", "🇹🇬"),
        "229": ("Benin", "🇧🇯"),
        "230": ("Mauritius", "🇲🇺"),
        "231": ("Liberia", "🇱🇷"),
        "232": ("Sierra Leone", "🇸🇱"),
        "233": ("Ghana", "🇬🇭"),
        "234": ("Nigeria", "🇳🇬"),
        "235": ("Chad", "🇹🇩"),
        "236": ("Central African Republic", "🇨🇫"),
        "237": ("Cameroon", "🇨🇲"),
        "238": ("Cape Verde", "🇨🇻"),
        "239": ("Sao Tome and Principe", "🇸🇹"),
        "240": ("Equatorial Guinea", "🇬🇶"),
        "241": ("Gabon", "🇬🇦"),
        "242": ("Congo", "🇨🇬"),
        "243": ("Congo", "🇨🇩"),
        "244": ("Angola", "🇦🇴"),
        "245": ("Guinea-Bissau", "🇬🇼"),
        "246": ("British Indian Ocean Territory", "🇮🇴"),
        "248": ("Seychelles", "🇸🇨"),
        "249": ("Sudan", "🇸🇩"),
        "250": ("Rwanda", "🇷🇼"),
        "251": ("Ethiopia", "🇪🇹"),
        "252": ("Somalia", "🇸🇴"),
        "253": ("Djibouti", "🇩🇯"),
        "254": ("Kenya", "🇰🇪"),
        "255": ("Tanzania", "🇹🇿"),
        "256": ("Uganda", "🇺🇬"),
        "257": ("Burundi", "🇧🇮"),
        "258": ("Mozambique", "🇲🇿"),
        "260": ("Zambia", "🇿🇲"),
        "261": ("Madagascar", "🇲🇬"),
        "262": ("Reunion", "🇷🇪"),
        "263": ("Zimbabwe", "🇿🇼"),
        "264": ("Namibia", "🇳🇦"),
        "265": ("Malawi", "🇲🇼"),
        "266": ("Lesotho", "🇱🇸"),
        "267": ("Botswana", "🇧🇼"),
        "268": ("Eswatini", "🇸🇿"),
        "269": ("Comoros", "🇰🇲"),
        "290": ("Saint Helena", "🇸🇭"),
        "291": ("Eritrea", "🇪🇷"),
        "297": ("Aruba", "🇦🇼"),
        "298": ("Faroe Islands", "🇫🇴"),
        "299": ("Greenland", "🇬🇱"),
        "350": ("Gibraltar", "🇬🇮"),
        "351": ("Portugal", "🇵🇹"),
        "352": ("Luxembourg", "🇱🇺"),
        "353": ("Ireland", "🇮🇪"),
        "354": ("Iceland", "🇮🇸"),
        "355": ("Albania", "🇦🇱"),
        "356": ("Malta", "🇲🇹"),
        "357": ("Cyprus", "🇨🇾"),
        "358": ("Finland", "🇫🇮"),
        "359": ("Bulgaria", "🇧🇬"),
        "370": ("Lithuania", "🇱🇹"),
        "371": ("Latvia", "🇱🇻"),
        "372": ("Estonia", "🇪🇪"),
        "373": ("Moldova", "🇲🇩"),
        "374": ("Armenia", "🇦🇲"),
        "375": ("Belarus", "🇧🇾"),
        "376": ("Andorra", "🇦🇩"),
        "377": ("Monaco", "🇲🇨"),
        "378": ("San Marino", "🇸🇲"),
        "380": ("Ukraine", "🇺🇦"),
        "381": ("Serbia", "🇷🇸"),
        "382": ("Montenegro", "🇲🇪"),
        "383": ("Kosovo", "🇽🇰"),
        "385": ("Croatia", "🇭🇷"),
        "386": ("Slovenia", "🇸🇮"),
        "387": ("Bosnia and Herzegovina", "🇧🇦"),
        "389": ("North Macedonia", "🇲🇰"),
        "420": ("Czech Republic", "🇨🇿"),
        "421": ("Slovakia", "🇸🇰"),
        "423": ("Liechtenstein", "🇱🇮"),
        "500": ("Falkland Islands", "🇫🇰"),
        "501": ("Belize", "🇧🇿"),
        "502": ("Guatemala", "🇬🇹"),
        "503": ("El Salvador", "🇸🇻"),
        "504": ("Honduras", "🇭🇳"),
        "505": ("Nicaragua", "🇳🇮"),
        "506": ("Costa Rica", "🇨🇷"),
        "507": ("Panama", "🇵🇦"),
        "508": ("Saint Pierre and Miquelon", "🇵🇲"),
        "509": ("Haiti", "🇭🇹"),
        "590": ("Guadeloupe", "🇬🇵"),
        "591": ("Bolivia", "🇧🇴"),
        "592": ("Guyana", "🇬🇾"),
        "593": ("Ecuador", "🇪🇨"),
        "594": ("French Guiana", "🇬🇫"),
        "595": ("Paraguay", "🇵🇾"),
        "596": ("Martinique", "🇲🇶"),
        "597": ("Suriname", "🇸🇷"),
        "598": ("Uruguay", "🇺🇾"),
        "599": ("Netherlands Antilles", "🇳🇱"),
        "670": ("Timor-Leste", "🇹🇱"),
        "672": ("Australian External Territories", "🇦🇺"),
        "673": ("Brunei", "🇧🇳"),
        "674": ("Nauru", "🇳🇷"),
        "675": ("Papua New Guinea", "🇵🇬"),
        "676": ("Tonga", "🇹🇴"),
        "677": ("Solomon Islands", "🇸🇧"),
        "678": ("Vanuatu", "🇻🇺"),
        "679": ("Fiji", "🇫🇯"),
        "680": ("Palau", "🇵🇼"),
        "681": ("Wallis and Futuna", "🇼🇫"),
        "682": ("Cook Islands", "🇨🇰"),
        "683": ("Niue", "🇳🇺"),
        "684": ("American Samoa", "🇦🇸"),
        "685": ("Samoa", "🇼🇸"),
        "686": ("Kiribati", "🇰🇮"),
        "687": ("New Caledonia", "🇳🇨"),
        "688": ("Tuvalu", "🇹🇻"),
        "689": ("French Polynesia", "🇵🇫"),
        "690": ("Tokelau", "🇹🇰"),
        "691": ("Micronesia", "🇫🇲"),
        "692": ("Marshall Islands", "🇲🇭"),
        "850": ("North Korea", "🇰🇵"),
        "852": ("Hong Kong", "🇭🇰"),
        "853": ("Macau", "🇲🇴"),
        "855": ("Cambodia", "🇰🇭"),
        "856": ("Laos", "🇱🇦"),
        "880": ("Bangladesh", "🇧🇩"),
        "886": ("Taiwan", "🇹🇼"),
        "960": ("Maldives", "🇲🇻"),
        "961": ("Lebanon", "🇱🇧"),
        "962": ("Jordan", "🇯🇴"),
        "963": ("Syria", "🇸🇾"),
        "964": ("Iraq", "🇮🇶"),
        "965": ("Kuwait", "🇰🇼"),
        "966": ("Saudi Arabia", "🇸🇦"),
        "967": ("Yemen", "🇾🇪"),
        "968": ("Oman", "🇴🇲"),
        "970": ("Palestine", "🇵🇸"),
        "971": ("United Arab Emirates", "🇦🇪"),
        "972": ("Israel", "🇮🇱"),
        "973": ("Bahrain", "🇧🇭"),
        "974": ("Qatar", "🇶🇦"),
        "975": ("Bhutan", "🇧🇹"),
        "976": ("Mongolia", "🇲🇳"),
        "977": ("Nepal", "🇳🇵"),
        "992": ("Tajikistan", "🇹🇯"),
        "993": ("Turkmenistan", "🇹🇲"),
        "994": ("Azerbaijan", "🇦🇿"),
        "995": ("Georgia", "🇬🇪"),
        "996": ("Kyrgyzstan", "🇰🇬"),
        "998": ("Uzbekistan", "🇺🇿"),
    }
    
    # Try different prefix lengths (longest first)
    for length in [3, 2, 1]:
        if len(phone_str) >= length:
            prefix = phone_str[:length]
            if prefix in country_codes:
                return country_codes[prefix]
    
    return "Unknown", "🌍"


# Global variables
shutdown_event = asyncio.Event()
bot_thread = None
manager_instance = None
json_lock = threading.Lock()

# Setup logging
logging.basicConfig(filename='bot_error.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def load_json_data(filepath, default_data):
    with json_lock:
        if not os.path.exists(filepath):
            return default_data
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return default_data

def save_json_data(filepath, data):
    with json_lock:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

def load_sent_sms_keys():
    return set(load_json_data(SENT_SMS_FILE, []))

def save_sent_sms_keys(keys):
    save_json_data(SENT_SMS_FILE, list(keys))

PANEL_BASE_URL = "http://51.89.99.105/NumberPanel"
PANEL_SMS_URL = "http://51.89.99.105/NumberPanel/agent/SMSDashboard"

def get_bst_now():
    """Returns current time, assuming UTC for simplicity as pytz is not available."""
    return datetime.now()

def extract_otp_from_text(text):
    if not text: return "N/A"
    clean = text.replace("-", "")
    match = re.findall(r"\b\d{4,8}\b", clean)
    return match[0] if match else "N/A"

def html_escape(text):
    return str(text).replace('<', '&lt;').replace('>', '&gt;')

def mask_phone_number(phone_number: str) -> str:
    if not phone_number:
        return "N/A"


    clean_number = re.sub(r'\D', '', phone_number)
    
    # Find the country code
    country_name, country_flag = detect_country_from_phone(phone_number)
    
    if country_name != "Unknown":
        phone_str = str(phone_number).replace("+", "").replace(" ", "").replace("-", "")
        country_codes = {
            "1": ("United States", "🇺🇸"),
            "7": ("Russia", "🇷🇺"),
            "20": ("Egypt", "🇪🇬"),
            "27": ("South Africa", "🇿🇦"),
            "30": ("Greece", "🇬🇷"),
            "31": ("Netherlands", "🇳🇱"),
            "32": ("Belgium", "🇧🇪"),
            "33": ("France", "🇫🇷"),
            "34": ("Spain", "🇪🇸"),
            "36": ("Hungary", "🇭🇺"),
            "39": ("Italy", "🇮🇹"),
            "40": ("Romania", "🇷🇴"),
            "41": ("Switzerland", "🇨🇭"),
            "43": ("Austria", "🇦🇹"),
            "44": ("United Kingdom", "🇬🇧"),
            "45": ("Denmark", "🇩🇰"),
            "46": ("Sweden", "🇸🇪"),
            "47": ("Norway", "🇳🇴"),
            "48": ("Poland", "🇵🇱"),
            "49": ("Germany", "🇩🇪"),
            "51": ("Peru", "🇵🇪"),
            "52": ("Mexico", "🇲🇽"),
            "53": ("Cuba", "🇨🇺"),
            "54": ("Argentina", "🇦🇷"),
            "55": ("Brazil", "🇧🇷"),
            "56": ("Chile", "🇨🇱"),
            "57": ("Colombia", "🇨🇴"),
            "58": ("Venezuela", "🇻🇪"),
            "60": ("Malaysia", "🇲🇾"),
            "61": ("Australia", "🇦🇺"),
            "62": ("Indonesia", "🇮🇩"),
            "63": ("Philippines", "🇵🇭"),
            "64": ("New Zealand", "🇳🇿"),
            "65": ("Singapore", "🇸🇬"),
            "66": ("Thailand", "🇹🇭"),
            "81": ("Japan", "🇯🇵"),
            "82": ("South Korea", "🇰🇷"),
            "84": ("Vietnam", "🇻🇳"),
            "86": ("China", "🇨🇳"),
            "90": ("Turkey", "🇹🇷"),
            "91": ("India", "🇮🇳"),
            "92": ("Pakistan", "🇵🇰"),
            "93": ("Afghanistan", "🇦🇫"),
            "94": ("Sri Lanka", "🇱🇰"),
            "95": ("Myanmar", "🇲🇲"),
            "98": ("Iran", "🇮🇷"),
            "212": ("Morocco", "🇲🇦"),
            "213": ("Algeria", "🇩🇿"),
            "216": ("Tunisia", "🇹🇳"),
            "218": ("Libya", "🇱🇾"),
            "220": ("Gambia", "🇬🇲"),
            "221": ("Senegal", "🇸🇳"),
            "222": ("Mauritania", "🇲🇷"),
            "223": ("Mali", "🇲🇱"),
            "224": ("Guinea", "🇬🇳"),
            "225": ("Ivory Coast", "🇨🇮"),
            "226": ("Burkina Faso", "🇧🇫"),
            "227": ("Niger", "🇳🇪"),
            "228": ("Togo", "🇹🇬"),
            "229": ("Benin", "🇧🇯"),
            "230": ("Mauritius", "🇲🇺"),
            "231": ("Liberia", "🇱🇷"),
            "232": ("Sierra Leone", "🇸🇱"),
            "233": ("Ghana", "🇬🇭"),
            "234": ("Nigeria", "🇳🇬"),
            "235": ("Chad", "🇹🇩"),
            "236": ("Central African Republic", "🇨🇫"),
            "237": ("Cameroon", "🇨🇲"),
            "238": ("Cape Verde", "🇨🇻"),
            "239": ("Sao Tome and Principe", "🇸🇹"),
            "240": ("Equatorial Guinea", "🇬🇶"),
            "241": ("Gabon", "🇬🇦"),
            "242": ("Congo", "🇨🇬"),
            "243": ("Congo", "🇨🇩"),
            "244": ("Angola", "🇦🇴"),
            "245": ("Guinea-Bissau", "🇬🇼"),
            "246": ("British Indian Ocean Territory", "🇮🇴"),
            "248": ("Seychelles", "🇸🇨"),
            "249": ("Sudan", "🇸🇩"),
            "250": ("Rwanda", "🇷🇼"),
            "251": ("Ethiopia", "🇪🇹"),
            "252": ("Somalia", "🇸🇴"),
            "253": ("Djibouti", "🇩🇯"),
            "254": ("Kenya", "🇰🇪"),
            "255": ("Tanzania", "🇹🇿"),
            "256": ("Uganda", "🇺🇬"),
            "257": ("Burundi", "🇧🇮"),
            "258": ("Mozambique", "🇲🇿"),
            "260": ("Zambia", "🇿🇲"),
            "261": ("Madagascar", "🇲🇬"),
            "262": ("Reunion", "🇷🇪"),
            "263": ("Zimbabwe", "🇿🇼"),
            "264": ("Namibia", "🇳🇦"),
            "265": ("Malawi", "🇲🇼"),
            "266": ("Lesotho", "🇱🇸"),
            "267": ("Botswana", "🇧🇼"),
            "268": ("Eswatini", "🇸🇿"),
            "269": ("Comoros", "🇰🇲"),
            "290": ("Saint Helena", "🇸🇭"),
            "291": ("Eritrea", "🇪🇷"),
            "297": ("Aruba", "🇦🇼"),
            "298": ("Faroe Islands", "🇫🇴"),
            "299": ("Greenland", "🇬🇱"),
            "350": ("Gibraltar", "🇬🇮"),
            "351": ("Portugal", "🇵🇹"),
            "352": ("Luxembourg", "🇱🇺"),
            "353": ("Ireland", "🇮🇪"),
            "354": ("Iceland", "🇮🇸"),
            "355": ("Albania", "🇦🇱"),
            "356": ("Malta", "🇲🇹"),
            "357": ("Cyprus", "🇨🇾"),
            "358": ("Finland", "🇫🇮"),
            "359": ("Bulgaria", "🇧🇬"),
            "370": ("Lithuania", "🇱🇹"),
            "371": ("Latvia", "🇱🇻"),
            "372": ("Estonia", "🇪🇪"),
            "373": ("Moldova", "🇲🇩"),
            "374": ("Armenia", "🇦🇲"),
            "375": ("Belarus", "🇧🇾"),
            "376": ("Andorra", "🇦🇩"),
            "377": ("Monaco", "🇲🇨"),
            "378": ("San Marino", "🇸🇲"),
            "380": ("Ukraine", "🇺🇦"),
            "381": ("Serbia", "🇷🇸"),
            "382": ("Montenegro", "🇲🇪"),
            "383": ("Kosovo", "🇽🇰"),
            "385": ("Croatia", "🇭🇷"),
            "386": ("Slovenia", "🇸🇮"),
            "387": ("Bosnia and Herzegovina", "🇧🇦"),
            "389": ("North Macedonia", "🇲🇰"),
            "420": ("Czech Republic", "🇨🇿"),
            "421": ("Slovakia", "🇸🇰"),
            "423": ("Liechtenstein", "🇱🇮"),
            "500": ("Falkland Islands", "🇫🇰"),
            "501": ("Belize", "🇧🇿"),
            "502": ("Guatemala", "🇬🇹"),
            "503": ("El Salvador", "🇸🇻"),
            "504": ("Honduras", "🇭🇳"),
            "505": ("Nicaragua", "🇳🇮"),
            "506": ("Costa Rica", "🇨🇷"),
            "507": ("Panama", "🇵🇦"),
            "508": ("Saint Pierre and Miquelon", "🇵🇲"),
            "509": ("Haiti", "🇭🇹"),
            "590": ("Guadeloupe", "🇬🇵"),
            "591": ("Bolivia", "🇧🇴"),
            "592": ("Guyana", "🇬🇾"),
            "593": ("Ecuador", "🇪🇨"),
            "594": ("French Guiana", "🇬🇫"),
            "595": ("Paraguay", "🇵🇾"),
            "596": ("Martinique", "🇲🇶"),
            "597": ("Suriname", "🇸🇷"),
            "598": ("Uruguay", "🇺🇾"),
            "599": ("Netherlands Antilles", "🇳🇱"),
            "670": ("Timor-Leste", "🇹🇱"),
            "672": ("Australian External Territories", "🇦🇺"),
            "673": ("Brunei", "🇧🇳"),
            "674": ("Nauru", "🇳🇷"),
            "675": ("Papua New Guinea", "🇵🇬"),
            "676": ("Tonga", "🇹🇴"),
            "677": ("Solomon Islands", "🇸🇧"),
            "678": ("Vanuatu", "🇻🇺"),
            "679": ("Fiji", "🇫🇯"),
            "680": ("Palau", "🇵🇼"),
            "681": ("Wallis and Futuna", "🇼🇫"),
            "682": ("Cook Islands", "🇨🇰"),
            "683": ("Niue", "🇳🇺"),
            "684": ("American Samoa", "🇦🇸"),
            "685": ("Samoa", "🇼🇸"),
            "686": ("Kiribati", "🇰🇮"),
            "687": ("New Caledonia", "🇳🇨"),
            "688": ("Tuvalu", "🇹🇻"),
            "689": ("French Polynesia", "🇵🇫"),
            "690": ("Tokelau", "🇹🇰"),
            "691": ("Micronesia", "🇫🇲"),
            "692": ("Marshall Islands", "🇲🇭"),
            "850": ("North Korea", "🇰🇵"),
            "852": ("Hong Kong", "🇭🇰"),
            "853": ("Macau", "🇲🇴"),
            "855": ("Cambodia", "🇰🇭"),
            "856": ("Laos", "🇱🇦"),
            "880": ("Bangladesh", "🇧🇩"),
            "886": ("Taiwan", "🇹🇼"),
            "960": ("Maldives", "🇲🇻"),
            "961": ("Lebanon", "🇱🇧"),
            "962": ("Jordan", "🇯🇴"),
            "963": ("Syria", "🇸🇾"),
            "964": ("Iraq", "🇮🇶"),
            "965": ("Kuwait", "🇰🇼"),
            "966": ("Saudi Arabia", "🇸🇦"),
            "967": ("Yemen", "🇾🇪"),
            "968": ("Oman", "🇴🇲"),
            "970": ("Palestine", "🇵🇸"),
            "971": ("United Arab Emirates", "🇦🇪"),
            "972": ("Israel", "🇮🇱"),
            "973": ("Bahrain", "🇧🇭"),
            "974": ("Qatar", "🇶🇦"),
            "975": ("Bhutan", "🇧🇹"),
            "976": ("Mongolia", "🇲🇳"),
            "977": ("Nepal", "🇳🇵"),
            "992": ("Tajikistan", "🇹🇯"),
            "993": ("Turkmenistan", "🇹🇲"),
            "994": ("Azerbaijan", "🇦🇿"),
            "995": ("Georgia", "🇬🇪"),
            "996": ("Kyrgyzstan", "🇰🇬"),
            "998": ("Uzbekistan", "🇺🇿"),
        }
        country_code_prefix = ""
        for length in [3, 2, 1]:
            if len(phone_str) >= length:
                prefix = phone_str[:length]
                if prefix in country_codes:
                    country_code_prefix = prefix
                    break
        
        local_number_digits = clean_number[len(country_code_prefix):]
    else:
        # If no country code found, treat the whole number as local
        local_number_digits = clean_number
        country_code_prefix = ""


    visible_start_local = 2
    visible_end_local = 5

    # If the local number is too short to apply the mask (e.g., less than 2+5+1=8 digits for masking at least one digit)
    if len(local_number_digits) < (visible_start_local + visible_end_local + 1):
        return phone_number # Return original if too short for specific mask

    start_part = local_number_digits[:visible_start_local]
    end_part = local_number_digits[-visible_end_local:]
    
    masked_length = len(local_number_digits) - visible_start_local - visible_end_local
    masked_part = 'X' * masked_length

    return f"+{country_code_prefix}{start_part}{masked_part}{end_part}"


def get_number_formats(phone_number_str):
    if not phone_number_str:
        return "N/A", "N/A"

    num = re.sub(r'\D', '', phone_number_str)

    country_name, country_flag = detect_country_from_phone(phone_number_str)

    if country_name != "Unknown":
        # This is a bit of a hack, but we need to get the country code length
        # to correctly format the number. We can do this by finding the country
        # code in the `country_codes` dictionary in `detect_country_from_phone`.
        # This is not ideal, but it's the best we can do without changing the
        # `detect_country_from_phone` function to return the country code as well.
        phone_str = str(phone_number_str).replace("+", "").replace(" ", "").replace("-", "")
        country_codes = {
            "1": ("United States", "🇺🇸"),
            "7": ("Russia", "🇷🇺"),
            "20": ("Egypt", "🇪🇬"),
            "27": ("South Africa", "🇿🇦"),
            "30": ("Greece", "🇬🇷"),
            "31": ("Netherlands", "🇳🇱"),
            "32": ("Belgium", "🇧🇪"),
            "33": ("France", "🇫🇷"),
            "34": ("Spain", "🇪🇸"),
            "36": ("Hungary", "🇭🇺"),
            "39": ("Italy", "🇮🇹"),
            "40": ("Romania", "🇷🇴"),
            "41": ("Switzerland", "🇨🇭"),
            "43": ("Austria", "🇦🇹"),
            "44": ("United Kingdom", "🇬🇧"),
            "45": ("Denmark", "🇩🇰"),
            "46": ("Sweden", "🇸🇪"),
            "47": ("Norway", "🇳🇴"),
            "48": ("Poland", "🇵🇱"),
            "49": ("Germany", "🇩🇪"),
            "51": ("Peru", "🇵🇪"),
            "52": ("Mexico", "🇲🇽"),
            "53": ("Cuba", "🇨🇺"),
            "54": ("Argentina", "🇦🇷"),
            "55": ("Brazil", "🇧🇷"),
            "56": ("Chile", "🇨🇱"),
            "57": ("Colombia", "🇨🇴"),
            "58": ("Venezuela", "🇻🇪"),
            "60": ("Malaysia", "🇲🇾"),
            "61": ("Australia", "🇦🇺"),
            "62": ("Indonesia", "🇮🇩"),
            "63": ("Philippines", "🇵🇭"),
            "64": ("New Zealand", "🇳🇿"),
            "65": ("Singapore", "🇸🇬"),
            "66": ("Thailand", "🇹🇭"),
            "81": ("Japan", "🇯🇵"),
            "82": ("South Korea", "🇰🇷"),
            "84": ("Vietnam", "🇻🇳"),
            "86": ("China", "🇨🇳"),
            "90": ("Turkey", "🇹🇷"),
            "91": ("India", "🇮🇳"),
            "92": ("Pakistan", "🇵🇰"),
            "93": ("Afghanistan", "🇦🇫"),
            "94": ("Sri Lanka", "🇱🇰"),
            "95": ("Myanmar", "🇲🇲"),
            "98": ("Iran", "🇮🇷"),
            "212": ("Morocco", "🇲🇦"),
            "213": ("Algeria", "🇩🇿"),
            "216": ("Tunisia", "🇹🇳"),
            "218": ("Libya", "🇱🇾"),
            "220": ("Gambia", "🇬🇲"),
            "221": ("Senegal", "🇸🇳"),
            "222": ("Mauritania", "🇲🇷"),
            "223": ("Mali", "🇲🇱"),
            "224": ("Guinea", "🇬🇳"),
            "225": ("Ivory Coast", "🇨🇮"),
            "226": ("Burkina Faso", "🇧🇫"),
            "227": ("Niger", "🇳🇪"),
            "228": ("Togo", "🇹🇬"),
            "229": ("Benin", "🇧🇯"),
            "230": ("Mauritius", "🇲🇺"),
            "231": ("Liberia", "🇱🇷"),
            "232": ("Sierra Leone", "🇸🇱"),
            "233": ("Ghana", "🇬🇭"),
            "234": ("Nigeria", "🇳🇬"),
            "235": ("Chad", "🇹🇩"),
            "236": ("Central African Republic", "🇨🇫"),
            "237": ("Cameroon", "🇨🇲"),
            "238": ("Cape Verde", "🇨🇻"),
            "239": ("Sao Tome and Principe", "🇸🇹"),
            "240": ("Equatorial Guinea", "🇬🇶"),
            "241": ("Gabon", "🇬🇦"),
            "242": ("Congo", "🇨🇬"),
            "243": ("Congo", "🇨🇩"),
            "244": ("Angola", "🇦🇴"),
            "245": ("Guinea-Bissau", "🇬🇼"),
            "246": ("British Indian Ocean Territory", "🇮🇴"),
            "248": ("Seychelles", "🇸🇨"),
            "249": ("Sudan", "🇸🇩"),
            "250": ("Rwanda", "🇷🇼"),
            "251": ("Ethiopia", "🇪🇹"),
            "252": ("Somalia", "🇸🇴"),
            "253": ("Djibouti", "🇩🇯"),
            "254": ("Kenya", "🇰🇪"),
            "255": ("Tanzania", "🇹🇿"),
            "256": ("Uganda", "🇺🇬"),
            "257": ("Burundi", "🇧🇮"),
            "258": ("Mozambique", "🇲🇿"),
            "260": ("Zambia", "🇿🇲"),
            "261": ("Madagascar", "🇲🇬"),
            "262": ("Reunion", "🇷🇪"),
            "263": ("Zimbabwe", "🇿🇼"),
            "264": ("Namibia", "🇳🇦"),
            "265": ("Malawi", "🇲🇼"),
            "266": ("Lesotho", "🇱🇸"),
            "267": ("Botswana", "🇧🇼"),
            "268": ("Eswatini", "🇸🇿"),
            "269": ("Comoros", "🇰🇲"),
            "290": ("Saint Helena", "🇸🇭"),
            "291": ("Eritrea", "🇪🇷"),
            "297": ("Aruba", "🇦🇼"),
            "298": ("Faroe Islands", "🇫🇴"),
            "299": ("Greenland", "🇬🇱"),
            "350": ("Gibraltar", "🇬🇮"),
            "351": ("Portugal", "🇵🇹"),
            "352": ("Luxembourg", "🇱🇺"),
            "353": ("Ireland", "🇮🇪"),
            "354": ("Iceland", "🇮🇸"),
            "355": ("Albania", "🇦🇱"),
            "356": ("Malta", "🇲🇹"),
            "357": ("Cyprus", "🇨🇾"),
            "358": ("Finland", "🇫🇮"),
            "359": ("Bulgaria", "🇧🇬"),
            "370": ("Lithuania", "🇱🇹"),
            "371": ("Latvia", "🇱🇻"),
            "372": ("Estonia", "🇪🇪"),
            "373": ("Moldova", "🇲🇩"),
            "374": ("Armenia", "🇦🇲"),
            "375": ("Belarus", "🇧🇾"),
            "376": ("Andorra", "🇦🇩"),
            "377": ("Monaco", "🇲🇨"),
            "378": ("San Marino", "🇸🇲"),
            "380": ("Ukraine", "🇺🇦"),
            "381": ("Serbia", "🇷🇸"),
            "382": ("Montenegro", "🇲🇪"),
            "383": ("Kosovo", "🇽🇰"),
            "385": ("Croatia", "🇭🇷"),
            "386": ("Slovenia", "🇸🇮"),
            "387": ("Bosnia and Herzegovina", "🇧🇦"),
            "389": ("North Macedonia", "🇲🇰"),
            "420": ("Czech Republic", "🇨🇿"),
            "421": ("Slovakia", "🇸🇰"),
            "423": ("Liechtenstein", "🇱🇮"),
            "500": ("Falkland Islands", "🇫🇰"),
            "501": ("Belize", "🇧🇿"),
            "502": ("Guatemala", "🇬🇹"),
            "503": ("El Salvador", "🇸🇻"),
            "504": ("Honduras", "🇭🇳"),
            "505": ("Nicaragua", "🇳🇮"),
            "506": ("Costa Rica", "🇨🇷"),
            "507": ("Panama", "🇵🇦"),
            "508": ("Saint Pierre and Miquelon", "🇵🇲"),
            "509": ("Haiti", "🇭🇹"),
            "590": ("Guadeloupe", "🇬🇵"),
            "591": ("Bolivia", "🇧🇴"),
            "592": ("Guyana", "🇬🇾"),
            "593": ("Ecuador", "🇪🇨"),
            "594": ("French Guiana", "🇬🇫"),
            "595": ("Paraguay", "🇵🇾"),
            "596": ("Martinique", "🇲🇶"),
            "597": ("Suriname", "🇸🇷"),
            "598": ("Uruguay", "🇺🇾"),
            "599": ("Netherlands Antilles", "🇳🇱"),
            "670": ("Timor-Leste", "🇹🇱"),
            "672": ("Australian External Territories", "🇦🇺"),
            "673": ("Brunei", "🇧🇳"),
            "674": ("Nauru", "🇳🇷"),
            "675": ("Papua New Guinea", "🇵🇬"),
            "676": ("Tonga", "🇹🇴"),
            "677": ("Solomon Islands", "🇸🇧"),
            "678": ("Vanuatu", "🇻🇺"),
            "679": ("Fiji", "🇫🇯"),
            "680": ("Palau", "🇵🇼"),
            "681": ("Wallis and Futuna", "🇼🇫"),
            "682": ("Cook Islands", "🇨🇰"),
            "683": ("Niue", "🇳🇺"),
            "684": ("American Samoa", "🇦🇸"),
            "685": ("Samoa", "🇼🇸"),
            "686": ("Kiribati", "🇰🇮"),
            "687": ("New Caledonia", "🇳🇨"),
            "688": ("Tuvalu", "🇹🇻"),
            "689": ("French Polynesia", "🇵🇫"),
            "690": ("Tokelau", "🇹🇰"),
            "691": ("Micronesia", "🇫🇲"),
            "692": ("Marshall Islands", "🇲🇭"),
            "850": ("North Korea", "🇰🇵"),
            "852": ("Hong Kong", "🇭🇰"),
            "853": ("Macau", "🇲🇴"),
            "855": ("Cambodia", "🇰🇭"),
            "856": ("Laos", "🇱🇦"),
            "880": ("Bangladesh", "🇧🇩"),
            "886": ("Taiwan", "🇹🇼"),
            "960": ("Maldives", "🇲🇻"),
            "961": ("Lebanon", "🇱🇧"),
            "962": ("Jordan", "🇯🇴"),
            "963": ("Syria", "🇸🇾"),
            "964": ("Iraq", "🇮🇶"),
            "965": ("Kuwait", "🇰🇼"),
            "966": ("Saudi Arabia", "🇸🇦"),
            "967": ("Yemen", "🇾🇪"),
            "968": ("Oman", "🇴🇲"),
            "970": ("Palestine", "🇵🇸"),
            "971": ("United Arab Emirates", "🇦🇪"),
            "972": ("Israel", "🇮🇱"),
            "973": ("Bahrain", "🇧🇭"),
            "974": ("Qatar", "🇶🇦"),
            "975": ("Bhutan", "🇧🇹"),
            "976": ("Mongolia", "🇲🇳"),
            "977": ("Nepal", "🇳🇵"),
            "992": ("Tajikistan", "🇹🇯"),
            "993": ("Turkmenistan", "🇹🇲"),
            "994": ("Azerbaijan", "🇦🇿"),
            "995": ("Georgia", "🇬🇪"),
            "996": ("Kyrgyzstan", "🇰🇬"),
            "998": ("Uzbekistan", "🇺🇿"),
        }
        found_code = ''
        for length in [3, 2, 1]:
            if len(phone_str) >= length:
                prefix = phone_str[:length]
                if prefix in country_codes:
                    found_code = prefix
                    break
        
        with_cc = f"+{num}"
        without_cc = num[len(found_code):]
        return with_cc, without_cc
    else:
        return f"+{num}", num

class NewPanelSmsManager:
    _instance = None
    _is_initialized = False
    _lock = threading.Lock()
    _application = None # Added for sending alerts
    _loop = None        # Added for sending alerts
    
    def set_application(self, application):
        self._application = application

    def set_loop(self, loop):
        self._loop = loop

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(NewPanelSmsManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._is_initialized:
            self._initialize_api()
    
    def _initialize_api(self):
        self._is_initialized = True
        logging.info("API-based SMS manager initialized")

    async def _send_critical_admin_alert(self, error_msg):
        """Sends a critical alert message to the admin."""
        if self._application and self._loop:
            try:
                await self._application.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=error_msg,
                    parse_mode=ParseMode.HTML
                )
                logging.info(f"Critical admin alert sent: {error_msg}")
            except Exception as e:
                logging.error(f"Failed to send critical admin alert: {e}")
        else:
            logging.error(f"Cannot send critical admin alert, bot application not set: {error_msg}")
    
    def get_api_url(self):
        today = datetime.now().strftime("%Y-%m-%d")
        # wider length to reduce pagination misses
        return f"{PANEL_BASE_URL}/agent/res/data_smscdr.php?fdate1={today}+00:00:00&fdate2={today}+23:59:59&iDisplayLength=200"
    
    def fetch_sms_from_api(self):
        session_check_headers = {
            "cookie": f"PHPSESSID={PHPSESSID}",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 OPR/122.0.0.0"
        }
        try:
            # Hit the /SMSCDRStats page to check for login redirect
            html_resp = requests.get(PANEL_SMS_URL, headers=session_check_headers, timeout=10)
            html_resp.raise_for_status()
            soup = BeautifulSoup(html_resp.text, "html.parser")
            title_tag = soup.find('title')
            
            if title_tag and 'Login' in title_tag.get_text():
                logging.error("Session check: appears to be login page. Update PHPSESSID.")
                if self._application and self._loop:
                    asyncio.run_coroutine_threadsafe(
                        self._send_critical_admin_alert(f"🚨 CRITICAL: Panel Session Expired! Update PHPSESSID in config.txt IMMEDIATELY. Time: {get_bst_now().strftime('%H:%M:%S')} BST"),
                        self._loop
                    )
                return []
        except Exception as e:
            logging.warning(f"Initial session check failed: {e}")
            return [] # Cannot proceed without a valid session or check

        data_url = self.get_api_url()
        data_headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "x-requested-with": "XMLHttpRequest", # Common AJAX header
            "cookie": f"PHPSESSID={PHPSESSID}",
            "referer": f"{PANEL_BASE_URL}/agent/SMSDashboard", # Required referer for the data endpoint
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 OPR/122.0.0.0"
        }
        
        retries = 3
        for attempt in range(retries):
            try:
                data_resp = requests.get(data_url, headers=data_headers, timeout=10)
                data_resp.raise_for_status()
                
                json_data = data_resp.json()
                
                if 'aaData' in json_data and isinstance(json_data['aaData'], list):
                    return json_data['aaData']
                elif isinstance(json_data, list):
                    return json_data # Sometimes it returns the array directly

                logging.warning(f"Data fetch attempt {attempt + 1}/{retries}: JSON missing 'aaData' or unexpected format.")
                
            except json.JSONDecodeError:
                logging.error(f"Data fetch attempt {attempt + 1}/{retries}: Response is not valid JSON. Response Text: {data_resp.text[:200]}...")
            except Exception as data_err:
                logging.warning(f"Data fetch attempt {attempt + 1}/{retries} failed: {data_err}")
                
            if attempt < retries - 1:
                time.sleep(5)
        
        logging.error("SMS data fetch failed after all attempts.")
        return []

    def scrape_and_save_all_sms(self):
        """Fetch SMS from API and save to file"""
        try:
            # This now calls the modified fetch_sms_from_api which returns structured data
            sms_data = self.fetch_sms_from_api()
            logging.info(f"Fetched {len(sms_data)} rows from API.") # Added logging
            sms_list = []
            
            for row in sms_data:
                try:
                    if len(row) >= 6:
                        # Extract data from API response
                        time_str = row[0] if len(row) > 0 else "N/A"
                        country_provider = row[1] if len(row) > 1 else "Unknown"
                        phone = row[2] if len(row) > 2 else "N/A"
                        service = row[3] if len(row) > 3 else "Unknown Service"
                        message = row[5] if len(row) > 5 else "N/A"
                        
                        # Extract country from country_provider string
                        country = "Unknown"
                        if isinstance(country_provider, str) and " " in country_provider:
                            country = country_provider.split()[0]
                        
                        if phone and message:
                            sms_list.append({
                                'country': country,
                                'provider': service,
                                'message': message,
                                'phone': phone
                            })
                except Exception as e:
                    logging.warning(f"Could not parse SMS row: {e}")

            logging.info(f"Processed {len(sms_list)} valid SMS entries.") # Added logging
            # Save SMS to file
            with self._lock:
                with open(SMS_CACHE_FILE, 'w', encoding='utf-8') as f:
                    for sms in sms_list:
                        f.write(json.dumps(sms) + "\n")
            
        except Exception as e:
            logging.error(f"SMS API fetch failed: {e}")

    def get_available_countries(self):
        countries = []
        country_counts = {}
        
        with self._lock:
            if not os.path.exists(NUMBERS_FILE):
                return countries
                
            with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    number = line.strip()
                    if number:
                        country, flag = detect_country_from_phone(number)
                        if country != "Unknown":
                            if country not in country_counts:
                                country_counts[country] = {'count': 0, 'flag': flag}
                            country_counts[country]['count'] += 1
        
        for country, data in country_counts.items():
            if data['count'] > 0:
                countries.append({'name': country, 'count': data['count'], 'flag': data['flag']})
        
        return sorted(countries, key=lambda x: x['count'], reverse=True)

    def assign_number_to_user(self, user_id: str, country: str = None) -> dict:
        users_data = load_json_data(USERS_FILE, {})
        
        if user_id not in users_data:
            users_data[user_id] = {"username": None, "first_name": "Anonymous", "last_number_time": 0}
        
        user_data = users_data[user_id]
        
        number = self.get_number_from_file(country)
        
        if not number:
            return {'success': False, 'error': 'No numbers available'}
        
        user_data['last_number_time'] = time.time()
        
        users_data[user_id] = user_data
        save_json_data(USERS_FILE, users_data)
        
        logging.info(f"Assigned number {number} to user {user_id} (country: {country}). No number tracking.")
        
        country_name, country_flag = detect_country_from_phone(number)

        return {
            'success': True,
            'number': number,
            'country': country_name,
            'flag': country_flag
        }
        
    def get_number_from_file(self, country=None):
        with self._lock:
            if not os.path.exists(NUMBERS_FILE):
                return None
            
            temp_file = f"{NUMBERS_FILE}.tmp"
            number_to_give = None
            
            try:
                with open(NUMBERS_FILE, 'r', encoding='utf-8') as original:
                    with open(temp_file, 'w', encoding='utf-8') as temp:
                        for line in original:
                            number = line.strip()
                            if not number:
                                continue
                                
                            if number_to_give is None:
                                if country:
                                    detected_country, _ = detect_country_from_phone(number)
                                    if detected_country == country:
                                        number_to_give = number
                                        continue
                                else:
                                    number_to_give = number
                                    continue
                            
                            temp.write(f"{number}\n")
                
                if number_to_give:
                    os.replace(temp_file, NUMBERS_FILE)
                    logging.info(f"Assigned number {number_to_give} for country {country}")
                else:
                    os.remove(temp_file)
                    if country:
                        logging.warning(f"No numbers available for country {country}")
                    else:
                        logging.warning("No numbers available")
                
                return number_to_give
                
            except Exception as e:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                logging.error(f"Error in get_number_from_file: {e}")
                return None

    def get_available_number_count(self):
        with self._lock:
            if not os.path.exists(NUMBERS_FILE):
                return 0
            with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
                return len([line for line in f if line.strip()])
    
    def cleanup(self):
        logging.info("Cleanup process started (no selenium drivers to quit).")

# --- Telegram Bot UI and Logic ---

def get_main_menu_keyboard():
    keyboard = [
        [KeyboardButton("📱 Paid Number"), KeyboardButton("📱 Get Number (Random)")],
        [KeyboardButton("🌍 Get Country"), KeyboardButton("🔐 OTP Check")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

async def check_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        
        status = member.status
        
        # --- BUG FIX: Compare string value to avoid AttributeError on enum ---
        status_value = str(status.value).lower()
        if status_value in ['member', 'administrator', 'creator']:
        # --- End of Bug Fix ---
            return True
        
        # Log if user has a non-member status (e.g., 'left' or 'kicked')
        logging.info(f"User {user_id} has non-member status: {status}")
        return False
        
    except error.BadRequest as e:
        # User not found in channel (or other bad request)
        logging.warning(f"User {user_id} not found in channel or bot issue: {e}")
        return False
    except Exception as e:
        # Log the full traceback for unexpected errors
        logging.error(f"CRITICAL: Unexpected error in check_subscription for {user_id}. Exception: {e}", exc_info=True)
        return False

# --- FIX: Added bypass_cooldown to function definition ---
async def perform_number_assignment(update: Update, context: ContextTypes.DEFAULT_TYPE, country: str = None, is_random: bool = False, bypass_cooldown: bool = False):
    user = update.effective_user
    user_id = str(user.id)
    query = update.callback_query
    
    users_data = load_json_data(USERS_FILE, {})
    user_data = users_data.get(user_id, {})
    cooldown = 5
    last_time = user_data.get('last_number_time', 0)
    current_time = time.time()

    if not bypass_cooldown and (current_time - last_time < cooldown):
        remaining_time = int(cooldown - (current_time - last_time))
        cooldown_msg = (
            f"<b>⏰ Please Wait</b>\n\n"
            f"<blockquote>🕐 <b>Cooldown:</b> {remaining_time} seconds remaining</blockquote>"
        )
        if query:
            # Must use reply_markup=None to avoid error when editing to a non-inline kbd
            await query.edit_message_text(cooldown_msg, parse_mode=ParseMode.HTML, reply_markup=None)
            # Send main menu in a new message
            await context.bot.send_message(chat_id=user.id, text="Please use the main menu.", reply_markup=get_main_menu_keyboard())
        else:
            await update.message.reply_text(cooldown_msg, parse_mode=ParseMode.HTML, reply_markup=get_main_menu_keyboard())
        return

    result = await asyncio.to_thread(manager_instance.assign_number_to_user, user_id, country)
    
    if result['success']:
        context.user_data['state'] = None
        
        assigned_country = result.get('country')
        country_flag = result.get('flag')
        country_display = f"{country_flag} {assigned_country}"
        with_cc, _ = get_number_formats(result['number']) # Changed to ignore without_cc
        
        current_time_str = datetime.now().strftime('%H:%M:%S')
        
        change_callback = "change_random" if is_random else f"change_country_{assigned_country}"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Number Change", callback_data=change_callback)],
            [InlineKeyboardButton("↩️ Back to Menu", callback_data="main_menu")]
        ])

        
        assignment_msg = (
            f"<b>🔥 {assigned_country} NUMBER ASSIGNED! ✨</b>\n\n" 
            f"<blockquote><b>⏰ Time:</b> <code>{current_time_str}</code></blockquote>\n\n"
            f"<blockquote><b>🌍 Country:</b> {html_escape(country_display)}</blockquote>\n\n"
            f"<blockquote><b>☎️ Number:</b> <code>{html_escape(with_cc)}</code></blockquote>"
            # Removed "Without CC" line
        )
        
        if query:
            await query.edit_message_text(
                text=assignment_msg,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
        else:
            await update.message.reply_text(
                text=assignment_msg,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
            
    else:
        error_msg = result.get('error', 'Unknown error')
        
        fail_msg = f"<b>❌ Assignment Failed</b>\n\n"
        
        # --- BUG FIX: Added missing <blockquote> tag ---
        fail_msg += "<blockquote>" 
        if country:
            country_display = f"{detect_country_from_phone(country)[1]} {country}"
            fail_msg += f"🌍 <b>Requested:</b> {country_display}\n"
        # --- End of Bug Fix ---
            
        fail_msg += f"⚠️ <b>Error:</b> {error_msg}</blockquote>\n\n"
        fail_msg += "<i>Please try again later.</i>"
        
        if query:
            await query.edit_message_text(fail_msg, parse_mode=ParseMode.HTML, reply_markup=None)
            await context.bot.send_message(chat_id=user.id, text="Please use the main menu.", reply_markup=get_main_menu_keyboard())
        else:
            await update.message.reply_text(fail_msg, parse_mode=ParseMode.HTML, reply_markup=get_main_menu_keyboard())


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = str(user.id)
    
    is_subscribed = await check_subscription(update, context, user.id)
    if not is_subscribed:
        join_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Join Update Channel", url=UPDATE_CHANNEL_LINK)],
            [InlineKeyboardButton("Try Again", callback_data="check_join")]
        ])
        
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"<b>🛑 Access Restricted!</b>\n\n"
                 f"<blockquote>Please join our official update channel to use the bot.</blockquote>",
            reply_markup=join_keyboard,
            parse_mode=ParseMode.HTML
        )
        return

    users_data = load_json_data(USERS_FILE, {})

    if user_id not in users_data:
        users_data[user_id] = {
            "username": user.username, "first_name": user.first_name, 
            "last_number_time": 0
        }
    
    save_json_data(USERS_FILE, users_data)
    
    current_time = datetime.now().strftime('%H:%M:%S')
    
    welcome_text = (
        f"<b>🎉 Welcome!</b>\n\n"
        f"<blockquote>👤 <b>User:</b> {user.first_name or 'Anonymous'}\n"
        f"⏰ <b>Time:</b> {current_time}</blockquote>\n\n"
        f"<b>🚀 Ready to get OTP numbers?</b> Click a button below to get started!"
    )
    
    if update.callback_query:
        # User clicked an inline button (like "Try Again" or "Back to Menu")
        try:
            # 1. Edit the original message to remove the inline buttons
            await update.callback_query.edit_message_text(
                text="<b>✅ Access Granted!</b>\n\n<i>Loading main menu...</i>",
                reply_markup=None, # Remove the inline buttons
                parse_mode=ParseMode.HTML
            )
        except error.BadRequest as e:
            if "message is not modified" not in str(e):
                logging.warning(f"Could not edit message on callback start: {e}")
        except Exception as e:
            logging.warning(f"Failed to edit original message on callback_start: {e}")
        
        # 2. Send a *new* message with the welcome text and the ReplyKeyboardMarkup
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=welcome_text,
            reply_markup=get_main_menu_keyboard(),
            parse_mode=ParseMode.HTML
        )
    else:
        # This is a normal /start command (user typed /start)
        await update.message.reply_text(
            welcome_text,
            reply_markup=get_main_menu_keyboard(),
            parse_mode=ParseMode.HTML
        )

async def safe_answer_callback(query, text="", show_alert=False):
    try:
        await query.answer(text=text, show_alert=show_alert)
        return True
    except (error.TimedOut, error.NetworkError) as e:
        logging.warning(f"Callback answer timeout: {e}")
        return False
    except error.BadRequest as e:
        logging.warning(f"Could not answer callback query: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error answering callback: {e}")
        return False

# --- Core Number Assignment Logic (Used by Random and Country flow) ---
# This function is now defined *before* start_command, so this is just a placeholder
# The real definition is above.

# --- New Button Handlers ---

async def handle_paid_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin_username = ADMIN_USERNAME.lstrip('@')
    
    user_msg = (
        f"<b>🤝 Contact Admin for Paid Number</b>\n\n"
        f"<blockquote>Please click the button below to message the admin directly with your request: 'sir i want to buy paid number'.</blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Message Admin", url=f"https://t.me/{admin_username}")]
    ])
    
    await update.message.reply_text(user_msg, parse_mode=ParseMode.HTML, reply_markup=keyboard)

async def handle_get_random_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    is_subscribed = await check_subscription(update, context, update.effective_user.id)
    if not is_subscribed:
        return await start_command(update, context) 
        
    await perform_number_assignment(update, context, country=None, is_random=True)

async def handle_get_country_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    is_subscribed = await check_subscription(update, context, update.effective_user.id)
    if not is_subscribed:
        return await start_command(update, context)

    context.user_data['state'] = None 
    user_id = str(update.effective_user.id)
    users_data = load_json_data(USERS_FILE, {})
    user_data = users_data.get(user_id, {})

    countries = await asyncio.to_thread(manager_instance.get_available_countries)
    
    if not countries:
        await update.message.reply_text(
            "<b>😔 No Numbers Available</b>\n\n"
            "<blockquote>📱 All numbers are currently assigned.</blockquote>\n\n"
            "<i>Please try again later or contact admin.</i>",
            parse_mode=ParseMode.HTML
        )
        return
    
    context.user_data['state'] = 'SELECTING_COUNTRY'
    
    keyboard_buttons = []
    for country in countries:
        country_name = country['name']
        country_flag = country['flag']
        country_display = f"{country_flag} {country_name}"
        button_text = f"{country_display} ({country['count']})"
        button = InlineKeyboardButton(button_text, callback_data=f"country_{country_name}")
        keyboard_buttons.append([button])
    
    cancel_button = InlineKeyboardButton("↩️ Back to Menu", callback_data='main_menu')
    keyboard_buttons.append([cancel_button])
    
    reply_markup = InlineKeyboardMarkup(keyboard_buttons)
    
    # User number count is removed from display
    await update.message.reply_text(
        f"<b>🌍 Choose Your Country</b>\n\n"
        f"<blockquote>📊 <b>Available Countries:</b> {len(countries)}</blockquote>\n\n"
        f"<i>Select a country to get a number from that region.</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup
    )


async def handle_otp_check_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    is_subscribed = await check_subscription(update, context, update.effective_user.id)
    if not is_subscribed:
        return await start_command(update, context)

    context.user_data['state'] = 'WAITING_FOR_OTP_NUMBER'
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Join OTP Group", url=GROUP_LINK)],
        [InlineKeyboardButton("Cancel", callback_data="main_menu")] # Changed text
    ])
    
    await update.message.reply_text(
        f"<b>🔐 OTP Check initiated.</b>\n\n"
        f"<blockquote>Please send the **full phone number** (with country code, e.g., +91XXXXXXXXXX) you received from the bot that you want to check the OTP for.</blockquote>",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard
    )

async def handle_otp_number_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get('state') != 'WAITING_FOR_OTP_NUMBER':
        return 

    input_number = update.message.text.strip()
    
    context.user_data['state'] = None 
    
    normalized_number = normalize_number(input_number)
    
    if len(normalized_number) < 5 or not normalized_number.isdigit():
        await update.message.reply_text(
            f"<b>❌ Invalid Number</b>\n\n"
            f"<blockquote>The number format is invalid. Please ensure you send the full phone number (e.g., +919876543210).</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_menu_keyboard()
        )
        return
    
    await update.message.reply_text("<b>🔍 Checking for OTP/SMS... Please wait.</b>", parse_mode=ParseMode.HTML)
    
    await asyncio.to_thread(manager_instance.scrape_and_save_all_sms)
    
    found_sms = []
    
    if os.path.exists(SMS_CACHE_FILE):
        with open(SMS_CACHE_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    sms_data = json.loads(line)
                    sms_phone = normalize_number(sms_data.get('phone', ''))
                    
                    if normalized_number == sms_phone:
                        found_sms.append(sms_data)
                except Exception as e:
                    logging.warning(f"Error parsing line in SMS cache for check: {e}")

    if found_sms:
        latest_sms = found_sms[-1]
        
        service_name = latest_sms.get('provider', 'Unknown Service')
        country_name = latest_sms.get('country', 'Unknown Country')
        message = latest_sms.get('message', '')
        otp = extract_otp_from_text(message)
        otp_display = f"<code>{otp}</code>" if otp != "N/A" else "Not found"
        phone_with_cc, _ = get_number_formats(latest_sms.get('phone'))
        current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        inbox_msg = (
            f"<b>✅ OTP/SMS Found!</b>\n\n"
            f"<blockquote><b>⏰ Time:</b> <code>{current_time_str}</code></blockquote>\n\n"
            f"<blockquote><b>🌍 Country:</b> {html_escape(country_name)}</blockquote>\n\n"
            f"<blockquote><b>⚙️ Service:</b> {html_escape(service_name)}</blockquote>\n\n"
            f"<blockquote><b>☎️ Number:</b> <code>{phone_with_cc}</code></blockquote>\n\n"
            f"<blockquote><b>🔑 OTP:</b> {otp_display}</blockquote>\n\n"
            f"<b>✉️ Full Message:</b>\n\n"
            f"<blockquote><i>{html_escape(message) if message else '(No message body)'}</i></blockquote>"
        )
        
        await update.message.reply_text(inbox_msg, parse_mode=ParseMode.HTML, reply_markup=get_main_menu_keyboard())

    else:
        await update.message.reply_text(
            f"<b>😔 No New OTP/SMS Found</b>\n\n"
            f"<blockquote>We could not find any new messages for <code>{input_number}</code> yet.\n"
            f"Please wait a few moments and try checking again.</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_menu_keyboard()
        )
    


async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback(query)

    if query.data == 'main_menu':
        await start_command(update, context)
        return
    
    elif query.data == 'check_join':
        await start_command(update, context)
        return

    elif query.data == 'change_random':
        await safe_answer_callback(query, text="🔄 Fetching new random number...")
        await perform_number_assignment(update, context, country=None, is_random=True, bypass_cooldown=True) # Bypass added

    elif query.data.startswith('change_country_'):
        country = query.data.split('_', 2)[2]
        await safe_answer_callback(query, text=f"🔄 Fetching new number for {country}...")
        await perform_number_assignment(update, context, country=country, is_random=False, bypass_cooldown=True) # Bypass added

    elif query.data.startswith('country_'):
        country = query.data.split('_', 1)[1]
        await safe_answer_callback(query, text=f"✅ Assigning number from {country}...")
        await perform_number_assignment(update, context, country=country, is_random=False)
    

async def handle_text_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    chat_type = update.effective_chat.type

    if context.user_data.get('state') == 'WAITING_FOR_OTP_NUMBER' and chat_type == 'private':
        await handle_otp_number_input(update, context)
        return
    
    if text == "📱 Paid Number":
        await handle_paid_number(update, context)
    elif text == "📱 Get Number (Random)":
        await handle_get_random_number(update, context)
    elif text == "🌍 Get Country":
        await handle_get_country_menu(update, context)
    elif text == "🔐 OTP Check":
        await handle_otp_check_menu(update, context)
    else:
        if chat_type == 'private':
            await update.message.reply_text(
                text="<b>❌ Error: Invalid message format</b>\n\n"
                     "<blockquote>I don't understand regular text messages. Please use the buttons provided.</blockquote>\n\n"
                     "<b>💡 Tip:</b> Click /start to see available options.",
                parse_mode=ParseMode.HTML,
                reply_markup=get_main_menu_keyboard()
            )


async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.username != ADMIN_USERNAME:
        logging.warning(f"Unauthorized broadcast attempt by {user.username}")
        return

    if not context.args:
        await update.message.reply_text("<b>Usage:</b> /update [your message]", parse_mode=ParseMode.HTML)
        return

    message_text = " ".join(context.args)
    formatted_message = f"<blockquote>{html_escape(message_text)}</blockquote>"
    
    users_data = load_json_data(USERS_FILE, {})
    user_ids = list(users_data.keys())
    
    success_count, fail_count = 0, 0
    
    await update.message.reply_text(f"📢 Starting to broadcast to {len(user_ids)} users...")

    for user_id in user_ids:
        try:
            await context.bot.send_message(chat_id=user_id, text=formatted_message, parse_mode=ParseMode.HTML)
            success_count += 1
            await asyncio.sleep(0.1)
        except Exception as e:
            fail_count += 1
            logging.error(f"Failed to send broadcast to {user_id}: {e}")
            
    await update.message.reply_text(
        f"<b>📢 Broadcast Complete!</b>\n"
        f"✅ Sent successfully to {success_count} users.\n"
        f"❌ Failed to send to {fail_count} users.",
        parse_mode=ParseMode.HTML
    )

async def add_numbers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.username != ADMIN_USERNAME:
        logging.warning(f"Unauthorized /add attempt by {user.username}")
        return

    if not context.args:
        await update.message.reply_text("<b>Usage:</b> /add [number1] [number2] ...", parse_mode=ParseMode.HTML)
        return

    added_count = 0
    failed_numbers = []
    
    with open(NUMBERS_FILE, 'a', encoding='utf-8') as f:
        for number in context.args:
            number = number.strip()
            if number:
                try:
                    f.write(f"{number}\n")
                    added_count += 1
                except Exception as e:
                    logging.error(f"Failed to write number {number} to file: {e}")
                    failed_numbers.append(number)

    if added_count > 0:
        await update.message.reply_text(f"<b>✅ Successfully added {added_count} new numbers.</b>", parse_mode=ParseMode.HTML)
    
    if failed_numbers:
        await update.message.reply_text(f"<b>❌ Failed to add the following numbers:</b>\n<code>{', '.join(failed_numbers)}</code>", parse_mode=ParseMode.HTML)


async def delete_numbers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.username != ADMIN_USERNAME:
        logging.warning(f"Unauthorized /delete attempt by {user.username}")
        return

    if not context.args:
        await update.message.reply_text("<b>Usage:</b> /delete [number1] [number2] ...", parse_mode=ParseMode.HTML)
        return

    deleted_count = 0
    failed_numbers = []
    
    with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    with open(NUMBERS_FILE, 'w', encoding='utf-8') as f:
        for line in lines:
            number = line.strip()
            if number not in context.args:
                f.write(f"{number}\n")
            else:
                deleted_count += 1

    if deleted_count > 0:
        await update.message.reply_text(f"<b>✅ Successfully deleted {deleted_count} numbers.</b>", parse_mode=ParseMode.HTML)
    
    if deleted_count != len(context.args):
        await update.message.reply_text(f"<b>❌ Failed to delete the following numbers:</b>\n<code>{', '.join(failed_numbers)}</code>", parse_mode=ParseMode.HTML)


async def sms_watcher_task(application: Application):
    logging.info("SMS watcher task started.")
    global manager_instance
    if not manager_instance:
        manager_instance = IvaSmsManager()
        
    while not shutdown_event.is_set():
        try:
            await asyncio.to_thread(manager_instance.scrape_and_save_all_sms)
            
            if not os.path.exists(SMS_CACHE_FILE):
                await asyncio.sleep(5)
                continue

            users_data = load_json_data(USERS_FILE, {})
            sent_sms_keys = load_sent_sms_keys()
            
            with open(SMS_CACHE_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        sms_data = json.loads(line)
                        phone = sms_data.get('phone')
                        message = sms_data.get('message')

                        if not phone:
                            continue

                        unique_key = f"{phone}|{message}"
                        if unique_key in sent_sms_keys:
                            continue

                        otp = extract_otp_from_text(message)
                        # --- FIX 1: Define otp_display ---
                        otp_display = f"<code>{otp}</code>" if otp != "N/A" else "Not found"
                        
                        user_first_name = 'Public User'
                        
                        phone_with_cc, _ = get_number_formats(phone)
                        masked_phone = mask_phone_number(phone_with_cc)
                        
                        country_name, country_flag = detect_country_from_phone(phone)
                        country_display = f"{country_flag} {country_name}"

                        group_title = "📱 <b>New OTP Received!</b> ✨" if otp != "N/A" else "📱 <b>New Message Received!</b> ✨"
                        
                        group_msg = (
                            f"<b>{group_title}</b>\n\n" 
                            f"<blockquote>📞 <b>Number:</b> <code>{masked_phone}</code></blockquote>\n\n"
                            f"<blockquote>🌍 <b>Country:</b> {html_escape(country_display)}</blockquote>\n\n"
                            f"<blockquote>🔑 <b>OTP Code:</b> {otp_display}</blockquote>\n\n"
                            f"<blockquote>👤 <b>User:</b> {html_escape(user_first_name)}</blockquote>\n\n"
                            f"<blockquote>⏰ <b>Time:</b> <code>{datetime.now().strftime('%H:%M:%S')}</code></blockquote>\n\n"
                            # --- FIX 2: Add Full Message to Group Message ---
                            f"<b>✉️ Full Message:</b>\n\n"
                            f"<blockquote><i>{html_escape(message) if message else '(No message body)'}</i></blockquote>"
                        )

                        group_keyboard = InlineKeyboardMarkup([
                            [InlineKeyboardButton("Update Channel", url=UPDATE_CHANNEL_LINK)]
                        ])

                        try:
                            await application.bot.send_message(
                                chat_id=GROUP_ID,
                                text=group_msg,
                                parse_mode=ParseMode.HTML,
                                reply_markup=group_keyboard
                            )
                            logging.info(f"Sent SMS to group: {phone_with_cc}")
                        except Exception as e:
                            logging.error(f"Failed to send SMS to group {GROUP_ID}: {type(e).__name__}: {e}")

                        admin_msg = (
                            f"<b>📱 New SMS Received (Untracked)</b>\n\n"
                            f"<blockquote>📞 <b>Number:</b> <code>{phone_with_cc}</code>\n"
                            f"🌍 <b>Country:</b> {html_escape(country_display)}\n"
                            f"🔑 <b>OTP Code:</b> {otp_display}\n\n"
                            f"📝 <b>Message:</b>\n{html_escape(message) if message else '<i>(No message body)</i>'}</blockquote>"
                        )
                        
                        try:
                            await application.bot.send_message(
                                chat_id=ADMIN_ID,
                                text=admin_msg,
                                parse_mode=ParseMode.HTML
                            )
                            logging.info(f"Sent SMS to admin: {phone_with_cc}")
                        except Exception as e:
                            logging.error(f"Failed to send SMS notification to admin {ADMIN_ID}: {type(e).__name__}: {e}")

                        sent_sms_keys.add(unique_key)
                    except Exception as e:
                        logging.error(f"Error processing SMS line: {e}")

            save_sent_sms_keys(sent_sms_keys)

        except Exception as e:
            logging.error(f"Error in sms_watcher_task: {e}")
        
        await asyncio.sleep(10)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.error("Exception while handling an update:", exc_info=context.error)
    
    if isinstance(context.error, error.TimedOut):
        logging.warning("Telegram API timeout - temporary")
        return
    elif isinstance(context.error, error.NetworkError):
        logging.warning("Network error - connection issues")
        return
    elif isinstance(context.error, error.BadRequest):
        logging.warning(f"Bad request: {context.error}")
        return
    
    logging.error(f"Unhandled error: {context.error}")

async def main_bot_loop():
    global manager_instance
    
    loop = asyncio.get_running_loop()
        
    manager_instance = NewPanelSmsManager()

    from telegram.request import HTTPXRequest
    
    request = HTTPXRequest(
        connection_pool_size=8,
        read_timeout=30.0,
        write_timeout=30.0,
        connect_timeout=10.0,
        pool_timeout=10.0
    )
    
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).request(request).build()
    
    if manager_instance:
        manager_instance.set_application(application)
        manager_instance.set_loop(loop)
        
    application.add_error_handler(error_handler)
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("update", broadcast_command))
    application.add_handler(CommandHandler("add", add_numbers_command))
    application.add_handler(CommandHandler("delete", delete_numbers_command))
    application.add_handler(CallbackQueryHandler(button_callback_handler))
    
    menu_buttons = ["📱 Paid Number", "📱 Get Number (Random)", "🌍 Get Country", "🔐 OTP Check"] 
    menu_filter = filters.TEXT & filters.Regex(f'^({"|".join(re.escape(btn) for btn in menu_buttons)})$')
    application.add_handler(MessageHandler(menu_filter, handle_text_menu))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_menu))

    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    logging.info("Bot started successfully.")

    try:
        await application.bot.send_message(
            chat_id=GROUP_ID,
            text="✅ Bot is Active"
        )
        logging.info("Startup message sent to the group.")
    except BaseException as e:
        logging.error(f"Failed to send startup message: {type(e).__name__}: {e}")

    sms_task = asyncio.create_task(sms_watcher_task(application))
    
    await shutdown_event.wait()
    
    sms_task.cancel()
    try:
        await sms_task
    except asyncio.CancelledError:
        logging.info("Background tasks cancelled.")

    await application.updater.stop()
    await application.stop()
    await application.shutdown()
    logging.info("Bot stopped gracefully.")

def main():
    logging.info("Bot starting...")
    loop = None
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(main_bot_loop())
    except KeyboardInterrupt:
        logging.info("Bot shutting down gracefully...")
        shutdown_event.set()
    except Exception as e:
        logging.error(f"Unhandled exception in main: {e}", exc_info=True)
    finally:
        # Wait for the bot to shut down
        time.sleep(5)
        global manager_instance
        if manager_instance:
            manager_instance.cleanup()
        if loop:
            # Wait for all tasks to complete
            tasks = asyncio.all_tasks(loop=loop)
            for task in tasks:
                task.cancel()
            group = asyncio.gather(*tasks, return_exceptions=True)
            loop.run_until_complete(group)
            loop.close()
        logging.info("Bot stopped.")

if __name__ == "__main__":
    main()
