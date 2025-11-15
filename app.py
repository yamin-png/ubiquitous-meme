import os
import json
import time
import asyncio
import threading
import aiohttp
from datetime import datetime
from bs4 import BeautifulSoup
from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup, error
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

import logging
import re
import configparser

# SeleniumBase will handle Cloudflare bypass automatically

CONFIG_FILE = 'config.txt'

def load_config():
    """Load configuration from config.txt."""
    if not os.path.exists(CONFIG_FILE):
        logging.critical(f"{CONFIG_FILE} not found!")
        raise FileNotFoundError(f"{CONFIG_FILE} not found!")
        
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE, encoding='utf-8')
    return config['Settings']


def normalize_number(num: str) -> str:
    if not num:
        return ''
    return re.sub(r'\D', '', str(num)).lstrip('0')


def format_number_variants(phone: str) -> dict:
    """Return a dict with 'international' and 'plain' variants.

    - international: tries to include +country if present; otherwise returns original
    - plain: digits-only without country code (easy to copy)
    """
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


def mask_phone_number(phone_number_str: str) -> str:
    """Masks a phone number, showing first 5-6 and last 4. e.g., +22176XX81326"""
    if not phone_number_str:
        return "N/A"
    
    # Clean the number of any formatting
    digits = re.sub(r'\D', '', phone_number_str)
    
    # Handle potential '+' prefix
    prefix = ""
    if phone_number_str.startswith('+'):
        prefix = "+"
        # This is a bit tricky, let's just work with the raw string if it has '+'
        raw_num = phone_number_str.lstrip('+')
    else:
        raw_num = phone_number_str

    if len(raw_num) > 10:
        # Assumes format like [CC][Number]
        # Find country code
        cc_len = 0
        for code in sorted(COUNTRY_CODES.keys(), key=len, reverse=True):
            code_digits = code.lstrip('+')
            if raw_num.startswith(code_digits):
                cc_len = len(code_digits)
                break
        
        if cc_len == 0: # Fallback if CC not in our list
             cc_len = len(raw_num) - 9 # Assume 9 digit local number
             if cc_len < 1: cc_len = 3

        local_num = raw_num[cc_len:]
        if len(local_num) > 6:
            # Format: +[CC][First 2 of local]XX[Last 4 of local]
            return f"{prefix}{raw_num[:cc_len]}{local_num[:2]}XX{local_num[-4:]}"
        else:
            # Not a standard length, just mask middle
            return f"{prefix}{raw_num[:4]}XX{raw_num[-4:]}"
    elif len(raw_num) > 6:
        # Shorter number, just mask middle
        return f"{prefix}{raw_num[:3]}XX{raw_num[-3:]}"
    else:
        return f"{prefix}{raw_num[:1]}XX" # Very short, just show first char


try:
    config = load_config()
    SMS_AMOUNT = float(config.get('SMS_AMOUNT', 0.0078))
    WITHDRAWAL_LIMIT = float(config.get('WITHDRAWAL_LIMIT', 50))
    BINANCE_MINIMUM_WITHDRAWAL = float(config.get('BINANCE_MINIMUM_WITHDRAWAL', 1250))
    REFERRAL_WITHDRAWAL_BONUS_PERCENT = float(config.get('REFERRAL_WITHDRAWAL_BONUS_PERCENT', 5))
    PAYMENT_METHODS = [method.strip() for method in config.get('PAYMENT_METHOD', 'Bkash,Nagad,Binance').split(',')]
except (FileNotFoundError, KeyError, ValueError) as e:
    print(f"Configuration Error: {e}")
    SMS_AMOUNT, WITHDRAWAL_LIMIT, REFERRAL_WITHDRAWAL_BONUS_PERCENT = 0.0078, 50.0, 5.0
    BINANCE_MINIMUM_WITHDRAWAL = 1250.0
    PAYMENT_METHODS = ["Bkash", "Nagad", "Binance"]
    
TELEGRAM_BOT_TOKEN = "7811577720:AAGNoS9KEaziHpllsdYu1v2pGqQU7TVqJGE"
GROUP_ID = -1003009605120
PAYMENT_CHANNEL_ID = -1003184589906
ADMIN_ID = 5473188537 # Admin ID for notifications
GROUP_LINK = "https://t.me/pgotp"
EMAIL = "smyaminhasan9@gmail.com"
PASSWORD = "yamin12134@!"
ADMIN_USERNAME = "smyaminhasan" # Admin username for broadcast command

# File Paths
USERS_FILE = 'users.json'
SMS_CACHE_FILE = 'sms.txt'
SENT_SMS_FILE = 'sent_sms.json'
NUMBERS_FILE = 'numbers.txt' # File to store available numbers

# Country code mapping with flag emojis
COUNTRY_CODES = {
    '+93': 'AFGHANISTAN',
    '+355': 'ALBANIA',
    '+213': 'ALGERIA',
    '+1684': 'AMERICAN SAMOA',
    '+376': 'ANDORRA',
    '+244': 'ANGOLA',
    '+1264': 'ANGUILLA',
    '+672': 'ANTARCTICA',
    '+1268': 'ANTIGUA AND BARBUDA',
    '+54': 'ARGENTINA',
    '+374': 'ARMENIA',
    '+297': 'ARUBA',
    '+61': 'AUSTRALIA',
    '+43': 'AUSTRIA',
    '+994': 'AZERBAIJAN',
    '+1242': 'BAHAMAS',
    '+973': 'BAHRAIN',
    '+880': 'BANGLADESH',
    '+1246': 'BARBADOS',
    '+375': 'BELARUS',
    '+32': 'BELGIUM',
    '+501': 'BELIZE',
    '+229': 'BENIN',
    '+1441': 'BERMUDA',
    '+975': 'BHUTAN',
    '+591': 'BOLIVIA',
    '+387': 'BOSNIA AND HERZEGOVINA',
    '+267': 'BOTSWANA',
    '+55': 'BRAZIL',
    '+246': 'BRITISH INDIAN OCEAN TERRITORY',
    '+673': 'BRUNEI DARUSSALAM',
    '+359': 'BULGARIA',
    '+226': 'BURKINA FASO',
    '+257': 'BURUNDI',
    '+855': 'CAMBODIA',
    '+237': 'CAMEROON',
    '+1': 'CANADA',
    '+238': 'CAPE VERDE',
    '+1345': 'CAYMAN ISLANDS',
    '+236': 'CENTRAL AFRICAN REPUBLIC',
    '+235': 'CHAD',
    '+56': 'CHILE',
    '+86': 'CHINA',
    '+61': 'CHRISTMAS ISLAND',
    '+61': 'COCOS (KEELING) ISLANDS',
    '+57': 'COLOMBIA',
    '+269': 'COMOROS',
    '+242': 'CONGO',
    '+243': 'CONGO, THE DEMOCRATIC REPUBLIC OF THE',
    '+682': 'COOK ISLANDS',
    '+506': 'COSTA RICA',
    '+225': 'COTE DIVOIRE',
    '+385': 'CROATIA',
    '+53': 'CUBA',
    '+357': 'CYPRUS',
    '+420': 'CZECH REPUBLIC',
    '+45': 'DENMARK',
    '+253': 'DJIBOUTI',
    '+1767': 'DOMINICA',
    '+1809': 'DOMINICAN REPUBLIC',
    '+593': 'ECUADOR',
    '+20': 'EGYPT',
    '+503': 'EL SALVADOR',
    '+240': 'EQUATORIAL GUINEA',
    '+291': 'ERITREA',
    '+372': 'ESTONIA',
    '+251': 'ETHIOPIA',
    '+500': 'FALKLAND ISLANDS (MALVINAS)',
    '+298': 'FAROE ISLANDS',
    '+679': 'FIJI',
    '+358': 'FINLAND',
    '+33': 'FRANCE',
    '+594': 'FRENCH GUIANA',
    '+689': 'FRENCH POLYNESIA',
    '+262': 'FRENCH SOUTHERN TERRITORIES',
    '+241': 'GABON',
    '+220': 'GAMBIA',
    '+995': 'GEORGIA',
    '+49': 'GERMANY',
    '+233': 'GHANA',
    '+350': 'GIBRALTAR',
    '+30': 'GREECE',
    '+299': 'GREENLAND',
    '+1473': 'GRENADA',
    '+590': 'GUADELOUPE',
    '+1671': 'GUAM',
    '+502': 'GUATEMALA',
    '+44': 'GUERNSEY',
    '+224': 'GUINEA',
    '+245': 'GUINEA-BISSAU',
    '+592': 'GUYANA',
    '+509': 'HAITI',
    '+39': 'HOLY SEE (VATICAN CITY STATE)',
    '+504': 'HONDURAS',
    '+852': 'HONG KONG',
    '+36': 'HUNGARY',
    '+354': 'ICELAND',
    '+91': 'INDIA',
    '+62': 'INDONESIA',
    '+98': 'IRAN, ISLAMIC REPUBLIC OF',
    '+964': 'IRAQ',
    '+353': 'IRELAND',
    '+44': 'ISLE OF MAN',
    '+972': 'ISRAEL',
    '+39': 'ITALY',
    '+1876': 'JAMAICA',
    '+81': 'JAPAN',
    '+44': 'JERSEY',
    '+962': 'JORDAN',
    '+7': 'KAZAKHSTAN',
    '+254': 'KENYA',
    '+686': 'KIRIBATI',
    '+850': 'KOREA, DEMOCRATIC PEOPLES REPUBLIC OF',
    '+82': 'KOREA, REPUBLIC OF',
    '+965': 'KUWAIT',
    '+996': 'KYRGYZSTAN',
    '+856': 'LAO PEOPLES DEMOCRATIC REPUBLIC',
    '+371': 'LATVIA',
    '+961': 'LEBANON',
    '+266': 'LESOTHO',
    '+231': 'LIBERIA',
    '+218': 'LIBYAN ARAB JAMAHIRIYA',
    '+423': 'LIECHTENSTEIN',
    '+370': 'LITHUANIA',
    '+352': 'LUXEMBOURG',
    '+853': 'MACAO',
    '+389': 'MACEDONIA, THE FORMER YUGOSLAV REPUBLIC OF',
    '+261': 'MADAGASCAR',
    '+265': 'MALAWI',
    '+60': 'MALAYSIA',
    '+960': 'MALDIVES',
    '+223': 'MALI',
    '+356': 'MALTA',
    '+692': 'MARSHALL ISLANDS',
    '+596': 'MARTINIQUE',
    '+222': 'MAURITANIA',
    '+230': 'MAURITIUS',
    '+262': 'MAYOTTE',
    '+52': 'MEXICO',
    '+691': 'MICRONESIA, FEDERATED STATES OF',
    '+373': 'MOLDOVA, REPUBLIC OF',
    '+377': 'MONACO',
    '+976': 'MONGOLIA',
    '+382': 'MONTENEGRO',
    '+1664': 'MONTSERRAT',
    '+212': 'MOROCCO',
    '+258': 'MOZAMBIQUE',
    '+95': 'MYANMAR',
    '+264': 'NAMIBIA',
    '+674': 'NAURU',
    '+977': 'NEPAL',
    '+31': 'NETHERLANDS',
    '+599': 'NETHERLANDS ANTILLES',
    '+687': 'NEW CALEDONIA',
    '+64': 'NEW ZEALAND',
    '+505': 'NICARAGUA',
    '+227': 'NIGER',
    '+234': 'NIGERIA',
    '+683': 'NIUE',
    '+672': 'NORFOLK ISLAND',
    '+1670': 'NORTHERN MARIANA ISLANDS',
    '+47': 'NORWAY',
    '+968': 'OMAN',
    '+92': 'PAKISTAN',
    '+680': 'PALAU',
    '+970': 'PALESTINIAN TERRITORY, OCCUPIED',
    '+507': 'PANAMA',
    '+675': 'PAPUA NEW GUINEA',
    '+595': 'PARAGUAY',
    '+51': 'PERU',
    '+63': 'PHILIPPINES',
    '+870': 'PITCAIRN',
    '+48': 'POLAND',
    '+351': 'PORTUGAL',
    '+1': 'PUERTO RICO',
    '+974': 'QATAR',
    '+262': 'REUNION',
    '+40': 'ROMANIA',
    '+7': 'RUSSIAN FEDERATION',
    '+250': 'RWANDA',
    '+590': 'SAINT BARTHELEMY',
    '+290': 'SAINT HELENA',
    '+1869': 'SAINT KITTS AND NEVIS',
    '+1758': 'SAINT LUCIA',
    '+590': 'SAINT MARTIN',
    '+508': 'SAINT PIERRE AND MIQUELON',
    '+1784': 'SAINT VINCENT AND THE GRENADINES',
    '+685': 'SAMOA',
    '+378': 'SAN MARINO',
    '+239': 'SAO TOME AND PRINCIPE',
    '+966': 'SAUDI ARABIA',
    '+221': 'SENEGAL',
    '+381': 'SERBIA',
    '+248': 'SEYCHELLES',
    '+232': 'SIERRA LEONE',
    '+65': 'SINGAPORE',
    '+421': 'SLOVAKIA',
    '+386': 'SLOVENIA',
    '+677': 'SOLOMON ISLANDS',
    '+252': 'SOMALIA',
    '+27': 'SOUTH AFRICA',
    '+500': 'SOUTH GEORGIA AND THE SOUTH SANDWICH ISLANDS',
    '+34': 'SPAIN',
    '+94': 'SRI LANKA',
    '+249': 'SUDAN',
    '+597': 'SURINAME',
    '+47': 'SVALBARD AND JAN MAYEN',
    '+268': 'SWAZILAND',
    '+46': 'SWEDEN',
    '+41': 'SWITZERLAND',
    '+963': 'SYRIAN ARAB REPUBLIC',
    '+886': 'TAIWAN, PROVINCE OF CHINA',
    '+992': 'TAJIKISTAN',
    '+255': 'TANZANIA, UNITED REPUBLIC OF',
    '+66': 'THAILAND',
    '+670': 'TIMOR-LESTE',
    '+228': 'TOGO',
    '+690': 'TOKELAU',
    '+676': 'TONGA',
    '+1868': 'TRINIDAD AND TOBAGO',
    '+216': 'TUNISIA',
    '+90': 'TURKEY',
    '+993': 'TURKMENISTAN',
    '+1649': 'TURKS AND CAICOS ISLANDS',
    '+688': 'TUVALU',
    '+256': 'UGANDA',
    '+380': 'UKRAINE',
    '+971': 'UNITED ARAB EMIRATES',
    '+44': 'UNITED KINGDOM',
    '+1': 'UNITED STATES',
    '+598': 'URUGUAY',
    '+998': 'UZBEKISTAN',
    '+678': 'VANUATU',
    '+58': 'VENEZUELA',
    '+84': 'VIET NAM',
    '+1284': 'VIRGIN ISLANDS, BRITISH',
    '+1340': 'VIRGIN ISLANDS, U.S.',
    '+681': 'WALLIS AND FUTUNA',
    '+212': 'WESTERN SAHARA',
    '+967': 'YEMEN',
    '+260': 'ZAMBIA',
    '+263': 'ZIMBABWE'
}

# Country name mapping with flags for display
COUNTRY_FLAGS = {
    'AFGHANISTAN': '🇦🇫 Afghanistan',
    'ALBANIA': '🇦🇱 Albania',
    'ALGERIA': '🇩🇿 Algeria',
    'AMERICAN SAMOA': '🇦🇸 American Samoa',
    'ANDORRA': '🇦🇩 Andorra',
    'ANGOLA': '🇦🇴 Angola',
    'ANGUILLA': '🇦🇮 Anguilla',
    'ANTARCTICA': '🇦🇶 Antarctica',
    'ANTIGUA AND BARBUDA': '🇦🇬 Antigua and Barbuda',
    'ARGENTINA': '🇦🇷 Argentina',
    'ARMENIA': '🇦🇲 Armenia',
    'ARUBA': '🇦🇼 Aruba',
    'AUSTRALIA': '🇦🇺 Australia',
    'AUSTRIA': '🇦🇹 Austria',
    'AZERBAIJAN': '🇦🇿 Azerbaijan',
    'BAHAMAS': '🇧🇸 Bahamas',
    'BAHRAIN': '🇧🇭 Bahrain',
    'BANGLADESH': '🇧🇩 Bangladesh',
    'BARBADOS': '🇧🇧 Barbados',
    'BELARUS': '🇧🇾 Belarus',
    'BELGIUM': '🇧🇪 Belgium',
    'BELIZE': '🇧🇿 Belize',
    'BENIN': '🇧🇯 Benin',
    'BERMUDA': '🇧🇲 Bermuda',
    'BHUTAN': '🇧🇹 Bhutan',
    'BOLIVIA': '🇧🇴 Bolivia',
    'BOSNIA AND HERZEGOVINA': '🇧🇦 Bosnia and Herzegovina',
    'BOTSWANA': '🇧🇼 Botswana',
    'BRAZIL': '🇧🇷 Brazil',
    'BRITISH INDIAN OCEAN TERRITORY': '🇮🇴 British Indian Ocean Territory',
    'BRUNEI DARUSSALAM': '🇧🇳 Brunei Darussalam',
    'BULGARIA': '🇧🇬 Bulgaria',
    'BURKINA FASO': '🇧🇫 Burkina Faso',
    'BURUNDI': '🇧🇮 Burundi',
    'CAMBODIA': '🇰🇭 Cambodia',
    'CAMEROON': '🇨🇲 Cameroon',
    'CANADA': '🇨🇦 Canada',
    'CAPE VERDE': '🇨🇻 Cape Verde',
    'CAYMAN ISLANDS': '🇰🇾 Cayman Islands',
    'CENTRAL AFRICAN REPUBLIC': '🇨🇫 Central African Republic',
    'CHAD': '🇹🇩 Chad',
    'CHILE': '🇨🇱 Chile',
    'CHINA': '🇨🇳 China',
    'CHRISTMAS ISLAND': '🇨🇽 Christmas Island',
    'COCOS (KEELING) ISLANDS': '🇨🇨 Cocos (Keeling) Islands',
    'COLOMBIA': '🇨🇴 Colombia',
    'COMOROS': '🇰🇲 Comoros',
    'CONGO': '🇨🇬 Congo',
    'CONGO, THE DEMOCRATIC REPUBLIC OF THE': '🇨🇩 Congo, The Democratic Republic of the',
    'COOK ISLANDS': '🇨🇰 Cook Islands',
    'COSTA RICA': '🇨🇷 Costa Rica',
    'COTE DIVOIRE': '🇨🇮 Cote D\'Ivoire',
    'CROATIA': '🇭🇷 Croatia',
    'CUBA': '🇨🇺 Cuba',
    'CYPRUS': '🇨🇾 Cyprus',
    'CZECH REPUBLIC': '🇨🇿 Czech Republic',
    'DENMARK': '🇩🇰 Denmark',
    'DJIBOUTI': '🇩🇯 Djibouti',
    'DOMINICA': '🇩🇲 Dominica',
    'DOMINICAN REPUBLIC': '🇩🇴 Dominican Republic',
    'ECUADOR': '🇪🇨 Ecuador',
    'EGYPT': '🇪🇬 Egypt',
    'EL SALVADOR': '🇸🇻 El Salvador',
    'EQUATORIAL GUINEA': '🇬🇶 Equatorial Guinea',
    'ERITREA': '🇪🇷 Eritrea',
    'ESTONIA': '🇪🇪 Estonia',
    'ETHIOPIA': '🇪🇹 Ethiopia',
    'FALKLAND ISLANDS (MALVINAS)': '🇫🇰 Falkland Islands (Malvinas)',
    'FAROE ISLANDS': '🇫🇴 Faroe Islands',
    'FIJI': '🇫🇯 Fiji',
    'FINLAND': '🇫🇮 Finland',
    'FRANCE': '🇫🇷 France',
    'FRENCH GUIANA': '🇬🇫 French Guiana',
    'FRENCH POLYNESIA': '🇵🇫 French Polynesia',
    'FRENCH SOUTHERN TERRITORIES': '🇹🇫 French Southern Territories',
    'GABON': '🇬🇦 Gabon',
    'GAMBIA': '🇬🇲 Gambia',
    'GEORGIA': '🇬🇪 Georgia',
    'GERMANY': '🇩🇪 Germany',
    'GHANA': '🇬🇭 Ghana',
    'GIBRALTAR': '🇬🇮 Gibraltar',
    'GREECE': '🇬🇷 Greece',
    'GREENLAND': '🇬🇱 Greenland',
    'GRENADA': '🇬🇩 Grenada',
    'GUADELOUPE': '🇬🇵 Guadeloupe',
    'GUAM': '🇬🇺 Guam',
    'GUATEMALA': '🇬🇹 Guatemala',
    'GUERNSEY': '🇬🇬 Guernsey',
    'GUINEA': '🇬🇳 Guinea',
    'GUINEA-BISSAU': '🇬🇼 Guinea-Bissau',
    'GUYANA': '🇬🇾 Guyana',
    'HAITI': '🇭🇹 Haiti',
    'HOLY SEE (VATICAN CITY STATE)': '🇻🇦 Holy See (Vatican City State)',
    'HONDURAS': '🇭🇳 Honduras',
    'HONG KONG': '🇭🇰 Hong Kong',
    'HUNGARY': '🇭🇺 Hungary',
    'ICELAND': '🇮🇸 Iceland',
    'INDIA': '🇮🇳 India',
    'INDONESIA': '🇮🇩 Indonesia',
    'IRAN, ISLAMIC REPUBLIC OF': '🇮🇷 Iran, Islamic Republic of',
    'IRAQ': '🇮🇶 Iraq',
    'IRELAND': '🇮🇪 Ireland',
    'ISLE OF MAN': '🇮🇲 Isle of Man',
    'ISRAEL': '🇮🇱 Israel',
    'ITALY': '🇮🇹 Italy',
    'JAMAICA': '🇯🇲 Jamaica',
    'JAPAN': '🇯🇵 Japan',
    'JERSEY': '🇯🇪 Jersey',
    'JORDAN': '🇯🇴 Jordan',
    'KAZAKHSTAN': '🇰🇿 Kazakhstan',
    'KENYA': '🇰🇪 Kenya',
    'KIRIBATI': '🇰🇮 Kiribati',
    'KOREA, DEMOCRATIC PEOPLES REPUBLIC OF': '🇰🇵 Korea, Democratic People\'s Republic of',
    'KOREA, REPUBLIC OF': '🇰🇷 Korea, Republic of',
    'KUWAIT': '🇰🇼 Kuwait',
    'KYRGYZSTAN': '🇰🇬 Kyrgyzstan',
    'LAO PEOPLES DEMOCRATIC REPUBLIC': '🇱🇦 Lao People\'s Democratic Republic',
    'LATVIA': '🇱🇻 Latvia',
    'LEBANON': '🇱🇧 Lebanon',
    'LESOTHO': '🇱🇸 Lesotho',
    'LIBERIA': '🇱🇷 Liberia',
    'LIBYAN ARAB JAMAHIRIYA': '🇱🇾 Libyan Arab Jamahiriya',
    'LIECHTENSTEIN': '🇱🇮 Liechtenstein',
    'LITHUANIA': '🇱🇹 Lithuania',
    'LUXEMBOURG': '🇱🇺 Luxembourg',
    'MACAO': '🇲🇴 Macao',
    'MACEDONIA, THE FORMER YUGOSLAV REPUBLIC OF': '🇲🇰 Macedonia, The Former Yugoslav Republic of',
    'MADAGASCAR': '🇲🇬 Madagascar',
    'MALAWI': '🇲🇼 Malawi',
    'MALAYSIA': '🇲🇾 Malaysia',
    'MALDIVES': '🇲🇻 Maldives',
    'MALI': '🇲🇱 Mali',
    'MALTA': '🇲🇹 Malta',
    'MARSHALL ISLANDS': '🇲🇭 Marshall Islands',
    'MARTINIQUE': '🇲🇶 Martinique',
    'MAURITANIA': '🇲🇷 Mauritania',
    'MAURITIUS': '🇲🇺 Mauritius',
    'MAYOTTE': '🇾🇹 Mayotte',
    'MEXICO': '🇲🇽 Mexico',
    'MICRONESIA, FEDERATED STATES OF': '🇫🇲 Micronesia, Federated States of',
    'MOLDOVA, REPUBLIC OF': '🇲🇩 Moldova, Republic of',
    'MONACO': '🇲🇨 Monaco',
    'MONGOLIA': '🇲🇳 Mongolia',
    'MONTENEGRO': '🇲🇪 Montenegro',
    'MONTSERRAT': '🇲🇸 Montserrat',
    'MOROCCO': '🇲🇦 Morocco',
    'MOZAMBIQUE': '🇲🇿 Mozambique',
    'MYANMAR': '🇲🇲 Myanmar',
    'NAMIBIA': '🇳🇦 Namibia',
    'NAURU': '🇳🇷 Nauru',
    'NEPAL': '🇳🇵 Nepal',
    'NETHERLANDS': '🇳🇱 Netherlands',
    'NETHERLANDS ANTILLES': '🇦🇳 Netherlands Antilles',
    'NEW CALEDONIA': '🇳🇨 New Caledonia',
    'NEW ZEALAND': '🇳🇿 New Zealand',
    'NICARAGUA': '🇳🇮 Nicaragua',
    'NIGER': '🇳🇪 Niger',
    'NIGERIA': '🇳🇬 Nigeria',
    'NIUE': '🇳🇺 Niue',
    'NORFOLK ISLAND': '🇳🇫 Norfolk Island',
    'NORTHERN MARIANA ISLANDS': '🇲🇵 Northern Mariana Islands',
    'NORWAY': '🇳🇴 Norway',
    'OMAN': '🇴🇲 Oman',
    'PAKISTAN': '🇵🇰 Pakistan',
    'PALAU': '🇵🇼 Palau',
    'PALESTINIAN TERRITORY, OCCUPIED': '🇵🇸 Palestinian Territory, Occupied',
    'PANAMA': '🇵🇦 Panama',
    'PAPUA NEW GUINEA': '🇵🇬 Papua New Guinea',
    'PARAGUAY': '🇵🇾 Paraguay',
    'PERU': '🇵🇪 Peru',
    'PHILIPPINES': '🇵🇭 Philippines',
    'PITCAIRN': '🇵🇳 Pitcairn',
    'POLAND': '🇵🇱 Poland',
    'PORTUGAL': '🇵🇹 Portugal',
    'PUERTO RICO': '🇵🇷 Puerto Rico',
    'QATAR': '🇶🇦 Qatar',
    'REUNION': '🇷🇪 Reunion',
    'ROMANIA': '🇷🇴 Romania',
    'RUSSIAN FEDERATION': '🇷🇺 Russian Federation',
    'RWANDA': '🇷🇼 Rwanda',
    'SAINT BARTHELEMY': '🇧🇱 Saint Barthelemy',
    'SAINT HELENA': '🇸🇭 Saint Helena',
    'SAINT KITTS AND NEVIS': '🇰🇳 Saint Kitts and Nevis',
    'SAINT LUCIA': '🇱🇨 Saint Lucia',
    'SAINT MARTIN': '🇲🇫 Saint Martin',
    'SAINT PIERRE AND MIQUELON': '🇵🇲 Saint Pierre and Miquelon',
    'SAINT VINCENT AND THE GRENADINES': '🇻🇨 Saint Vincent and the Grenadines',
    'SAMOA': '🇼🇸 Samoa',
    'SAN MARINO': '🇸🇲 San Marino',
    'SAO TOME AND PRINCIPE': '🇸🇹 Sao Tome and Principe',
    'SAUDI ARABIA': '🇸🇦 Saudi Arabia',
    'SENEGAL': '🇸🇳 Senegal',
    'SERBIA': '🇷🇸 Serbia',
    'SEYCHELLES': '🇸🇨 Seychelles',
    'SIERRA LEONE': '🇸🇱 Sierra Leone',
    'SINGAPORE': '🇸🇬 Singapore',
    'SLOVAKIA': '🇸🇰 Slovakia',
    'SLOVENIA': '🇸🇮 Slovenia',
    'SOLOMON ISLANDS': '🇸🇧 Solomon Islands',
    'SOMALIA': '🇸🇴 Somalia',
    'SOUTH AFRICA': '🇿🇦 South Africa',
    'SOUTH GEORGIA AND THE SOUTH SANDWICH ISLANDS': '🇬🇸 South Georgia and the South Sandwich Islands',
    'SPAIN': '🇪🇸 Spain',
    'SRI LANKA': '🇱🇰 Sri Lanka',
    'SUDAN': '🇸🇩 Sudan',
    'SURINAME': '🇸🇷 Suriname',
    'SVALBARD AND JAN MAYEN': '🇸🇯 Svalbard and Jan Mayen',
    'SWAZILAND': '🇸🇿 Swaziland',
    'SWEDEN': '🇸🇪 Sweden',
    'SWITZERLAND': '🇨🇭 Switzerland',
    'SYRIAN ARAB REPUBLIC': '🇸🇾 Syrian Arab Republic',
    'TAIWAN, PROVINCE OF CHINA': '🇹🇼 Taiwan, Province of China',
    'TAJIKISTAN': '🇹🇯 Tajikistan',
    'TANZANIA, UNITED REPUBLIC OF': '🇹🇿 Tanzania, United Republic of',
    'THAILAND': '🇹🇭 Thailand',
    'TIMOR-LESTE': '🇹🇱 Timor-Leste',
    'TOGO': '🇹🇬 Togo',
    'TOKELAU': '🇹🇰 Tokelau',
    'TONGA': '🇹🇴 Tonga',
    'TRINIDAD AND TOBAGO': '🇹🇹 Trinidad and Tobago',
    'TUNISIA': '🇹🇳 Tunisia',
    'TURKEY': '🇹🇷 Turkey',
    'TURKMENISTAN': '🇹🇲 Turkmenistan',
    'TURKS AND CAICOS ISLANDS': '🇹🇨 Turks and Caicos Islands',
    'TUVALU': '🇹🇻 Tuvalu',
    'UGANDA': '🇺🇬 Uganda',
    'UKRAINE': '🇺🇦 Ukraine',
    'UNITED ARAB EMIRATES': '🇦🇪 United Arab Emirates',
    'UNITED KINGDOM': '🇬🇧 United Kingdom',
    'UNITED STATES': '🇺🇸 United States',
    'URUGUAY': '🇺🇾 Uruguay',
    'UZBEKISTAN': '🇺🇿 Uzbekistan',
    'VANUATU': '🇻🇺 Vanuatu',
    'VENEZUELA': '🇻🇪 Venezuela',
    'VIET NAM': '🇻🇳 Viet Nam',
    'VIRGIN ISLANDS, BRITISH': '🇻🇬 Virgin Islands, British',
    'VIRGIN ISLANDS, U.S.': '🇻🇮 Virgin Islands, U.S.',
    'WALLIS AND FUTUNA': '🇼🇫 Wallis and Futuna',
    'WESTERN SAHARA': '🇪🇭 Western Sahara',
    'YEMEN': '🇾🇪 Yemen',
    'ZAMBIA': '🇿🇲 Zambia',
    'ZIMBABWE': '🇿🇼 Zimbabwe',
    'Unknown': '❓ Unknown'
}

def detect_country_from_number(phone_number):
    """Fast country detection using optimized lookup."""
    if not phone_number:
        return 'Unknown'
    
    phone_str = str(phone_number).strip()
    clean_number = phone_str.replace('+', '').replace('-', '').replace(' ', '').lstrip('0')
    
    # Check exact matches first
    for code in sorted(COUNTRY_CODES.keys(), key=len, reverse=True):
        if phone_str.startswith(code) or clean_number.startswith(code.replace('+', '')):
            return COUNTRY_CODES[code]
    
    # Quick common country checks
    if clean_number.startswith('1'): return 'US'
    elif clean_number.startswith('44'): return 'UK'
    elif clean_number.startswith('92'): return 'PK'
    elif clean_number.startswith('91'): return 'IN'
    elif clean_number.startswith('86'): return 'CN'
    elif clean_number.startswith('81'): return 'JP'
    elif clean_number.startswith('82'): return 'KR'
    elif clean_number.startswith('62'): return 'INDONESIA'
    elif clean_number.startswith('63'): return 'PHILIPPINES'
    elif clean_number.startswith('261'): return 'MADAGASCAR'
    elif clean_number.startswith('20'): return 'EGYPT'
    elif clean_number.startswith('234'): return 'NIGERIA'
    elif clean_number.startswith('852'): return 'HONG_KONG'
    elif clean_number.startswith('375'): return 'BELARUS'
    elif clean_number.startswith('228'): return 'TOGO'
    elif clean_number.startswith('225'): return 'IVORY_COAST'
    elif clean_number.startswith('93'): return 'AFGHANISTAN'
    elif clean_number.startswith('51'): return 'PERU'
    elif clean_number.startswith('593'): return 'ECUADOR'
    elif clean_number.startswith('591'): return 'BOLIVIA'
    elif clean_number.startswith('1876'): return 'JAMAICA'
    elif clean_number.startswith('224'): return 'GUINEA'
    elif clean_number.startswith('977'): return 'NEPAL'
    elif clean_number.startswith('212'): return 'MOROCCO'
    elif clean_number.startswith('996'): return 'KYRGYZSTAN'
    elif clean_number.startswith('229'): return 'BENIN'
    elif clean_number.startswith('251'): return 'ETHIOPIA'
    elif clean_number.startswith('880'): return 'BANGLADESH'
    elif clean_number.startswith('968'): return 'OMAN'
    elif clean_number.startswith('7'): return 'RU'
    elif clean_number.startswith('49'): return 'DE'
    elif clean_number.startswith('33'): return 'FR'
    
    return 'Unknown'

# Global variables
shutdown_event = asyncio.Event()
bot_thread = None
manager_instance = None
json_lock = threading.Lock()

# Smart caching and performance optimization for 1k+ users
class SmartCache:
    """Intelligent caching system for better performance."""
    
    def __init__(self):
        self._user_cache = {}
        self._country_cache = {}
        self._cache_timestamps = {}
        self._cache_ttl = 300  # 5 minutes
    
    def get_user_data(self, user_id: str):
        """Get user data with smart caching."""
        current_time = time.time()
        
        if (user_id in self._user_cache and 
            current_time - self._cache_timestamps.get(user_id, 0) < self._cache_ttl):
            return self._user_cache[user_id]
        
        # Load from file and cache
        users_data = load_json_data(USERS_FILE, {})
        user_data = users_data.get(user_id, {})
        
        self._user_cache[user_id] = user_data
        self._cache_timestamps[user_id] = current_time
        
        return user_data
    
    def clear_cache(self):
        """Clear all caches."""
        self._user_cache.clear()
        self._country_cache.clear()
        self._cache_timestamps.clear()

# Global smart cache instance
smart_cache = SmartCache()

# Setup logging (Changed to ERROR for minor logging)
logging.basicConfig(filename='bot_error.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')

# Enhanced balance tracking and user management
class UserManager:
    """Smart user management for handling 1k+ members efficiently."""
    
    @staticmethod
    def get_user_stats():
        """Get comprehensive user statistics."""
        users_data = load_json_data(USERS_FILE, {})
        
        total_users = len(users_data)
        active_users = sum(1 for user in users_data.values() if user.get('phone_numbers'))
        users_with_balance = sum(1 for user in users_data.values() if user.get('balance', 0) > 0)
        total_balance = sum(user.get('balance', 0) for user in users_data.values())
        total_referrals = sum(user.get('referrals', 0) for user in users_data.values())
        
        return {
            'total_users': total_users,
            'active_users': active_users,
            'users_with_balance': users_with_balance,
            'total_balance': total_balance,
            'total_referrals': total_referrals,
            'average_balance': total_balance / total_users if total_users > 0 else 0
        }
    
    @staticmethod
    def update_user_balance(user_id: str, amount: float, reason: str = "SMS"):
        """Update user balance with transaction logging."""
        users_data = load_json_data(USERS_FILE, {})
        
        if user_id not in users_data:
            return False
        
        user_data = users_data[user_id]
        old_balance = user_data.get('balance', 0)
        new_balance = old_balance + amount
        
        user_data['balance'] = new_balance
        
        # Add transaction log
        if 'transactions' not in user_data:
            user_data['transactions'] = []
        
        user_data['transactions'].append({
            'timestamp': datetime.now().isoformat(),
            'type': 'credit',
            'amount': amount,
            'reason': reason,
            'old_balance': old_balance,
            'new_balance': new_balance
        })
        
        # Keep only last 50 transactions
        user_data['transactions'] = user_data['transactions'][-50:]
        
        users_data[user_id] = user_data
        save_json_data(USERS_FILE, users_data)
        
        logging.info(f"Updated balance for user {user_id}: {old_balance:.4f} -> {new_balance:.4f} ({reason})")
        return True

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

def extract_otp_from_text(text):
    if not text: return "N/A"
    patterns = [
        r'<#>\s*(\d{8})\s',  # Facebook 8-digit: <#> 56053095 es tu código...
        r'(\d{8})',          # General 8-digit
        r'G-(\d{6})',
        r'code is\s*(\d+)',
        r'code:\s*(\d+)', 
        r'verification code[:\s]*(\d+)', 
        r'OTP is\s*(\d+)', 
        r'pin[:\s]*(\d+)',
        r'(\d{6})',
        r'(\d{4,7})'         # Catch 4-7 digits
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            otp = match.group(1)
            if 4 <= len(otp) <= 8 and otp.isdigit():
                return otp
    fallback_match = re.search(r'\b(\d{4,8})\b', text) # Fallback for 4-8 digits
    return fallback_match.group(1) if fallback_match else "N/A"

def blockquote_format(text: str) -> str:
    """Formats each line of a message with <blockquote> tags."""
    if not text:
        return ""
    lines = text.split('\n')
    formatted_lines = [f"<blockquote>{line}</blockquote>" for line in lines if line.strip()]
    return '\n\n'.join(formatted_lines)

def html_escape(text):
    return str(text).replace('<', '&lt;').replace('>', '&gt;')


def get_number_formats(phone_number_str):
    """Returns a tuple of (with_country_code, without_country_code)."""
    if not phone_number_str:
        return "N/A", "N/A"

    # Normalize input
    num = re.sub(r'\D', '', phone_number_str)

    # Find country code
    found_code = ''
    for code in sorted(COUNTRY_CODES.keys(), key=len, reverse=True):
        code_digits = code.replace('+', '')
        if num.startswith(code_digits):
            found_code = code_digits
            break
    
    if found_code:
        with_cc = f"+{num}"
        without_cc = num[len(found_code):]
        return with_cc, without_cc
    else:
        # Fallback if no code found
        return f"+{num}", num

# --- IvaSmsManager Class (Web Scraping) ---
class IvaSmsManager:
    _instance = None
    _sms_driver = None
    _is_initialized = False
    _numbers_scraped = False  # Flag to track if numbers have been scraped
    _lock = threading.Lock() # Lock for file operations
    _application = None
    _loop = None

    def set_application(self, application):
        self._application = application

    def set_loop(self, loop):
        self._loop = loop
        
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(IvaSmsManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._is_initialized:
            self._initialize_drivers()
            self._last_refresh_time = time.time()
    
    def _create_driver(self):
        """Create SeleniumBase driver with Cloudflare bypass."""
        max_attempts = 3
        
        # --- FIX: Use an absolute path in the user's home directory for permissions ---
        try:
            home_dir = os.path.expanduser('~')
            # Create a dedicated directory for the bot data in the home folder
            data_dir = os.path.join(home_dir, 'pg_otp_bot_data', 'sms_driver_data')
            
            # Ensure the directory exists with the correct permissions
            os.makedirs(data_dir, exist_ok=True)
            
            logging.info(f"Using data directory: {data_dir}")
            
        except Exception as e:
            logging.error(f"Failed to create data directory at {os.path.join(home_dir, 'pg_otp_bot_data')}: {e}")
            # Fallback to a relative path if home dir fails for some reason
            data_dir = './sms_driver_data_fallback'
            os.makedirs(data_dir, exist_ok=True)
            logging.info(f"Falling back to relative path: {data_dir}")
        # --- End of fix ---

        for attempt in range(max_attempts):
            try:
                # Initialize SeleniumBase driver with UC mode for Cloudflare bypass
                # Add a persistent user_data_dir for the SMS driver
                driver = Driver(
                    uc=True, 
                    headless2=True, # <-- USE headless2 (replaces headless=True and args)
                    user_data_dir=data_dir # Use the robust, absolute path
                )
                logging.info(f"✅ Created SeleniumBase driver with persistent session at {data_dir}")
                return driver
            except Exception as e:
                logging.error(f"Driver creation attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_attempts - 1:
                    time.sleep(3)
                else:
                    raise
    
    def _initialize_drivers(self):
        # Initialize SMS driver
        self._sms_driver = self._create_driver()
        self._is_initialized = True
        self._setup_driver(self._sms_driver, "https://www.ivasms.com/portal/live/my_sms")
        logging.info("Number scraping at startup is disabled. Numbers must be managed manually via commands.")


    def _login_driver(self, driver):
        """Login to ivasms.com using SeleniumBase UC mode."""
        try:
            logging.info("🔐 Logging in to ivasms.com with Cloudflare bypass...")
            # Use SeleniumBase UC mode to bypass Cloudflare
            driver.uc_open_with_reconnect("https://www.ivasms.com/login", reconnect_time=6)
            time.sleep(3)
            
            # Check if already logged in by looking for portal URL
            if "portal" in driver.current_url:
                logging.info("✅ Already logged in (session restored)")
                return
            
            wait = WebDriverWait(driver, 20)
            
            email_field = wait.until(EC.visibility_of_element_located((By.NAME, "email")))
            email_field.clear()
            email_field.send_keys(EMAIL)
            time.sleep(1)
            
            password_field = driver.find_element(By.NAME, "password")
            password_field.clear()
            password_field.send_keys(PASSWORD)
            time.sleep(1)
            
            login_button = driver.find_element(By.TAG_NAME, "button")
            login_button.click()
            
            wait.until(EC.url_contains("portal"))
            time.sleep(2)
            
            logging.info("✅ Login completed with Cloudflare bypass")
            
        except Exception as e:
            logging.error(f"❌ Login failed: {e}")
            raise

    def _click_tutorial_popups(self, driver):
        """Click tutorial popups if they appear."""
        for _ in range(5):
            try:
                wait = WebDriverWait(driver, 3)
                next_button = wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.driver-popover-next-btn"))
                )
                driver.execute_script("arguments[0].click();", next_button)
                time.sleep(0.5)
            except Exception:
                break

    def _setup_driver(self, driver, url):
        try:
            self._login_driver(driver)
            # Use SeleniumBase UC mode for the target URL as well
            driver.uc_open_with_reconnect(url, reconnect_time=6)
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            self._click_tutorial_popups(driver)
        except Exception as e:
            logging.error(f"Failed to setup driver for {url}: {e}")
            raise

    def refresh_sms_page(self):
        """Refresh SMS page every 20 minutes."""
        try:
            current_time = time.time()
            time_since_refresh = current_time - self._last_refresh_time
            
            if time_since_refresh >= 1200:
                logging.info(f"🔄 Refreshing SMS page after {time_since_refresh/60:.1f} minutes")
                
                if self._application and self._loop:
                    asyncio.run_coroutine_threadsafe(
                        self._application.bot.send_message(
                            chat_id=GROUP_ID,
                            text="এসএমএস পেঁজ এখন রেফেশ হবে দয়া করে কাজ বন্ধ করুন নয়তো কোড মিস যেতে পারে ।"
                        ),
                        self._loop
                    )
                    time.sleep(5)

                self._sms_driver.uc_open_with_reconnect("https://www.ivasms.com/portal/live/my_sms", reconnect_time=6)
                WebDriverWait(self._sms_driver, 20).until(EC.presence_of_element_located((By.ID, "LiveTestSMS")))
                
                self._last_refresh_time = current_time
                logging.info("✅ SMS page refreshed")
            else:
                logging.debug(f"SMS page refresh not needed yet. Next refresh in {(1200 - time_since_refresh)/60:.1f} minutes")
                
        except Exception as e:
            logging.error(f"Failed to refresh SMS page: {e}")
            try:
                self._setup_driver(self._sms_driver, "https://www.ivasms.com/portal/live/my_sms")
                self._last_refresh_time = time.time()
                logging.info("✅ SMS driver setup after page refresh failure")
            except Exception as fallback_e:
                logging.error(f"Failed to setup SMS driver after page refresh failure: {fallback_e}")

    def scrape_and_save_all_sms(self):
        # Refresh SMS page if needed before scraping
        self.refresh_sms_page()
        
        try:
            WebDriverWait(self._sms_driver, 20).until(EC.presence_of_element_located((By.ID, "LiveTestSMS")))
            soup = BeautifulSoup(self._sms_driver.page_source, "html.parser")
            table_body = soup.select_one("tbody#LiveTestSMS")
            if not table_body:
                return
            
            sms_list = []
            for row in table_body.select("tr"):
                try:
                    phone_elem = row.select_one("p.CopyText.text-500")
                    country_raw_elem = row.select_one("h6.mb-1.fw-semi-bold a")
                    provider_elem = row.select_one("div.fw-semi-bold.ms-2")
                    message_elem = row.select("td.align-middle.text-end.fw-semi-bold")

                    if phone_elem and country_raw_elem and provider_elem and message_elem:
                        phone = phone_elem.get_text(strip=True)
                        country_raw = country_raw_elem.get_text(strip=True)
                        country = re.sub(r'\d', '', country_raw).strip()
                        provider = provider_elem.get_text(strip=True)
                        message = message_elem[-1].get_text(strip=True)
                        sms_list.append({'country': country, 'provider': provider, 'message': message, 'phone': phone})
                except Exception as e:
                    logging.warning(f"Could not parse an SMS row: {e}")

            with open(SMS_CACHE_FILE, 'w', encoding='utf-8') as f:
                for sms in sms_list:
                    f.write(json.dumps(sms) + "\n")
        except Exception as e:
            logging.error(f"SMS scraping failed: {e}")
            self._setup_driver(self._sms_driver, "https://www.ivasms.com/portal/live/my_sms")



    def get_available_countries(self):
        """Get available countries with counts."""
        countries = []
        country_counts = {}
        
        with self._lock:
            if not os.path.exists(NUMBERS_FILE):
                return countries
                
            with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
                users_data = load_json_data(USERS_FILE, {})
                assigned_numbers = set()
                for user in users_data.values():
                    assigned_numbers.update(user.get("phone_numbers", []))
                
                for line in f:
                    number = line.strip()
                    if number and number not in assigned_numbers:
                        country = detect_country_from_number(number)
                        country_counts[country] = country_counts.get(country, 0) + 1
        
        for country, count in country_counts.items():
            if count > 0:
                countries.append({'name': country, 'count': count})
        
        return sorted(countries, key=lambda x: x['count'], reverse=True)

    def assign_number_to_user(self, user_id: str, country: str = None) -> dict:
        """Smart number assignment with enhanced tracking."""
        users_data = load_json_data(USERS_FILE, {})
        
        if user_id not in users_data:
            return {'success': False, 'error': 'User not found'}
        
        user_data = users_data[user_id]
        
        # Get number from file
        number = self.get_number_from_file(country)
        
        if not number:
            return {'success': False, 'error': 'No numbers available'}
        
        # Update user data
        if 'phone_numbers' not in user_data:
            user_data['phone_numbers'] = []
        
        user_data['phone_numbers'].append(number)
        user_data['phone_numbers'] = user_data['phone_numbers'][-10:]  # Keep last 10
        user_data['last_number_time'] = time.time()
        
        # Add assignment log
        if 'number_assignments' not in user_data:
            user_data['number_assignments'] = []
        
        user_data['number_assignments'].append({
            'timestamp': datetime.now().isoformat(),
            'number': number,
            'country': country or 'Any',
            'total_assigned': len(user_data['phone_numbers'])
        })
        
        # Keep only last 20 assignments
        user_data['number_assignments'] = user_data['number_assignments'][-20:]
        
        users_data[user_id] = user_data
        save_json_data(USERS_FILE, users_data)
        
        logging.info(f"Assigned number {number} to user {user_id} (country: {country})")
        
        return {
            'success': True,
            'number': number,
            'country': country or detect_country_from_number(number),
            'total_numbers': len(user_data['phone_numbers'])
        }
        
    def get_number_from_file(self, country=None):
        """Gets one number from numbers.txt for specified country using atomic file operations."""
        with self._lock:
            if not os.path.exists(NUMBERS_FILE):
                return None
            
            # Use temporary file for atomic operation
            temp_file = f"{NUMBERS_FILE}.tmp"
            number_to_give = None
            
            try:
                # Read original file and write to temp file
                with open(NUMBERS_FILE, 'r', encoding='utf-8') as original:
                    with open(temp_file, 'w', encoding='utf-8') as temp:
                        for line in original:
                            number = line.strip()
                            if not number:
                                continue
                                
                            # Check if this is the number we want to assign
                            if number_to_give is None:
                                if country:
                                    detected_country = detect_country_from_number(number)
                                    if detected_country == country:
                                        number_to_give = number
                                        continue  # Skip writing this number to temp file
                                else:
                                    number_to_give = number
                                    continue  # Skip writing this number to temp file
                            
                            # Write remaining numbers to temp file
                            temp.write(f"{number}\n")
                
                # Atomic replacement: temp file becomes the new numbers file
                if number_to_give:
                    os.replace(temp_file, NUMBERS_FILE)
                    logging.info(f"Assigned number {number_to_give} for country {country}")
                else:
                    # No number found, remove temp file
                    os.remove(temp_file)
                    if country:
                        logging.warning(f"No numbers available for country {country}")
                    else:
                        logging.warning("No numbers available")
                
                return number_to_give
                
            except Exception as e:
                # Clean up temp file on error
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                logging.error(f"Error in get_number_from_file: {e}")
                return None

    def get_available_number_count(self):
        """Get count of available numbers."""
        with self._lock:
            if not os.path.exists(NUMBERS_FILE):
                return 0
            with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
                return len([line for line in f if line.strip()])
    
    def cleanup(self):
        """Clean up SeleniumBase driver instances."""
        logging.info("Cleaning up SeleniumBase driver instances.")
        try:
            if self._sms_driver:
                self._sms_driver.quit()
                logging.info("SMS driver quit successfully.")
        except Exception as e:
            logging.error(f"Error quitting SMS driver: {e}")
            
        self._sms_driver = None
        logging.info("SeleniumBase driver cleanup complete.")

# --- Telegram Bot UI and Logic ---

def get_main_menu_keyboard():
    keyboard = [
        [KeyboardButton("🎁 Get Number")],
        [KeyboardButton("👤 Account"), KeyboardButton("💰 Balance")],
        [KeyboardButton("💸 Withdraw"), KeyboardButton("📋 Withdraw History")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = str(user.id)
    users_data = load_json_data(USERS_FILE, {})

    if user_id not in users_data:
        referred_by = context.args[0] if context.args and context.args[0].isdigit() and context.args[0] != user_id else None
        
        users_data[user_id] = {
            "username": user.username, "first_name": user.first_name, "phone_numbers": [],
            "balance": 0, "referrals": 0, "referred_by": referred_by,
            "last_number_time": 0, "withdraw_history": []
        }
        
        if referred_by and referred_by in users_data:
            users_data[referred_by]['referrals'] = users_data[referred_by].get('referrals', 0) + 1
    
    else:
        # User exists, update their name and username
        users_data[user_id]['username'] = user.username
        users_data[user_id]['first_name'] = user.first_name
        logging.info(f"Updated user info for {user_id}")

    save_json_data(USERS_FILE, users_data)
    
    # Enhanced welcome message with better formatting
    user_balance = users_data.get(user_id, {}).get('balance', 0)
    user_numbers = len(users_data.get(user_id, {}).get('phone_numbers', []))
    current_time = datetime.now().strftime('%H:%M:%S')
    
    welcome_text = (
        f"<b>🎉 Welcome!</b>\n\n"
        f"<blockquote>👤 <b>User:</b> {user.first_name or 'Anonymous'}\n"
        f"💰 <b>Balance:</b> ৳{user_balance:.2f}\n"
        f"📱 <b>Numbers:</b> {user_numbers}/10\n"
        f"⏰ <b>Time:</b> {current_time}</blockquote>\n\n"
        f"<b>🚀 Ready to earn money?</b> Click any button below to get started!"
    )

    if update.callback_query:
        await update.callback_query.message.delete()
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=welcome_text,
            reply_markup=get_main_menu_keyboard(),
            parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text(
            welcome_text,
            reply_markup=get_main_menu_keyboard(),
            parse_mode=ParseMode.HTML
        )

async def safe_answer_callback(query, text="", show_alert=False):
    """Safely answer callback query."""
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


async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_callback(query)

    user_id = str(query.from_user.id)
    
    if query.data == 'main_menu':
        await start_command(update, context)
        return

    elif query.data.startswith('withdraw_method_'):
        method = query.data.split('_', 2)[2]
        
        min_withdraw = BINANCE_MINIMUM_WITHDRAWAL if method == 'Binance' else WITHDRAWAL_LIMIT
        
        users_data = load_json_data(USERS_FILE, {})
        balance = users_data.get(user_id, {}).get('balance', 0)

        if balance < min_withdraw:
            await query.answer(f"⚠️ Insufficient balance. You need at least ৳{min_withdraw:.2f} to withdraw via {method}.", show_alert=True)
            return

        context.user_data['withdrawal_method'] = method
        context.user_data['state'] = 'AWAITING_WITHDRAWAL_AMOUNT'
        
        cancel_button = InlineKeyboardButton("❌ Cancel", callback_data='cancel_withdrawal')
        keyboard = InlineKeyboardMarkup([[cancel_button]])
        
        await query.edit_message_text(
            text=f"<b>💸 Withdrawing via {method}.</b>\n\n"
                 f"<blockquote>Please enter the amount you want to withdraw.\n"
                 f"The minimum for this method is ৳{min_withdraw:.2f}.</blockquote>\n\n"
                 f"<i>Click Cancel to abort the withdrawal process.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

    elif query.data.startswith('admin_accept_'):
        target_user_id = query.data.split('_')[2]
        await query.edit_message_text(f"<b>✅ Payment Approved. (User ID: {target_user_id})</b>", parse_mode=ParseMode.HTML)
        await context.bot.send_message(chat_id=target_user_id, text="<b>🎉 Congratulations! Your withdrawal request has been approved.</b>", parse_mode=ParseMode.HTML)

    elif query.data.startswith('admin_decline_'):
        target_user_id = query.data.split('_')[2]
        await query.edit_message_text(f"<b>❌ Payment Declined. (User ID: {target_user_id})</b>", parse_mode=ParseMode.HTML)
        await context.bot.send_message(chat_id=target_user_id, text="<b>😔 Sorry! Your withdrawal request has been declined.</b>", parse_mode=ParseMode.HTML)

    elif query.data == 'cancel_withdrawal':
        context.user_data['state'] = None
        context.user_data['withdrawal_method'] = None
        context.user_data['withdrawal_amount'] = None
        await query.edit_message_text(
            text="<b>❌ Withdrawal cancelled.</b>\n\n<blockquote>You can start a new withdrawal anytime.</blockquote>",
            parse_mode=ParseMode.HTML
        )

    elif query.data.startswith('country_'):
        country = query.data.split('_', 1)[1]
        await handle_country_selection(update, context, country)

    elif query.data == 'cancel_country_selection':
        context.user_data['state'] = None
        await query.edit_message_text(
            text="<b>❌ Country selection cancelled.</b>\n\n<blockquote>You can try again anytime.</blockquote>",
            parse_mode=ParseMode.HTML
        )
    

# --- Handlers for ReplyKeyboard buttons ---

async def handle_country_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, selected_country: str):
    """Enhanced country selection with better user experience."""
    user_id = str(update.effective_user.id)
    
    # Use the new assign_number_to_user method
    result = await asyncio.to_thread(manager_instance.assign_number_to_user, user_id, selected_country)
    
    if result['success']:
        # Reset state
        context.user_data['state'] = None
        
        country_display = COUNTRY_FLAGS.get(selected_country, f"🌍 {selected_country}")
        # Provide both international (with country code) and plain (no country code) variants
        with_cc, without_cc = get_number_formats(result['number'])
        
        # Enhanced number assignment message with better formatting
        users_data = load_json_data(USERS_FILE, {})
        user_data = users_data.get(update.effective_user.id, {})
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("otp group", url=GROUP_LINK)]
        ])

        await update.callback_query.edit_message_text(
            text=f"<b>{selected_country} NUMBER ASSIGNED! ✨</b>\n\n"
                 f"<blockquote><b>🌍 Country:</b> {html_escape(country_display)}</blockquote>\n\n"
                 f"<blockquote><b>☎️ Number:</b> <code>{html_escape(with_cc)}</code></blockquote>\n\n"
                 f"<blockquote><b>📋 Without CC:</b> <code>{html_escape(without_cc)}</code></blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
            disable_web_page_preview=True
        )
    else:
        country_display = COUNTRY_FLAGS.get(selected_country, f"🌍 {selected_country}")
        
        # Get available countries and show them
        available_countries = await asyncio.to_thread(manager_instance.get_available_countries)
        country_list = []
        for country in available_countries:
            country_name = country['name']
            country_flag = COUNTRY_FLAGS.get(country_name, f"🌍 {country_name}")
            country_list.append(f"{country_flag} ({country['count']})")
        
        error_msg = result.get('error', 'Unknown error')
        
        await update.callback_query.edit_message_text(
            text=f"<b>❌ Assignment Failed</b>\n\n"
                 f"<blockquote>🌍 <b>Requested:</b> {country_display}\n"
                 f"⚠️ <b>Error:</b> {error_msg}</blockquote>\n\n"
                 f"<b>📱 Available Countries:</b>\n"
                 f"<blockquote>{chr(10).join(country_list)}</blockquote>\n\n"
                 f"<i>Click 'Get Number' again to try another country.</i>",
            parse_mode=ParseMode.HTML
        )

async def handle_get_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced number getting with better user experience."""
    # Reset any withdrawal state when getting a number
    context.user_data['state'] = None
    context.user_data['withdrawal_method'] = None
    context.user_data['withdrawal_amount'] = None
    
    user_id = str(update.effective_user.id)
    users_data = load_json_data(USERS_FILE, {})
    user_data = users_data.get(user_id)
    
    if 'phone_numbers' not in user_data or not isinstance(user_data['phone_numbers'], list):
        user_data['phone_numbers'] = []

    cooldown = 5
    last_time = user_data.get('last_number_time', 0)
    current_time = time.time()

    if current_time - last_time < cooldown:
        remaining_time = int(cooldown - (current_time - last_time))
        await update.message.reply_text(
            f"<b>⏰ Please Wait</b>\n\n"
            f"<blockquote>🕐 <b>Cooldown:</b> {remaining_time} seconds remaining\n"
            f"📱 <b>Your Numbers:</b> {len(user_data['phone_numbers'])}/10\n"
            f"💰 <b>Your Balance:</b> ৳{user_data.get('balance', 0):.2f}</blockquote>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Get available countries
    countries = await asyncio.to_thread(manager_instance.get_available_countries)
    
    if not countries:
        await update.message.reply_text(
            "<b>😔 No Numbers Available</b>\n\n"
            "<blockquote>📱 All numbers are currently assigned\n"
            f"💰 <b>Your Balance:</b> ৳{user_data.get('balance', 0):.2f}\n"
            f"📊 <b>Your Numbers:</b> {len(user_data['phone_numbers'])}/10</blockquote>\n\n"
            "<i>Please try again later or contact admin.</i>",
            parse_mode=ParseMode.HTML
        )
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"<b>⚠️ ADMIN ALERT: Bot out of numbers!</b>\n\n"
                     f"📊 <b>Total Users:</b> {len(users_data)}\n"
                     f"📱 <b>Active Users:</b> {sum(1 for u in users_data.values() if u.get('phone_numbers'))}\n"
                     f"💰 <b>Total Balance:</b> ৳{sum(u.get('balance', 0) for u in users_data.values()):.2f}",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logging.error(f"Failed to send out-of-stock notification to admin: {e}")
        return
    
    # Show country selection with enhanced UI
    context.user_data['state'] = 'SELECTING_COUNTRY'
    
    keyboard_buttons = []
    for country in countries:
        country_name = country['name']
        country_display = COUNTRY_FLAGS.get(country_name, f"🌍 {country_name}")
        button_text = f"{country_display} ({country['count']})"
        button = InlineKeyboardButton(button_text, callback_data=f"country_{country_name}")
        keyboard_buttons.append([button])
    
    cancel_button = InlineKeyboardButton("❌ Cancel", callback_data='cancel_country_selection')
    keyboard_buttons.append([cancel_button])
    
    reply_markup = InlineKeyboardMarkup(keyboard_buttons)
    
    await update.message.reply_text(
        f"<b>🌍 Choose Your Country</b>\n\n"
        f"<blockquote>📱 <b>Your Numbers:</b> {len(user_data['phone_numbers'])}/10\n"
        f"💰 <b>Your Balance:</b> ৳{user_data.get('balance', 0):.2f}\n"
        f"📊 <b>Available Countries:</b> {len(countries)}</blockquote>\n\n"
        f"<i>Select a country to get a number from that region.</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup
    )


async def handle_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    users_data = load_json_data(USERS_FILE, {})
    user_data = users_data.get(user_id)
    
    bot_username = (await context.bot.get_me()).username
    referral_link = f"https://t.me/{bot_username}?start={user_id}"
    account_text = (
        f"<b>👤 Your Account</b>\n\n"
        f"<blockquote><b>Name:</b> {html_escape(user_data.get('first_name'))}\n"
        f"<b>Total Referrals:</b> {user_data.get('referrals', 0)}</blockquote>\n"
        f"<b>🔗 Your Referral Link:</b>\n<blockquote><code>{referral_link}</code></blockquote>\n"
        f"<b>Share this link. You will receive a {REFERRAL_WITHDRAWAL_BONUS_PERCENT}% bonus when your referred user withdraws.</b>"
    )
    await update.message.reply_text(account_text, parse_mode=ParseMode.HTML)

async def handle_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced balance display with detailed information."""
    user_id = str(update.effective_user.id)
    users_data = load_json_data(USERS_FILE, {})
    user_data = users_data.get(user_id, {})
    
    balance = user_data.get('balance', 0)
    phone_count = len(user_data.get('phone_numbers', []))
    referrals = user_data.get('referrals', 0)
    
    # Get recent transactions
    transactions = user_data.get('transactions', [])
    recent_transactions = transactions[-5:] if transactions else []
    
    # Enhanced balance display with better formatting
    current_time = datetime.now().strftime('%H:%M:%S')
    potential_earnings = phone_count * SMS_AMOUNT * 10  # Estimate for 10 messages per number
    
    balance_text = (
        f"<b>💰 Balance Dashboard</b>\n\n"
        f"<blockquote>💵 <b>Current Balance:</b> ৳{balance:.2f}\n"
        f"📱 <b>Active Numbers:</b> {phone_count}/10\n"
        f"👥 <b>Total Referrals:</b> {referrals}\n"
        f"🎯 <b>Referral Bonus:</b> {REFERRAL_WITHDRAWAL_BONUS_PERCENT}%\n"
        f"⏰ <b>Last Updated:</b> {current_time}</blockquote>\n\n"
        f"<b>📈 Earnings Potential:</b>\n"
        f"<blockquote>💡 <b>Per OTP:</b> ৳{SMS_AMOUNT:.4f}\n"
        f"🚀 <b>Estimated Daily:</b> ৳{potential_earnings:.2f}</blockquote>\n\n"
    )
    
    if recent_transactions:
        balance_text += f"<b>📊 Recent Activity:</b>\n"
        for tx in reversed(recent_transactions):
            timestamp = datetime.fromisoformat(tx['timestamp']).strftime("%m-%d %H:%M")
            tx_type = "➕" if tx['type'] == 'credit' else "➖"
            balance_text += f"<blockquote>{tx_type} ৳{tx['amount']:.4f} ({tx['reason']}) - {timestamp}</blockquote>"
    
    await update.message.reply_text(balance_text, parse_mode=ParseMode.HTML)

async def handle_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced withdrawal handler with better UX."""
    user_id = str(update.effective_user.id)
    users_data = load_json_data(USERS_FILE, {})
    user_data = users_data.get(user_id, {})
    
    balance = user_data.get('balance', 0)
    lowest_min_withdraw = min(WITHDRAWAL_LIMIT, BINANCE_MINIMUM_WITHDRAWAL)

    if balance < lowest_min_withdraw:
        await update.message.reply_text(
            f"<b>⚠️ Insufficient Balance</b>\n\n"
            f"<blockquote>💰 <b>Your Balance:</b> ৳{balance:.2f}\n"
            f"💸 <b>Minimum Required:</b> ৳{lowest_min_withdraw:.2f}\n\n"
            f"📱 <b>Earn more by:</b>\n"
            f"• Getting SMS numbers\n"
            f"• Referring friends\n"
            f"• Receiving OTP messages</blockquote>",
            parse_mode=ParseMode.HTML
        )
    else:
        # Enhanced withdrawal options with better formatting
        keyboard = []
        for method in PAYMENT_METHODS:
            min_amount = BINANCE_MINIMUM_WITHDRAWAL if method == 'Binance' else WITHDRAWAL_LIMIT
            button_text = f"💳 {method} (Min: ৳{min_amount:.0f})"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f'withdraw_method_{method}')])
        
        keyboard.append([InlineKeyboardButton("🔙 Cancel", callback_data='main_menu')])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"<b>💸 Withdrawal Options</b>\n\n"
            f"<blockquote>💰 <b>Your Balance:</b> ৳{balance:.2f}\n"
            f"📊 <b>Available for Withdrawal:</b> ৳{balance:.2f}\n\n"
            f"💡 <b>Choose your preferred method:</b></blockquote>",
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )

async def handle_withdrawal_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced withdrawal amount handler with better validation."""
    if context.user_data.get('state') != 'AWAITING_WITHDRAWAL_AMOUNT':
        await handle_text_menu(update, context)
        return

    user_id = str(update.effective_user.id)
    users_data = load_json_data(USERS_FILE, {})
    user_data = users_data.get(user_id)
    balance = user_data.get('balance', 0)
    method = context.user_data.get('withdrawal_method', 'N/A')

    # Enhanced input validation
    input_text = update.message.text.strip()
    
    # Remove common currency symbols and spaces
    clean_input = input_text.replace('৳', '').replace('$', '').replace(',', '').replace(' ', '')
    
    if not clean_input.replace('.', '').isdigit():
        cancel_button = InlineKeyboardButton("❌ Cancel", callback_data='cancel_withdrawal')
        keyboard = InlineKeyboardMarkup([[cancel_button]])
        await update.message.reply_text(
            "<b>⚠️ Invalid Amount</b>\n\n"
            "<blockquote>Please enter a valid number (e.g., 100, 100.50)\n"
            f"💡 <b>Your balance:</b> ৳{balance:.2f}</blockquote>\n\n"
            "<i>Click Cancel to abort the withdrawal process.</i>", 
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
        return
    
    try:
        amount_to_withdraw = float(clean_input)
    except ValueError:
        cancel_button = InlineKeyboardButton("❌ Cancel", callback_data='cancel_withdrawal')
        keyboard = InlineKeyboardMarkup([[cancel_button]])
        await update.message.reply_text(
            "<b>⚠️ Invalid Amount Format</b>\n\n"
            "<blockquote>Please enter a valid number\n"
            f"💡 <b>Your balance:</b> ৳{balance:.2f}</blockquote>\n\n"
            "<i>Click Cancel to abort the withdrawal process.</i>", 
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
        return
    
    min_withdraw = BINANCE_MINIMUM_WITHDRAWAL if method == 'Binance' else WITHDRAWAL_LIMIT

    if amount_to_withdraw < min_withdraw:
        cancel_button = InlineKeyboardButton("❌ Cancel", callback_data='cancel_withdrawal')
        keyboard = InlineKeyboardMarkup([[cancel_button]])
        await update.message.reply_text(
            f"<b>⚠️ Minimum Amount Required</b>\n\n"
            f"<blockquote>💳 <b>Method:</b> {method}\n"
            f"💰 <b>Minimum:</b> ৳{min_withdraw:.2f}\n"
            f"📊 <b>Your Request:</b> ৳{amount_to_withdraw:.2f}</blockquote>\n\n"
            "<i>Click Cancel to abort the withdrawal process.</i>", 
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
    elif amount_to_withdraw > balance:
        cancel_button = InlineKeyboardButton("❌ Cancel", callback_data='cancel_withdrawal')
        keyboard = InlineKeyboardMarkup([[cancel_button]])
        await update.message.reply_text(
            f"<b>⚠️ Insufficient Balance</b>\n\n"
            f"<blockquote>💰 <b>Your Balance:</b> ৳{balance:.2f}\n"
            f"📊 <b>Requested Amount:</b> ৳{amount_to_withdraw:.2f}\n"
            f"📉 <b>Shortfall:</b> ৳{amount_to_withdraw - balance:.2f}</blockquote>\n\n"
            "<i>Click Cancel to abort the withdrawal process.</i>", 
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
    else:
        context.user_data['withdrawal_amount'] = amount_to_withdraw
        context.user_data['state'] = 'AWAITING_WITHDRAWAL_INFO'
        
        info_prompt = f"Please enter your '{method}' number"
        if method == 'Binance':
            info_prompt = "Please enter your Binance Pay ID or Email"

        cancel_button = InlineKeyboardButton("❌ Cancel", callback_data='cancel_withdrawal')
        keyboard = InlineKeyboardMarkup([[cancel_button]])
        
        await update.message.reply_text(
            f"<b>💸 Withdrawal Confirmation</b>\n\n"
            f"<blockquote>💰 <b>Amount:</b> ৳{amount_to_withdraw:.2f}\n"
            f"💳 <b>Method:</b> {method}\n"
            f"📊 <b>Remaining Balance:</b> ৳{balance - amount_to_withdraw:.2f}</blockquote>\n\n"
            f"<b>{info_prompt}:</b>\n\n"
            "<i>Click Cancel to abort the withdrawal process.</i>", 
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

async def handle_withdrawal_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get('state') != 'AWAITING_WITHDRAWAL_INFO':
        await handle_text_menu(update, context)
        return

    user_id = str(update.effective_user.id)
    payment_info_input = update.message.text.strip()
    payment_method = context.user_data.get('withdrawal_method', 'N/A')
    amount_to_withdraw = context.user_data.get('withdrawal_amount')

    if not amount_to_withdraw:
        await update.message.reply_text("<b>⚠️ An error occurred. Please start the withdrawal process again.</b>", parse_mode=ParseMode.HTML)
        context.user_data['state'] = None
        return

    # Validate payment information based on method
    if payment_method != 'Binance':
        # For Bkash, Nagad, Rocket - only accept numeric values
        if not payment_info_input.replace('+', '').replace('-', '').replace(' ', '').isdigit():
            cancel_button = InlineKeyboardButton("❌ Cancel", callback_data='cancel_withdrawal')
            keyboard = InlineKeyboardMarkup([[cancel_button]])
            await update.message.reply_text(
                f"<b>⚠️ Invalid {payment_method} number. Please enter numbers only (e.g., 01712345678).</b>\n\n<i>Click Cancel to abort the withdrawal process.</i>", 
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard
            )
            return
    else:
        # For Binance - validate email format or numeric ID
        if '@' not in payment_info_input and not payment_info_input.isdigit():
            cancel_button = InlineKeyboardButton("❌ Cancel", callback_data='cancel_withdrawal')
            keyboard = InlineKeyboardMarkup([[cancel_button]])
            await update.message.reply_text(
                "<b>⚠️ Invalid Binance ID. Please enter your Binance Pay ID (email) or numeric ID.</b>\n\n<i>Click Cancel to abort the withdrawal process.</i>", 
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard
            )
            return

    payment_info = payment_info_input

    all_users_data = load_json_data(USERS_FILE, {})
    user_data = all_users_data[user_id]
    
    # Record withdrawal in history before deducting balance
    withdrawal_record = {
        "timestamp": datetime.now().isoformat(),
        "amount": amount_to_withdraw,
        "method": payment_method,
        "payment_info": payment_info,
        "status": "pending"
    }
    
    if 'withdraw_history' not in user_data:
        user_data['withdraw_history'] = []
    user_data['withdraw_history'].append(withdrawal_record)
    
    # Deduct balance
    user_data['balance'] -= amount_to_withdraw
    
    context.user_data['state'] = None
    context.user_data['withdrawal_method'] = None
    context.user_data['withdrawal_amount'] = None

    referrer_id = user_data.get("referred_by")
    if referrer_id:
        referrer_data = all_users_data.get(referrer_id)
        if referrer_data:
            bonus_amount = amount_to_withdraw * (REFERRAL_WITHDRAWAL_BONUS_PERCENT / 100.0)
            referrer_data['balance'] += bonus_amount
            
            try:
                await context.bot.send_message(
                    chat_id=int(referrer_id),
                    text=f"<b>🎉 Congratulations! A user you referred has withdrawn funds.</b>\n"
                         f"<blockquote>You have received a bonus of ৳{bonus_amount:.2f}."
                         f"</blockquote>",
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logging.error(f"Could not send referral bonus notification to {referrer_id}: {e}")

    save_json_data(USERS_FILE, all_users_data)

    admin_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Approve", callback_data=f'admin_accept_{user_id}'),
         InlineKeyboardButton("❌ Decline", callback_data=f'admin_decline_{user_id}')]
    ])
    
    username = f"@{user_data.get('username')}" if user_data.get('username') else "N/A"
    
    admin_message = (
        f"<b>🔥 New Withdrawal Request!</b>\n\n"
        f"<b>User:</b> {html_escape(user_data['first_name'])}\n"
        f"<b>Username:</b> {username}\n"
        f"<b>ID:</b> <code>{user_id}</code>\n"
        f"<b>Amount:</b> ৳{amount_to_withdraw:.2f}\n"
        f"<b>Method:</b> {payment_method}\n"
        f"<b>Payment Info:</b> <code>{payment_info}</code>"
    )
    
    await context.bot.send_message(
        chat_id=PAYMENT_CHANNEL_ID, text=admin_message,
        parse_mode=ParseMode.HTML, reply_markup=admin_keyboard
    )
    
    await update.message.reply_text("<b>✅ Your withdrawal request has been submitted successfully. The admin will review it.</b>", parse_mode=ParseMode.HTML)

async def handle_withdraw_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    users_data = load_json_data(USERS_FILE, {})
    user_data = users_data.get(user_id, {})
    
    withdraw_history = user_data.get('withdraw_history', [])
    
    if not withdraw_history:
        await update.message.reply_text("<b>📋 Withdraw History</b>\n\n<blockquote>No withdrawal history found.</blockquote>", parse_mode=ParseMode.HTML)
        return
    
    # Show last 10 withdrawals
    recent_withdrawals = withdraw_history[-10:]
    history_text = "<b>📋 Withdraw History (Last 10)</b>\n\n"
    
    for i, withdrawal in enumerate(reversed(recent_withdrawals), 1):
        timestamp = datetime.fromisoformat(withdrawal['timestamp']).strftime("%Y-%m-%d %H:%M")
        status_emoji = "✅" if withdrawal['status'] == "approved" else "⏳" if withdrawal['status'] == "pending" else "❌"
        history_text += f"<blockquote>{i}. {status_emoji} ৳{withdrawal['amount']:.2f} via {withdrawal['method']}\n"
        history_text += f"📅 {timestamp} - {withdrawal['status'].title()}</blockquote>\n"
    
    await update.message.reply_text(history_text, parse_mode=ParseMode.HTML)

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.username != ADMIN_USERNAME:
        logging.warning(f"Unauthorized broadcast attempt by {user.username}")
        return

    await update.message.reply_text(
        "<b>⚠️ Broadcast Deprecated</b>\n\n"
        "<blockquote>To prevent the bot from crashing, broadcasting is now handled by a separate script.</blockquote>\n\n"
        "<b>Please run the `broadcast_tool.py` script from your server's command line to send a broadcast.</b>",
        parse_mode=ParseMode.HTML
    )

async def admin_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced admin stats for monitoring 1k+ users."""
    user = update.effective_user
    if user.username != ADMIN_USERNAME:
        logging.warning(f"Unauthorized stats attempt by {user.username}")
        return

    stats = UserManager.get_user_stats()
    
    # Get additional metrics
    users_data = load_json_data(USERS_FILE, {})
    
    # Calculate more detailed stats
    balance_ranges = {
        '0-10': 0, '10-50': 0, '50-100': 0, '100+': 0
    }
    
    for user_data in users_data.values():
        balance = user_data.get('balance', 0)
        if balance < 10:
            balance_ranges['0-10'] += 1
        elif balance < 50:
            balance_ranges['10-50'] += 1
        elif balance < 100:
            balance_ranges['50-100'] += 1
        else:
            balance_ranges['100+'] += 1
    
    # Get top users by balance
    top_users = sorted(
        [(uid, data.get('balance', 0), data.get('first_name', 'Unknown')) 
         for uid, data in users_data.items()],
        key=lambda x: x[1], reverse=True
    )[:5]
    
    stats_message = (
        f"<b>📊 Bot Statistics (Enhanced)</b>\n\n"
        f"<blockquote>👥 <b>Total Users:</b> {stats['total_users']:,}\n"
        f"📱 <b>Active Users:</b> {stats['active_users']:,}\n"
        f"💰 <b>Users with Balance:</b> {stats['users_with_balance']:,}\n"
        f"💵 <b>Total Balance:</b> ৳{stats['total_balance']:,.2f}\n"
        f"📈 <b>Average Balance:</b> ৳{stats['average_balance']:.2f}\n"
        f"👥 <b>Total Referrals:</b> {stats['total_referrals']:,}</blockquote>\n\n"
        f"<b>💰 Balance Distribution:</b>\n"
        f"<blockquote>0-10: {balance_ranges['0-10']:,} users\n"
        f"10-50: {balance_ranges['10-50']:,} users\n"
        f"50-100: {balance_ranges['50-100']:,} users\n"
        f"100+: {balance_ranges['100+']:,} users</blockquote>\n\n"
        f"<b>🏆 Top Users by Balance:</b>\n"
    )
    
    for i, (uid, balance, name) in enumerate(top_users, 1):
        stats_message += f"<blockquote>{i}. {name}: ৳{balance:.2f}</blockquote>"
    
    await update.message.reply_text(stats_message, parse_mode=ParseMode.HTML)


async def add_numbers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Allows the admin to add new numbers to the bot."""
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
    """Allows the admin to delete numbers from the bot."""
    user = update.effective_user
    if user.username != ADMIN_USERNAME:
        logging.warning(f"Unauthorized /delete attempt by {user.username}")
        return

    if not context.args:
        await update.message.reply_text("<b>Usage:</b> /delete [number1] [number2] ...", parse_mode=ParseMode.HTML)
        return

    numbers_to_delete = {normalize_number(num) for num in context.args if num}
    if not numbers_to_delete:
        await update.message.reply_text("<b>No valid numbers provided to delete.</b>", parse_mode=ParseMode.HTML)
        return

    deleted_count = 0
    temp_file = f"{NUMBERS_FILE}.tmp"
    
    with manager_instance._lock: # Use the manager's lock for file safety
        if not os.path.exists(NUMBERS_FILE):
            await update.message.reply_text(f"<b>File not found: {NUMBERS_FILE}</b>", parse_mode=ParseMode.HTML)
            return
            
        try:
            with open(NUMBERS_FILE, 'r', encoding='utf-8') as original:
                with open(temp_file, 'w', encoding='utf-8') as temp:
                    for line in original:
                        number = line.strip()
                        if not number:
                            continue
                        
                        normalized = normalize_number(number)
                        if normalized in numbers_to_delete:
                            deleted_count += 1
                            continue # Skip writing this number
                        
                        temp.write(f"{number}\n")
            
            os.replace(temp_file, NUMBERS_FILE)
            
        except Exception as e:
            if os.path.exists(temp_file):
                os.remove(temp_file)
            logging.error(f"Error deleting numbers: {e}")
            await update.message.reply_text(f"<b>Error during deletion: {e}</b>", parse_mode=ParseMode.HTML)
            return

    await update.message.reply_text(f"<b>✅ Successfully deleted {deleted_count} numbers.</b>", parse_mode=ParseMode.HTML)


async def top_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show top 10 users by balance - motivational leaderboard."""
    users_data = load_json_data(USERS_FILE, {})
    
    if not users_data:
        await update.message.reply_text(
            "<b>📊 Top Users</b>\n\n<blockquote>No users found.</blockquote>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Get top 10 users by balance
    top_users = sorted(users_data.items(), key=lambda x: x[1].get('balance', 0), reverse=True)[:10]
    
    if not top_users:
        await update.message.reply_text(
            "<b>📊 Top Users</b>\n\n<blockquote>No users with balance found.</blockquote>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Format the response - simple and motivational
    top_text = "<b>🏆 Top 10 Users</b>\n\n"
    
    for i, (user_id, user_data) in enumerate(top_users, 1):
        name = user_data.get('first_name', 'Anonymous')
        balance = user_data.get('balance', 0)
        
        # Add medal emojis for top 3
        if i == 1:
            medal = "🥇"
        elif i == 2:
            medal = "🥈"
        elif i == 3:
            medal = "🥉"
        else:
            medal = f"{i}."
        
        top_text += f"{medal} <b>{name}</b> - ৳{balance:.2f}\n"
    
    await update.message.reply_text(top_text, parse_mode=ParseMode.HTML)


async def log_sms_to_d1(sms_data: dict, otp: str, owner_id: str):
    """
    Asynchronously sends SMS data to a Cloudflare Worker which logs it to D1.
    """
    # !!! --- SET THESE VALUES --- !!!
    # Find this URL in your Cloudflare Worker dashboard
    CLOUDFLARE_WORKER_URL = "https://calm-tooth-c2f4.smyaminhasan50.workers.dev"
    # API Key removed as requested
    # !!! ------------------------ !!!

    # --- FIX ---
    # The check was comparing against the correct URL.
    # It should compare against the placeholder URL.
    if CLOUDFLARE_WORKER_URL == "https://YOUR_WORKER_NAME.YOUR_ACCOUNT.workers.dev":
        logging.warning("Cloudflare Worker URL is not set. Skipping D1 log.")
        return

    payload = {
        "phone": sms_data.get('phone'),
        "country": sms_data.get('country'),
        "provider": sms_data.get('provider'),
        "message": sms_data.get('message'),
        "otp": otp,
        "owner_id": owner_id
    }
    
    headers = {
        "Content-Type": "application/json"
        # "X-API-Key" header removed
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(CLOUDFLARE_WORKER_URL, json=payload, headers=headers) as response:
                if response.status == 201:
                    logging.info(f"Successfully logged SMS for {payload['phone']} to D1.")
                else:
                    logging.error(f"Failed to log SMS to D1. Status: {response.status}, Body: {await response.text()}")
    except Exception as e:
        logging.error(f"Error connecting to Cloudflare Worker: {e}")


async def sms_watcher_task(application: Application):
    """Optimized SMS watcher with smart caching for 1k+ users."""
    global manager_instance
    if not manager_instance:
        manager_instance = IvaSmsManager()
        
    while not shutdown_event.is_set():
        try:
            await asyncio.to_thread(manager_instance.scrape_and_save_all_sms)
            
            if not os.path.exists(SMS_CACHE_FILE):
                await asyncio.sleep(5)
                continue

            # Use smart cache for better performance
            users_data = load_json_data(USERS_FILE, {})
            sent_sms_keys = load_sent_sms_keys()
            new_sms_found_for_user = False
            
            # Build optimized phone-to-user mapping with caching
            phone_to_user_map = {}
            for uid, udata in users_data.items():
                for num in udata.get("phone_numbers", []):
                    normalized = str(num).replace('+', '').replace('-', '').replace(' ', '').strip()
                    phone_to_user_map[normalized] = uid
                    phone_to_user_map[str(num)] = uid

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
                        
                        # Fast user detection with caching
                        normalized_phone = str(phone).replace('+', '').replace('-', '').replace(' ', '').strip()
                        owner_id = phone_to_user_map.get(normalized_phone) or phone_to_user_map.get(phone)
                        
                        # Simple fallback: try without leading zeros
                        if not owner_id:
                            clean_phone = normalized_phone.lstrip('0')
                            for stored_num, uid in phone_to_user_map.items():
                                stored_clean = str(stored_num).replace('+', '').replace('-', '').replace(' ', '').lstrip('0').strip()
                                if clean_phone == stored_clean:
                                    owner_id = uid
                                    break
                        
                        # --- ADD THIS BLOCK ---
                        # Asynchronously log to Cloudflare D1
                        try:
                            await log_sms_to_d1(sms_data, otp, owner_id)
                        except Exception as e:
                            logging.error(f"Failed to log SMS to D1: {e}")
                        # --- END OF BLOCK ---

                        if owner_id:
                            logging.info(f"✅ Found owner for phone {phone} -> User ID: {owner_id}")
                        else:
                            logging.warning(f"❌ No owner found for phone {phone}")

                        # Format and send messages
                        otp_display = f"<code>{otp}</code>" if otp != "N/A" else "Not found"
                        user_first_name = users_data.get(owner_id, {}).get('first_name', 'User') if owner_id else 'User'

                        phone_with_cc, phone_without_cc = get_number_formats(phone)
                        masked_phone = mask_phone_number(phone_with_cc)
                        provider = sms_data.get('provider', 'N/A')

                        # Enhanced group message with better formatting
                        group_title = "📱 <b>New OTP Received!</b> ✨" if otp != "N/A" else "📱 <b>New Message Received!</b> ✨"
                        
                        group_msg = (
                            # Title/Header (not indented, followed by spacing)
                            f"<b>{group_title}</b>\n\n" 

                            # Data lines are indented (<blockquote>) and separated by double newlines (\n\n)
                            f"<blockquote>📞 <b>Number:</b> <code>{masked_phone}</code></blockquote>\n\n"
                            f"<blockquote>🌍 <b>Country:</b> {html_escape(sms_data.get('country', 'N/A'))}</blockquote>\n\n"
                            f"<blockquote>⚙️ <b>Service:</b> {html_escape(provider)}</blockquote>\n\n"
                            f"<blockquote>🔑 <b>OTP Code:</b> {otp_display}</blockquote>\n\n"
                            
                            # These lines are also indented for consistent look
                            f"<blockquote>👤 <b>আল্লাহর বান্দা :</b> {user_first_name}</blockquote>\n\n"
                            f"<blockquote>💰 <b>Earned:</b> ৳{SMS_AMOUNT:.4f}</blockquote>\n\n"
                            f"<blockquote>⏰ <b>Time:</b> <code>{datetime.now().strftime('%H:%M:%S')}</code></blockquote>\n\n"
                            
                            # Full Message Header
                            f"<b>✉️ Full Message:</b>\n\n"
                            
                            # Full Message Body is indented using <blockquote>
                            f"<blockquote><i>{html_escape(message) if message else '(No message body)'}</i></blockquote>"
                        )

                        group_keyboard = InlineKeyboardMarkup([
                            [InlineKeyboardButton("Number Bot", url="https://t.me/ToolExprole_bot"),
                             InlineKeyboardButton("Update Group", url="https://chat.whatsapp.com/IR1iW9eePp3Kfx44sKO6u9")]
                        ])

                        # Send to group
                        try:
                            await application.bot.send_message(
                                chat_id=GROUP_ID,
                                text=group_msg,
                                parse_mode=ParseMode.HTML,
                                reply_markup=group_keyboard
                            )
                        except Exception as e:
                            logging.error(f"Failed to send SMS to group: {e}")

                        # Handle user-specific actions with enhanced tracking
                        if owner_id:
                            # Use UserManager for balance updates
                            UserManager.update_user_balance(owner_id, SMS_AMOUNT, "SMS Received")
                            new_sms_found_for_user = True

                            # Enhanced inbox message with container-style formatting like the image
                            service_name = sms_data.get('provider', 'Unknown Service')
                            country_name = sms_data.get('country', 'Unknown Country')
                            current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            
                            # The message string formatted with <blockquote> for indentation and \n\n for spacing.
                            inbox_msg = (
                                f"<b>{service_name} {country_name} RECEIVED! ✨</b>\n\n"

                                # Data lines are indented (<blockquote>) and separated by double newlines (\n\n)
                                f"<blockquote><b>⏰ Time:</b> <code>{current_time_str}</code></blockquote>\n\n"
                                f"<blockquote><b>🌍 Country:</b> {html_escape(country_name)}</blockquote>\n\n"
                                f"<blockquote><b>⚙️ Service:</b> {html_escape(service_name)}</blockquote>\n\n"
                                f"<blockquote><b>☎️ Number:</b> <code>{phone_with_cc}</code></blockquote>\n\n"
                                f"<blockquote><b>🔑 OTP:</b> {otp_display}</blockquote>\n\n"
                                
                                # Full Message Header
                                f"<b>✉️ Full Message:</b>\n\n"
                                
                                # Full Message Body is indented using <blockquote> for better presentation
                                f"<blockquote><i>{html_escape(message) if message else '(No message body)'}</i></blockquote>"
                            )

                            try:
                                await application.bot.send_message(
                                    chat_id=owner_id,
                                    text=inbox_msg,
                                    parse_mode=ParseMode.HTML
                                )
                            except Exception as e:
                                logging.error(f"Failed to send inbox SMS to user {owner_id}: {e}")
                        else:
                            # No user found - send to admin and update admin balance
                            UserManager.update_user_balance(str(ADMIN_ID), SMS_AMOUNT, "Unassigned SMS")
                            
                            admin_msg = (
                                f"<b>📱 Unassigned SMS Received</b>\n\n"
                                f"<blockquote>📞 <b>Number:</b> <code>{phone_with_cc}</code>\n"
                                f"🌍 <b>Country:</b> {html_escape(sms_data.get('country', 'N/A'))}\n"
                                f"🔑 <b>OTP Code:</b> {otp_display}\n\n"
                                f"📝 <b>Message:</b>\n{html_escape(message) if message else '<i>(No message body)</i>'}</blockquote>\n\n"
                                f"<b>⚠️ No user found for this number!</b>\n"
                                f"💰 <b>Admin earned:</b> ৳{SMS_AMOUNT:.4f}"
                            )
                            
                            try:
                                await application.bot.send_message(
                                    chat_id=ADMIN_ID,
                                    text=admin_msg,
                                    parse_mode=ParseMode.HTML
                                )
                                logging.info(f"Sent unassigned SMS to admin: {phone}")
                            except Exception as e:
                                logging.error(f"Failed to send unassigned SMS to admin: {e}")

                        sent_sms_keys.add(unique_key)
                    except Exception as e:
                        logging.error(f"Error processing SMS line: {e}")

            if new_sms_found_for_user:
                # Clear cache after updates
                smart_cache.clear_cache()
            
            save_sent_sms_keys(sent_sms_keys)

        except Exception as e:
            logging.error(f"Error in sms_watcher_task: {e}")
        
        await asyncio.sleep(10)


async def handle_text_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_state = context.user_data.get('state')
    if user_state == 'AWAITING_WITHDRAWAL_AMOUNT':
        await handle_withdrawal_amount(update, context)
        return
    if user_state == 'AWAITING_WITHDRAWAL_INFO':
        await handle_withdrawal_request(update, context)
        return

    if text == "🎁 Get Number":
        await handle_get_number(update, context)
    elif text == "👤 Account":
        await handle_account(update, context)
    elif text == "💰 Balance":
        await handle_balance(update, context)
    elif text == "💸 Withdraw":
        await handle_withdraw(update, context)
    elif text == "📋 Withdraw History":
        await handle_withdraw_history(update, context)
    else:
        # Handle any other text message with error and /start instruction
        # Only send error message in private chats (inbox), not in group chats
        if update.effective_chat.type == 'private':
            await update.message.reply_text(
                text="<b>❌ Error: Invalid message format</b>\n\n"
                     "<blockquote>I don't understand regular text messages. Please use /start to begin using this bot and try again.</blockquote>\n\n"
                     "<b>💡 Instructions:</b>\n"
                     "• Click /start to see available options\n"
                     "• Use the buttons provided by the bot\n"
                     "• Don't send regular text messages",
                parse_mode=ParseMode.HTML,
                reply_markup=get_main_menu_keyboard()
            )
        # For group chats, silently ignore unknown text messages


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log errors and handle gracefully."""
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

async def main():
    """Main bot loop for headless operation."""
    global manager_instance
    try:
        load_config()
    except Exception as e:
        print(f"CRITICAL: Could not load config. {e}") # <-- ADDED PRINT
        logging.critical(f"CRITICAL: Could not load config. {e}")
        return
        
    try:
        manager_instance = IvaSmsManager()
    except Exception as e:
        print(f"CRITICAL: Failed to initialize SeleniumBase driver: {e}") # <-- Fixed typo
        logging.critical(f"CRITICAL: Failed to initialize SeleniumBase driver: {e}")
        return

    # Configure application with optimized settings
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
        manager_instance.set_loop(asyncio.get_running_loop())
        
    application.add_error_handler(error_handler)
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("top", top_command))
    application.add_handler(CommandHandler("update", broadcast_command)) # Keep stub to inform admin
    application.add_handler(CommandHandler("stats", admin_stats_command))
    application.add_handler(CommandHandler("add", add_numbers_command))
    application.add_handler(CommandHandler("delete", delete_numbers_command)) # Add delete handler
    application.add_handler(CallbackQueryHandler(button_callback_handler))
    
    menu_buttons = ["🎁 Get Number", "👤 Account", "💰 Balance", "💸 Withdraw", "📋 Withdraw History"]
    menu_filter = filters.TEXT & filters.Regex(f'^({"|".join(re.escape(btn) for btn in menu_buttons)})$')
    application.add_handler(MessageHandler(menu_filter, handle_text_menu))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_menu))

    sms_task = None
    try:
        await application.initialize()
        await application.start()
        await application.updater.start_polling()
        logging.info("Bot started successfully.")

        sms_task = asyncio.create_task(sms_watcher_task(application))
        
        await shutdown_event.wait()
    
    except (KeyboardInterrupt, SystemExit):
        logging.info("Shutdown signal received.")
    finally:
        shutdown_event.set()
        if sms_task and not sms_task.done():
            sms_task.cancel()
            try:
                await sms_task
            except asyncio.CancelledError:
                logging.info("Background SMS watcher cancelled.")

        if hasattr(application, 'updater') and application.updater and application.updater.is_running():
            await application.updater.stop()
        
        await application.stop()
        await application.shutdown()
        
        if manager_instance:
            manager_instance.cleanup()
            
        logging.info("Bot stopped gracefully.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        shutdown_event.set()
        # The main() function's finally block will handle cleanup
        logging.info("Program interrupted by user. Initiating shutdown.")
