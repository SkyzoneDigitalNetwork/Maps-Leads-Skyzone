# -*- coding: utf-8 -*-
import logging
import os
import json
import asyncio
import re
import csv
import io
import requests
import time
import urllib.parse
from datetime import datetime, timedelta
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import (
    Application, CommandHandler, ContextTypes, 
    CallbackQueryHandler, MessageHandler, filters
)

import firebase_admin
from firebase_admin import credentials, db
from playwright.async_api import async_playwright
import aiohttp
from aiohttp import web

# --- Load Env ---
load_dotenv()
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Variables ---
TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
SUPER_ADMIN_ID = os.environ.get('SUPER_ADMIN_ID')
LOG_GROUP_ID = os.environ.get('LOG_GROUP_ID')
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')
FB_JSON = os.environ.get('FIREBASE_CREDENTIALS_JSON')
FB_URL = os.environ.get('FIREBASE_DATABASE_URL')
RENDER_URL = os.environ.get('RENDER_EXTERNAL_URL')
PORT = int(os.environ.get('PORT', '8080'))

WEB_APP_URL = RENDER_URL if RENDER_URL else f"http://localhost:{PORT}"

# --- Global State ---
active_tasks = {} 
recent_logs = [] 
BOT_USERNAME = "" 

CATEGORIES =[
    "Restaurants", "IT Companies", "Hospitals", "Real Estate", 
    "Plumbers", "Gyms", "Hotels", "Coffee Shops", "Car Repair", "Dentists"
]

# 🌟 DEFAULT WEB CONFIG (For dynamic Web App)
DEFAULT_WEB_CONFIG = {
    "packages":[
        {"id": "p1", "name": "🥉 1 Month (Basic)", "duration_minutes": 43200, "price": "500 BDT", "badge": ""},
        {"id": "p2", "name": "🥈 3 Months (Standard)", "duration_minutes": 129600, "price": "1350 BDT", "badge": "10% Discount"},
        {"id": "p3", "name": "🥇 6 Months (Premium)", "duration_minutes": 259200, "price": "2500 BDT", "badge": "Best Value"}
    ],
    "methods":[
        {"name": "Bkash", "number": "01757481646"},
        {"name": "Nagad", "number": "01757481646"},
        {"name": "Rocket", "number": "01757481646"},
        {"name": "Binance", "number": "PayID: 12345678"}
    ],
    "rules": "উপরের যেকোনো নাম্বারে Send Money করার পর ট্রানজেকশন আইডি এবং নাম্বার দিয়ে সাবমিট করুন।"
}

# --- Firebase Initialization ---
try:
    if not firebase_admin._apps:
        cred_dict = json.loads(FB_JSON) if isinstance(FB_JSON, str) and FB_JSON.startswith('{') else FB_JSON
        if isinstance(cred_dict, str) and os.path.exists(cred_dict): 
            cred = credentials.Certificate(cred_dict)
        else: 
            cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred, {'databaseURL': FB_URL})
        logger.info("🔥 Firebase Connected Successfully!")
except Exception as e:
    logger.error(f"❌ Firebase Init Error: {e}")

# --- Helper Functions ---
def is_super_admin(uid): 
    return str(uid) == str(SUPER_ADMIN_ID)

def get_user_data(uid):
    if is_super_admin(uid): 
        return {
            "name": "Super Admin", 
            "sub_ends": (datetime.now() + timedelta(days=3650)).isoformat(), 
            "lt_searches": 0, "lt_leads": 0, "team_limit": 0, "team_added": 0
        }
    return db.reference(f'bot_users/{uid}').get()

def check_subscription(uid):
    if is_super_admin(uid): return True, None
    user = get_user_data(uid)
    if not user: return False, "not_found"
    
    parent_id = user.get('parent_id')
    if parent_id:
        parent_user = db.reference(f'bot_users/{parent_id}').get()
        if not parent_user: return False, "expired"
        sub_ends_str = parent_user.get('sub_ends')
    else:
        sub_ends_str = user.get('sub_ends')

    if not sub_ends_str: return False, "expired"
    sub_ends = datetime.fromisoformat(sub_ends_str)
    if datetime.now() > sub_ends: return False, "expired"
    return True, sub_ends

async def post_init(app: Application):
    global BOT_USERNAME
    bot_info = await app.bot.get_me()
    BOT_USERNAME = bot_info.username
    logger.info(f"🤖 Bot Username: @{BOT_USERNAME}")

async def keep_alive_task(context: ContextTypes.DEFAULT_TYPE):
    if not RENDER_URL: return
    while True:
        try: requests.get(RENDER_URL, timeout=10)
        except: pass
        await asyncio.sleep(600)

async def analyze_with_ai(context, is_error=False):
    if not GROQ_API_KEY or not LOG_GROUP_ID: return
    if not recent_logs: return

    logs_text = "\n".join(recent_logs)
    recent_logs.clear()
    
    if is_error:
        prompt = f"Expert AI System Analyst. Analyze this Google Maps Scraper bot ERROR log and give an emergency Bengali report:\n{logs_text}"
    else:
        prompt = f"Expert AI System Analyst. Analyze these B2B Scraper logs and give a short Bengali report on team performance and errors:\n{logs_text}"

    try:
        url = "https://api.groq.com/openai/v1/chat/completions"
        headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
        payload = {"model": "llama-3.3-70b-versatile", "messages":[{"role": "user", "content": prompt}], "temperature": 0.7}
        
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        if response.status_code == 200:
            ai_reply = response.json()['choices'][0]['message']['content']
            header = "🚨 **AI EMERGENCY ALERT:**" if is_error else "🧠 **AI Auto Analysis Report:**"
            await context.bot.send_message(chat_id=LOG_GROUP_ID, text=f"{header}\n\n{ai_reply}", parse_mode='Markdown')
    except Exception as e:
        logger.error(f"AI Error: {e}")

async def send_log(context, user_name, user_id, action):
    log_text = f"👤 **{user_name}** (`{user_id}`)\n📌 অ্যাকশন: {action}\n🕒 সময়: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    if LOG_GROUP_ID:
        try: await context.bot.send_message(chat_id=LOG_GROUP_ID, text=log_text, parse_mode='Markdown')
        except: pass

    recent_logs.append(log_text)
    if "এরর" in action or "Error" in action or "Executable doesn't exist" in action: 
        asyncio.create_task(analyze_with_ai(context, is_error=True))
    elif len(recent_logs) >= 15: 
        asyncio.create_task(analyze_with_ai(context, is_error=False))

async def extract_email(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    html = await response.text()
                    emails = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', html)
                    valid_emails =[e for e in emails if not any(x in e.lower() for x in['.png', '.jpg', 'sentry', 'example'])]
                    return valid_emails[0] if valid_emails else "N/A"
    except: pass
    return "N/A"

# --- Scraper Engine ---
async def scraper_worker(query, user_id, user_name, context):
    bot = context.bot if hasattr(context, 'bot') else context
    await send_log(bot, user_name, user_id, f"স্ক্র্যাপিং শুরু করেছে: `{query}`")
    
    if not is_super_admin(user_id):
        user_ref = db.reference(f'bot_users/{user_id}')
        current_searches = user_ref.child('lt_searches').get() or 0
        user_ref.update({'lt_searches': current_searches + 1})

    ref = db.reference(f'gmaps_leads/{user_id}')
    leads_found = 0
    session_leads =[] 

    status_msg = await bot.send_message(
        chat_id=user_id, 
        text=f"🚀 **{query}** এর জন্য ম্যাপে খোঁজা হচ্ছে...\nদয়া করে অপেক্ষা করুন।", 
        parse_mode='Markdown'
    )

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
            page = await browser.new_page()
            
            search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
            await page.goto(search_url, timeout=60000)
            await page.wait_for_timeout(5000)
            
            scrollable_div = await page.query_selector('div[role="feed"]')
            if scrollable_div:
                for _ in range(6):
                    if user_id not in active_tasks: break
                    await page.evaluate('(element) => element.scrollBy(0, 3000)', scrollable_div)
                    await page.wait_for_timeout(2000)
            
            places = await page.query_selector_all('a[href*="/maps/place/"]')
            place_urls = list(set([await place.get_attribute('href') for place in places if await place.get_attribute('href')]))
            
            total_places = len(place_urls)
            await bot.edit_message_text(
                chat_id=user_id, message_id=status_msg.message_id, 
                text=f"🔍 **{total_places}** টি বিজনেস পাওয়া গেছে।\nডেটা সেভ করা হচ্ছে...", parse_mode='Markdown'
            )

            for idx, url in enumerate(place_urls):
                if user_id not in active_tasks: break
                if idx % 3 == 0 or idx == 0:
                    try: 
                        await bot.edit_message_text(
                            chat_id=user_id, message_id=status_msg.message_id, 
                            text=f"⏳ **লাইভ স্ক্র্যাপিং...**\n🎯 টার্গেট: `{query}`\n🔍 পাওয়া গেছে: **{total_places}**\n🔄 চেক: **{idx}**\n✅ সেভ: **{leads_found}**", 
                            parse_mode='Markdown'
                        )
                    except: pass

                try:
                    await page.goto(url, timeout=30000)
                    await page.wait_for_timeout(3000) 
                    
                    name_el = await page.query_selector('h1')
                    name = await name_el.inner_text() if name_el else "Unknown"
                    
                    rating = 0.0
                    total_reviews = 0
                    
                    rating_text = await page.evaluate('''() => {
                        let el = document.querySelector('span[aria-label*="stars"], div[aria-label*="stars"], button[aria-label*="stars"]');
                        if (el) return el.getAttribute('aria-label');
                        let fallback = document.querySelector('.F7nice');
                        return fallback ? fallback.innerText : '';
                    }''')
                    
                    if rating_text:
                        r_match = re.search(r'([\d\.]+)\s*stars?', rating_text, re.IGNORECASE)
                        if r_match: rating = float(r_match.group(1))
                        else:
                            r_match2 = re.search(r'([\d\.]+)', rating_text)
                            if r_match2: rating = float(r_match2.group(1))
                            
                        rev_match = re.search(r'([\d,]+)\s*reviews?', rating_text, re.IGNORECASE)
                        if rev_match: total_reviews = int(rev_match.group(1).replace(',', ''))
                        else:
                            rev_match2 = re.search(r'\(([\d,]+)\)', rating_text)
                            if rev_match2: total_reviews = int(rev_match2.group(1).replace(',', ''))

                    # Extract Histogram
                    r5 = r4 = r3 = r2 = r1 = 0
                    try:
                        reviews_tab = await page.query_selector('button[aria-label*="Reviews"], button:has-text("Reviews"), div[role="tab"]:has-text("Reviews")')
                        if reviews_tab:
                            await reviews_tab.click()
                            await page.wait_for_timeout(2500) 
                            
                            histogram_data = await page.evaluate('''() => {
                                let data = {5:0, 4:0, 3:0, 2:0, 1:0};
                                let elements = document.querySelectorAll('[aria-label*="stars,"]');
                                elements.forEach(el => {
                                    let label = el.getAttribute('aria-label');
                                    let match = label.match(/(\d)\s*stars?,\s*([\d,]+)\s*reviews?/i);
                                    if(match) {
                                        data[parseInt(match[1])] = parseInt(match[2].replace(/,/g, ''));
                                    }
                                });
                                return data;
                            }''')
                            r5, r4, r3, r2, r1 = histogram_data.get('5', 0), histogram_data.get('4', 0), histogram_data.get('3', 0), histogram_data.get('2', 0), histogram_data.get('1', 0)
                    except Exception: pass
                        
                    phone_el = await page.query_selector('button[data-item-id^="phone:"]')
                    phone = await phone_el.get_attribute('aria-label') if phone_el else "N/A"
                    if phone != "N/A": phone = phone.replace("Phone:", "").strip()
                    
                    addr_el = await page.query_selector('button[data-item-id="address"]')
                    address = await addr_el.get_attribute('aria-label') if addr_el else "N/A"
                    if address != "N/A": address = address.replace("Address:", "").strip()
                    
                    web_el = await page.query_selector('a[data-item-id="authority"]')
                    website = await web_el.get_attribute('href') if web_el else "N/A"
                    
                    email = await extract_email(website) if website != "N/A" else "N/A"
                    
                    safe_key = re.sub(r'\D', '', phone) if phone != "N/A" else re.sub(r'[^a-zA-Z0-9]', '', name)
                    if not safe_key: safe_key = str(int(time.time()))
                    if ref.child(safe_key).get(): continue
                        
                    lead_data = {
                        'name': name, 'rating': rating, 'total_reviews': total_reviews,
                        'stars_5': r5, 'stars_4': r4, 'stars_3': r3, 'stars_2': r2, 'stars_1': r1,
                        'phone': phone, 'email': email, 'website': website, 'address': address,
                        'query': query, 'date': datetime.now().isoformat(), 'is_deleted_by_user': False 
                    }
                    ref.child(safe_key).set(lead_data)
                    session_leads.append(lead_data) 
                    leads_found += 1
                except Exception: continue
            
            await browser.close()
            
    except Exception as e:
        await send_log(bot, user_name, user_id, f"❌ স্ক্র্যাপিং এরর: {str(e)[:100]}")
        try: await bot.edit_message_text(chat_id=user_id, message_id=status_msg.message_id, text=f"❌ স্ক্র্যাপিং এরর।")
        except: pass
        if user_id in active_tasks: del active_tasks[user_id]
        return
    
    if user_id in active_tasks: del active_tasks[user_id]
    
    if not is_super_admin(user_id) and leads_found > 0:
        user_ref = db.reference(f'bot_users/{user_id}')
        current_leads = user_ref.child('lt_leads').get() or 0
        user_ref.update({'lt_leads': current_leads + leads_found})

    try:
        await bot.edit_message_text(
            chat_id=user_id, message_id=status_msg.message_id, 
            text=f"✅ **স্ক্র্যাপিং সম্পন্ন!**\n🎯 টার্গেট: `{query}`\n📥 নতুন লিড: **{leads_found}** টি।\n\nঅটোমেটিক ফাইল তৈরি করা হচ্ছে...", parse_mode='Markdown'
        )
    except: pass

    if session_leads:
        si = io.StringIO()
        cw = csv.writer(si)
        cw.writerow(['Business Name', 'Rating', 'Total Reviews', '5-Star', '4-Star', '3-Star', '2-Star', '1-Star', 'Phone', 'Email', 'Website', 'Address', 'Query', 'Date'])
        for v in session_leads:
            cw.writerow([
                v.get('name',''), v.get('rating',''), v.get('total_reviews',''),
                v.get('stars_5',''), v.get('stars_4',''), v.get('stars_3',''), v.get('stars_2',''), v.get('stars_1',''),
                v.get('phone',''), v.get('email',''), v.get('website',''), v.get('address',''), v.get('query',''), v.get('date','')
            ])
        output = io.BytesIO(si.getvalue().encode('utf-8'))
        output.name = f"Report_{query.replace(' ', '_')}_{datetime.now().strftime('%H%M%S')}.csv"
        await bot.send_document(user_id, output, caption=f"📊 **অটোমেটিক রিপোর্ট:** `{query}`\nএই সেশনে পাওয়া **{leads_found}** টি নতুন লিড।")

    await send_log(bot, user_name, user_id, f"স্ক্র্যাপিং শেষ। নতুন লিড: {leads_found}")

# --- Menus ---
def get_main_menu(uid):
    user_data = get_user_data(uid)
    lt_leads = user_data.get('lt_leads', 0) if user_data else 0
    lt_searches = user_data.get('lt_searches', 0) if user_data else 0
    sub_ends = user_data.get('sub_ends', '') if user_data else ''
    team_limit = user_data.get('team_limit', 0) if user_data else 0
    team_added = user_data.get('team_added', 0) if user_data else 0
    role = 'admin' if is_super_admin(uid) else 'user'
    
    web_url = f"{WEB_APP_URL}/?uid={uid}&name={urllib.parse.quote(user_data.get('name', 'User') if user_data else 'User')}&leads={lt_leads}&searches={lt_searches}&ends={sub_ends}&role={role}&team_limit={team_limit}&team_added={team_added}&bot={BOT_USERNAME}"
    
    keyboard = []
    
    if is_super_admin(uid):
        keyboard.append([InlineKeyboardButton("🌐 অ্যাডমিন প্যানেল (Web App)", web_app=WebAppInfo(url=web_url))])
    else:
        keyboard.append([InlineKeyboardButton("🌐 প্রোফাইল ও ড্যাশবোর্ড (Web App)", web_app=WebAppInfo(url=web_url))])
    
    hidden_btns = db.reference('bot_settings/hidden_buttons').get() or {}
    
    if is_super_admin(uid) or not hidden_btns.get('btn_target'):
        keyboard.append([InlineKeyboardButton("🎯 টার্গেট সেট করুন (ক্যাটাগরি)", callback_data='set_target')])
    if is_super_admin(uid) or not hidden_btns.get('btn_scrape'):
        keyboard.append([InlineKeyboardButton("🚀 শুরু করুন", callback_data='start_scraping'), InlineKeyboardButton("🛑 বন্ধ করুন", callback_data='stop_scraping')])
    if is_super_admin(uid) or not hidden_btns.get('btn_download'):
        keyboard.append([InlineKeyboardButton("📥 আমার লিড ডাউনলোড", callback_data='download_leads')])
    if is_super_admin(uid) or not hidden_btns.get('btn_clear'):
        keyboard.append([InlineKeyboardButton("🗑️ প্যানেল ক্লিয়ার করুন", callback_data='soft_delete_leads')])
        
    if team_limit > 0 and (is_super_admin(uid) or not hidden_btns.get('btn_team')):
        keyboard.append([InlineKeyboardButton("➕ মেম্বার অ্যাড করুন (Team)", callback_data='tl_add_member')])

    keyboard.append([InlineKeyboardButton("🔄 রিফ্রেশ (Restart)", callback_data='refresh_bot')])

    if is_super_admin(uid):
        keyboard.append([InlineKeyboardButton("👑 টেলিগ্রাম অ্যাডমিন প্যানেল", callback_data='super_admin_panel')])
    
    return InlineKeyboardMarkup(keyboard)

def get_expired_menu(uid):
    user_data = get_user_data(uid)
    web_url = f"{WEB_APP_URL}/?uid={uid}&name={urllib.parse.quote(user_data.get('name', 'User') if user_data else 'User')}&ends=expired&role=user&bot={BOT_USERNAME}"
    return InlineKeyboardMarkup([[InlineKeyboardButton("🌐 ওয়েব অ্যাপ (প্যাকেজ ও পেমেন্ট)", web_app=WebAppInfo(url=web_url))]])

async def show_toggle_menu(message_obj, uid):
    hidden = db.reference('bot_settings/hidden_buttons').get() or {}
    def get_icon(key): return "❌" if hidden.get(key) else "✅"

    btns = [[InlineKeyboardButton(f"{get_icon('btn_target')} টার্গেট সেট", callback_data='tgl_btn_target')],[InlineKeyboardButton(f"{get_icon('btn_scrape')} শুরু/বন্ধ করুন", callback_data='tgl_btn_scrape')],[InlineKeyboardButton(f"{get_icon('btn_download')} লিড ডাউনলোড", callback_data='tgl_btn_download')],[InlineKeyboardButton(f"{get_icon('btn_clear')} প্যানেল ক্লিয়ার", callback_data='tgl_btn_clear')],[InlineKeyboardButton(f"{get_icon('btn_team')} টিম মেম্বার অ্যাড", callback_data='tgl_btn_team')],
        [InlineKeyboardButton("🔙 ব্যাক", callback_data='super_admin_panel')]
    ]
    if hasattr(message_obj, 'edit_text'):
        await message_obj.edit_text("👁️ **বাটন হাইড/শো কন্ট্রোল:**\n(❌ মানে হাইড, ✅ মানে শো)", reply_markup=InlineKeyboardMarkup(btns))
    else:
        await message_obj.reply_text("👁️ **বাটন হাইড/শো কন্ট্রোল:**\n(❌ মানে হাইড, ✅ মানে শো)", reply_markup=InlineKeyboardMarkup(btns))

# --- Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    uname = update.effective_user.first_name
    text = update.message.text.strip() if update.message else ""
    
    # 🌟 Deep Link Catchers
    if text == '/start do_scrape':
        req = db.reference(f'pending_requests/{uid}').get()
        if req and req.get('action') == 'scrape':
            query = req.get('query')
            db.reference(f'pending_requests/{uid}').delete() 
            
            is_auth, sub_status = check_subscription(uid)
            if not is_auth:
                await update.message.reply_text("⚠️ আপনার অ্যাকাউন্টের মেয়াদ শেষ!", reply_markup=get_expired_menu(uid))
                return
                
            if uid in active_tasks:
                await update.message.reply_text("⚠️ আপনার একটি কাজ অলরেডি চলছে!")
                return
            task = asyncio.create_task(scraper_worker(query, uid, uname, context.bot))
            active_tasks[uid] = task
        return

    elif text == '/start do_payment':
        req = db.reference(f'pending_requests/{uid}').get()
        if req and req.get('action') == 'payment':
            plan = req.get('plan', '')
            price = req.get('price', '')
            sender = req.get('sender', '')
            trxid = req.get('trxid', '')
            db.reference(f'pending_requests/{uid}').delete() 
            
            admin_msg = f"💰 **New Payment Received!**\n\n👤 **User:** {uname} (`{uid}`)\n📦 **Package:** {plan}\n💵 **Amount:** {price}\n📱 **Sender:** `{sender}`\n🔢 **TrxID:** `{trxid}`\n\n✅ To approve, copy this command and send:\n`/approve_sub {uid} 30`"
            await context.bot.send_message(chat_id=SUPER_ADMIN_ID, text=admin_msg, parse_mode='Markdown')
            await update.message.reply_text("✅ আপনার পেমেন্ট রিকোয়েস্ট অ্যাডমিনের কাছে পাঠানো হয়েছে। ভেরিফাই করে দ্রুত আপনার অ্যাকাউন্ট আপডেট করা হবে।")
        return

    context.user_data.clear()
    if uid in active_tasks:
        active_tasks[uid].cancel()
        del active_tasks[uid]

    is_auth, sub_status = check_subscription(uid)
    if not is_auth:
        if sub_status == "not_found": 
            await update.message.reply_text("⛔ আপনি অনুমোদিত নন। অ্যাডমিনের সাথে যোগাযোগ করুন।")
        elif sub_status == "expired": 
            await update.message.reply_text("⚠️ **আপনার অ্যাকাউন্টের মেয়াদ শেষ!**\n\nনতুন করে রিনিউ করতে বা প্যাকেজ দেখতে নিচের '🌐 ওয়েব অ্যাপ' বাটনে ক্লিক করুন।", reply_markup=get_expired_menu(uid))
        return

    await update.message.reply_text("🗺️ **Google Maps Scraper Dashboard**\n\nআপনার কাজ শুরু করতে নিচের বাটন ব্যবহার করুন:", reply_markup=get_main_menu(uid))

async def approve_sub_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not is_super_admin(uid): return
    try:
        target_uid = context.args[0]
        days = int(context.args[1])
        user_ref = db.reference(f'bot_users/{target_uid}')
        user_data = user_ref.get()
        if user_data:
            current_end_str = user_data.get('sub_ends')
            if current_end_str:
                current_end = datetime.fromisoformat(current_end_str)
                if current_end < datetime.now() and days > 0: current_end = datetime.now()
            else:
                current_end = datetime.now()
            new_end = current_end + timedelta(days=days)
            user_ref.update({'sub_ends': new_end.isoformat()})
            await update.message.reply_text(f"✅ ইউজার `{target_uid}` এর মেয়াদ {days} দিন বাড়ানো হয়েছে।")
            await context.bot.send_message(chat_id=target_uid, text=f"🎉 **অভিনন্দন!**\nআপনার পেমেন্ট ভেরিফাই হয়েছে এবং অ্যাকাউন্টের মেয়াদ {days} দিন বাড়ানো হয়েছে।\nএখন আপনি কাজ শুরু করতে পারেন।")
        else:
            await update.message.reply_text("❌ ইউজার পাওয়া যায়নি।")
    except Exception as e:
        await update.message.reply_text("❌ ব্যবহার: `/approve_sub <user_id> <days>`")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = str(update.effective_user.id)
    uname = update.effective_user.first_name
    
    try: await query.answer()
    except: pass 
    
    is_auth, sub_status = check_subscription(uid)
    if not is_auth and query.data not in ['main_menu', 'refresh_bot']:
        await query.edit_message_text("⚠️ **আপনার অ্যাকাউন্টের মেয়াদ শেষ!**", reply_markup=get_expired_menu(uid))
        return

    if query.data == 'refresh_bot':
        if uid in active_tasks:
            active_tasks[uid].cancel()
            del active_tasks[uid]
        context.user_data.clear()
        await query.edit_message_text("🔄 সবকিছু রিসেট করা হয়েছে। নতুন করে শুরু করুন:", reply_markup=get_main_menu(uid))
        return

    if query.data == 'main_menu':
        await query.edit_message_text("🗺️ **Google Maps Scraper Dashboard**", reply_markup=get_main_menu(uid))
        return

    if query.data == 'set_target':
        keyboard = [[InlineKeyboardButton("✍️ কাস্টম ক্যাটাগরি লিখব", callback_data='cat_custom')],[InlineKeyboardButton("🔙 ব্যাক", callback_data='main_menu')]]
        await query.edit_message_text("📂 **প্রথমে একটি ক্যাটাগরি সিলেক্ট করুন:**", reply_markup=InlineKeyboardMarkup(keyboard))
        
    elif query.data == 'cat_custom':
        context.user_data['awaiting_custom_cat'] = True
        await query.message.reply_text("✍️ আপনার কাস্টম ক্যাটাগরি লিখে পাঠান (যেমন: Car Wash):")

    elif query.data == 'start_scraping':
        target = context.user_data.get('target_query')
        if not target: return
        if uid in active_tasks: return
        task = asyncio.create_task(scraper_worker(target, uid, uname, context.bot))
        active_tasks[uid] = task
        
    elif query.data == 'stop_scraping':
        if uid in active_tasks:
            active_tasks[uid].cancel()
            del active_tasks[uid]
            await query.edit_message_text("🛑 স্ক্র্যাপিং বন্ধ করা হয়েছে।", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ব্যাক", callback_data='main_menu')]]))
            
    elif query.data == 'soft_delete_leads':
        leads_ref = db.reference(f'gmaps_leads/{uid}')
        leads = leads_ref.get() or {}
        count = 0
        for key, val in leads.items():
            if not val.get('is_deleted_by_user'):
                leads_ref.child(key).update({'is_deleted_by_user': True})
                count += 1
        await query.edit_message_text(f"🗑️ আপনার প্যানেল থেকে **{count}** টি লিড ক্লিয়ার করা হয়েছে।", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ব্যাক", callback_data='main_menu')]]))
            
    elif query.data == 'download_leads':
        leads = db.reference(f'gmaps_leads/{uid}').get() or {}
        active_leads = {k: v for k, v in leads.items() if not v.get('is_deleted_by_user')}
        if not active_leads:
            await query.message.reply_text("⚠️ আপনার ডাউনলোড করার মতো কোনো নতুন লিড নেই।")
            return
            
        si = io.StringIO()
        cw = csv.writer(si)
        cw.writerow(['Business Name', 'Rating', 'Total Reviews', '5-Star', '4-Star', '3-Star', '2-Star', '1-Star', 'Phone', 'Email', 'Website', 'Address', 'Query', 'Date'])
        for key, v in active_leads.items():
            cw.writerow([
                v.get('name',''), v.get('rating',''), v.get('total_reviews',''),
                v.get('stars_5',''), v.get('stars_4',''), v.get('stars_3',''), v.get('stars_2',''), v.get('stars_1',''),
                v.get('phone',''), v.get('email',''), v.get('website',''), v.get('address',''), v.get('query',''), v.get('date','')
            ])
        output = io.BytesIO(si.getvalue().encode('utf-8'))
        output.name = f"My_Leads_{datetime.now().strftime('%Y%m%d')}.csv"
        await context.bot.send_document(uid, output, caption=f"✅ আপনার মোট {len(active_leads)} টি লিড।")

    elif query.data == 'tl_add_member':
        user_data = get_user_data(uid)
        limit = user_data.get('team_limit', 0)
        added = user_data.get('team_added', 0)
        if added >= limit:
            await query.edit_message_text("⚠️ আপনার টিম লিমিট শেষ হয়ে গেছে।", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ব্যাক", callback_data='main_menu')]]))
            return
        
        context.user_data['add_user_step'] = 'name'
        context.user_data['add_user_is_tl'] = False
        context.user_data['add_user_parent'] = uid
        await query.message.reply_text(f"✍️ **নতুন মেম্বারের নাম লিখুন:**\n(আপনার লিমিট বাকি আছে: {limit - added} জন)")

    elif query.data == 'super_admin_panel':
        if not is_super_admin(uid): return
        btns = [[InlineKeyboardButton("➕ ইউজার অ্যাড (24h)", callback_data='sa_add_user'), InlineKeyboardButton("👥 টিম লিডার অ্যাড", callback_data='sa_add_tl')],[InlineKeyboardButton("👁️ বাটন হাইড/শো করুন", callback_data='sa_toggle_menu')],[InlineKeyboardButton("🔙 ব্যাক", callback_data='main_menu')]
        ]
        # Rest of the admin controls will be handled from the Web App now.
        await query.edit_message_text("👑 **Super Admin Control (Basic)**\n\n(ইউজার কন্ট্রোল ও প্যাকেজ ম্যানেজ করতে '🌐 অ্যাডমিন প্যানেল (Web App)' বাটনে ক্লিক করুন)", reply_markup=InlineKeyboardMarkup(btns))
        
    elif query.data == 'sa_add_user':
        if not is_super_admin(uid): return
        context.user_data['add_user_step'] = 'name'
        context.user_data['add_user_is_tl'] = False
        await query.message.reply_text("✍️ **নতুন ইউজারের নাম লিখুন:**")

    elif query.data == 'sa_add_tl':
        if not is_super_admin(uid): return
        context.user_data['add_user_step'] = 'name'
        context.user_data['add_user_is_tl'] = True
        await query.message.reply_text("✍️ **টিম লিডারের নাম লিখুন:**")

    elif query.data == 'sa_toggle_menu':
        if not is_super_admin(uid): return
        await show_toggle_menu(query.message, uid)

    elif query.data.startswith('tgl_'):
        if not is_super_admin(uid): return
        key = query.data.split('tgl_')[1]
        current = db.reference(f'bot_settings/hidden_buttons/{key}').get()
        db.reference(f'bot_settings/hidden_buttons/{key}').set(not current)
        await show_toggle_menu(query.message, uid)

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    text = update.message.text.strip()
    
    if context.user_data.get('add_user_step') == 'name':
        context.user_data['add_user_name'] = text
        context.user_data['add_user_step'] = 'uid'
        await update.message.reply_text("✍️ **এবার ইউজারের টেলিগ্রাম ID (UID) দিন:**")
        return
        
    elif context.user_data.get('add_user_step') == 'uid':
        uid_input = text
        name = context.user_data['add_user_name']
        is_tl = context.user_data.get('add_user_is_tl', False)
        parent_id = context.user_data.get('add_user_parent', None)
        
        if is_tl:
            context.user_data['add_user_uid'] = uid_input
            context.user_data['add_user_step'] = 'limit'
            await update.message.reply_text("✍️ **এবার টিম মেম্বার লিমিট (সংখ্যা) দিন:**")
            return
        elif parent_id:
            user_data = get_user_data(parent_id)
            sub_ends = user_data.get('sub_ends')
            added = user_data.get('team_added', 0)
            db.reference(f'bot_users/{uid_input}').set({"name": name, "sub_ends": sub_ends, "lt_searches": 0, "lt_leads": 0, "parent_id": parent_id})
            db.reference(f'bot_users/{parent_id}').update({"team_added": added + 1})
            await update.message.reply_text(f"✅ টিম মেম্বার `{uid_input}` ({name}) যুক্ত হয়েছে।")
            context.user_data.clear()
            return
        else:
            trial_ends = (datetime.now() + timedelta(days=1)).isoformat() 
            db.reference(f'bot_users/{uid_input}').set({"name": name, "sub_ends": trial_ends, "lt_searches": 0, "lt_leads": 0, "team_limit": 0, "team_added": 0})
            await update.message.reply_text(f"✅ ইউজার `{uid_input}` ({name}) ২৪ ঘণ্টার ট্রায়ালসহ যুক্ত হয়েছে।")
            context.user_data.clear()
            return
            
    elif context.user_data.get('add_user_step') == 'limit':
        try:
            limit = int(text)
            uid_input = context.user_data['add_user_uid']
            name = context.user_data['add_user_name']
            trial_ends = (datetime.now() + timedelta(days=30)).isoformat() 
            db.reference(f'bot_users/{uid_input}').set({"name": name, "sub_ends": trial_ends, "lt_searches": 0, "lt_leads": 0, "team_limit": limit, "team_added": 0})
            await update.message.reply_text(f"✅ টিম লিডার `{uid_input}` ({name}) যুক্ত হয়েছে। লিমিট: {limit} জন।")
            context.user_data.clear()
        except ValueError:
            await update.message.reply_text("❌ দয়া করে শুধু সংখ্যা লিখুন।")
        return

    is_auth, sub_status = check_subscription(uid)
    if not is_auth: return
    
    if context.user_data.get('awaiting_custom_cat'):
        context.user_data['selected_category'] = text
        context.user_data['awaiting_custom_cat'] = False
        context.user_data['awaiting_location'] = True
        await update.message.reply_text(f"✅ ক্যাটাগরি: **{text}**\n🌍 **এবার জায়গার নাম লিখে পাঠান:**")
        return

    if context.user_data.get('awaiting_location'):
        location = text
        category = context.user_data.get('selected_category', 'Businesses')
        context.user_data['target_query'] = f"{category} in {location}"
        context.user_data['awaiting_location'] = False
        
        keyboard = [[InlineKeyboardButton("🚀 স্ক্র্যাপিং শুরু করুন", callback_data='start_scraping')],[InlineKeyboardButton("🔙 ব্যাক", callback_data='main_menu')]]
        await update.message.reply_text(f"✅ **টার্গেট:** `{context.user_data['target_query']}`\nএখন শুরু করতে পারেন।", reply_markup=InlineKeyboardMarkup(keyboard))

# =========================================================
# 🌟 AIOHTTP SERVER ROUTES (To host HTML and Dynamic APIs)
# =========================================================

async def serve_index(request):
    """Serve the index.html from templates folder"""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, 'templates', 'index.html')
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return web.Response(text=content, content_type='text/html')
    except Exception as e:
        logger.error(f"HTML load error: {e}")
        return web.Response(text=f"<h1>Error loading HTML</h1><p>Ensure file is at: <b>templates/index.html</b></p>", status=404, content_type='text/html')

# 🌟 NEW: Dynamic API to fetch Web Config & Users
async def api_get_config(request):
    uid = request.query.get('uid')
    
    config = db.reference('bot_settings/web_config').get()
    if not config:
        config = DEFAULT_WEB_CONFIG
        db.reference('bot_settings/web_config').set(config)

    response_data = {"config": config}

    # If super admin, fetch all users for the dashboard
    if is_super_admin(uid):
        users = db.reference('bot_users').get() or {}
        # Fetch lead counts quickly (just getting lengths)
        for u_id, u_data in users.items():
            leads = db.reference(f'gmaps_leads/{u_id}').get() or {}
            u_data['active_leads'] = len([k for k, v in leads.items() if not v.get('is_deleted_by_user')])
        response_data['users'] = users

    return web.json_response({"status": "success", "data": response_data})

# 🌟 NEW: Dynamic API to Update Web Config (Packages, Rules)
async def api_update_config(request):
    data = await request.json()
    uid = data.get('uid')
    
    if not is_super_admin(uid):
        return web.json_response({"status": "error", "message": "Unauthorized"})
    
    new_config = data.get('config')
    if new_config:
        db.reference('bot_settings/web_config').set(new_config)
        return web.json_response({"status": "success"})
    return web.json_response({"status": "error"})

# 🌟 NEW: Dynamic API to Manage Users (Add Time, Delete)
async def api_manage_user(request):
    data = await request.json()
    uid = data.get('uid')
    
    if not is_super_admin(uid):
        return web.json_response({"status": "error"})
    
    action = data.get('action')
    target_uid = data.get('target_uid')
    
    if action == 'add_time':
        minutes = int(data.get('minutes', 0))
        user_ref = db.reference(f'bot_users/{target_uid}')
        user = user_ref.get()
        if user:
            current_ends = user.get('sub_ends')
            if current_ends:
                current_dt = datetime.fromisoformat(current_ends)
                if current_dt < datetime.now() and minutes > 0:
                    current_dt = datetime.now()
            else:
                current_dt = datetime.now()
            
            new_dt = current_dt + timedelta(minutes=minutes)
            user_ref.update({'sub_ends': new_dt.isoformat()})
            return web.json_response({"status": "success", "new_date": new_dt.strftime('%Y-%m-%d %I:%M %p')})
            
    elif action == 'delete_user':
        db.reference(f'bot_users/{target_uid}').delete()
        db.reference(f'gmaps_leads/{target_uid}').delete()
        return web.json_response({"status": "success"})
        
    elif action == 'clear_leads':
        db.reference(f'gmaps_leads/{target_uid}').delete()
        return web.json_response({"status": "success"})

    return web.json_response({"status": "error"})

async def main_async():
    app = Application.builder().token(TOKEN).build()
    
    global BOT_USERNAME
    try:
        await app.initialize()
        bot_info = await app.bot.get_me()
        BOT_USERNAME = bot_info.username
        logger.info(f"🤖 Bot Username set to: {BOT_USERNAME}")
    except Exception as e:
        logger.error(f"Error fetching bot info: {e}")

    asyncio.create_task(keep_alive_task(None))
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("approve_sub", approve_sub_cmd))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    
    if RENDER_URL:
        webhook_path = f"/{TOKEN[-10:]}"
        await app.bot.set_webhook(url=f"{RENDER_URL}{webhook_path}")
        
        web_app = web.Application()
        web_app['bot_instance'] = app.bot 
        
        async def telegram_webhook(request):
            data = await request.json()
            await app.update_queue.put(Update.de_json(data=data, bot=app.bot))
            return web.Response()
            
        web_app.router.add_post(webhook_path, telegram_webhook)
        web_app.router.add_get("/", serve_index)
        
        # 🌟 API Routes mapped
        web_app.router.add_get("/api/config", api_get_config)
        web_app.router.add_post("/api/admin/config", api_update_config)
        web_app.router.add_post("/api/admin/user", api_manage_user)
        
        runner = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
        
        logger.info(f"🌐 Server hosting HTML and APIs on port {PORT}")
        
        await app.start()
        await asyncio.Event().wait()
    else:
        logger.info("🤖 Bot running on polling mode...")
        app.run_polling()

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
