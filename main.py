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

# 🌟 WEB APP URL (GitHub Pages Link)
WEB_APP_URL = os.environ.get('WEB_APP_URL', 'https://your-username.github.io/skyzone-app/index.html')

# --- Global State ---
active_tasks = {} 
recent_logs =[] 

CATEGORIES =[
    "Restaurants", "IT Companies", "Hospitals", "Real Estate", 
    "Plumbers", "Gyms", "Hotels", "Coffee Shops", "Car Repair", "Dentists"
]

# --- Firebase Init ---
try:
    if not firebase_admin._apps:
        cred_dict = json.loads(FB_JSON) if isinstance(FB_JSON, str) and FB_JSON.startswith('{') else FB_JSON
        if isinstance(cred_dict, str) and os.path.exists(cred_dict):
            cred = credentials.Certificate(cred_dict)
        else:
            cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred, {'databaseURL': FB_URL})
        logger.info("🔥 Firebase Connected!")
except Exception as e:
    logger.error(f"❌ Firebase Init Error: {e}")

# --- Helper Functions ---
def is_super_admin(uid):
    return str(uid) == str(SUPER_ADMIN_ID)

def get_user_data(uid):
    if is_super_admin(uid): return {"name": "Super Admin", "sub_ends": (datetime.now() + timedelta(days=3650)).isoformat()}
    return db.reference(f'bot_users/{uid}').get()

def check_subscription(uid):
    if is_super_admin(uid): return True, None
    user = get_user_data(uid)
    if not user: return False, "not_found"
    
    sub_ends_str = user.get('sub_ends')
    if not sub_ends_str: return False, "expired"
    
    sub_ends = datetime.fromisoformat(sub_ends_str)
    if datetime.now() > sub_ends:
        return False, "expired"
    return True, sub_ends

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
    
    prompt = f"Expert AI System Analyst. Analyze these B2B Scraper logs and give a short Bengali report on team performance and errors:\n{logs_text}"
    try:
        url = "https://api.groq.com/openai/v1/chat/completions"
        headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
        payload = {"model": "llama-3.3-70b-versatile", "messages":[{"role": "user", "content": prompt}], "temperature": 0.7}
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        if response.status_code == 200:
            ai_reply = response.json()['choices'][0]['message']['content']
            await context.bot.send_message(chat_id=LOG_GROUP_ID, text=f"🧠 **AI Report:**\n\n{ai_reply}", parse_mode='Markdown')
    except: pass

async def send_log(context, user_name, user_id, action):
    log_text = f"👤 **{user_name}** (`{user_id}`)\n📌 অ্যাকশন: {action}\n🕒 সময়: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    if LOG_GROUP_ID:
        try: await context.bot.send_message(chat_id=LOG_GROUP_ID, text=log_text, parse_mode='Markdown')
        except: pass
    recent_logs.append(log_text)
    if "এরর" in action or len(recent_logs) >= 15:
        asyncio.create_task(analyze_with_ai(context, is_error=("এরর" in action)))

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
    await send_log(context, user_name, user_id, f"স্ক্র্যাপিং শুরু করেছে: `{query}`")
    
    # 🌟 Update Lifetime Searches
    if not is_super_admin(user_id):
        user_ref = db.reference(f'bot_users/{user_id}')
        current_searches = user_ref.child('lt_searches').get() or 0
        user_ref.update({'lt_searches': current_searches + 1})

    ref = db.reference(f'gmaps_leads/{user_id}')
    leads_found = 0

    status_msg = await context.bot.send_message(chat_id=user_id, text=f"🚀 **{query}** এর জন্য ম্যাপে খোঁজা হচ্ছে...\nদয়া করে অপেক্ষা করুন।", parse_mode='Markdown')

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
            place_urls =[await place.get_attribute('href') for place in places if await place.get_attribute('href')]
            place_urls = list(set(place_urls)) # Remove duplicates
            
            total_places = len(place_urls)
            await context.bot.edit_message_text(chat_id=user_id, message_id=status_msg.message_id, text=f"🔍 **{total_places}** টি বিজনেস পাওয়া গেছে।\nডেটা সেভ করা হচ্ছে...", parse_mode='Markdown')

            for idx, url in enumerate(place_urls):
                if user_id not in active_tasks: break
                if idx % 3 == 0 or idx == 0:
                    try: await context.bot.edit_message_text(chat_id=user_id, message_id=status_msg.message_id, text=f"⏳ **লাইভ স্ক্র্যাপিং...**\n🎯 টার্গেট: `{query}`\n🔍 পাওয়া গেছে: **{total_places}**\n🔄 চেক: **{idx}**\n✅ সেভ: **{leads_found}**", parse_mode='Markdown')
                    except: pass

                try:
                    await page.goto(url, timeout=30000)
                    await page.wait_for_timeout(3000) 
                    
                    name_el = await page.query_selector('h1')
                    name = await name_el.inner_text() if name_el else "Unknown"
                    
                    rating = 0.0
                    total_reviews = 0
                    rating_text = await page.evaluate('''() => {
                        let el = document.querySelector('span[aria-label*="stars"], div[aria-label*="stars"]');
                        if (el) return el.getAttribute('aria-label');
                        let fallback = document.querySelector('.F7nice');
                        return fallback ? fallback.innerText : '';
                    }''')
                    
                    if rating_text:
                        r_match = re.search(r'([\d\.]+)\s*stars?', rating_text, re.IGNORECASE)
                        if r_match: rating = float(r_match.group(1))
                        rev_match = re.search(r'([\d,]+)\s*reviews?', rating_text, re.IGNORECASE)
                        if rev_match: total_reviews = int(rev_match.group(1).replace(',', ''))

                    r5 = r4 = r3 = r2 = r1 = 0
                    try:
                        reviews_tab = await page.query_selector('button[aria-label*="Reviews"], button:has-text("Reviews")')
                        if reviews_tab:
                            await reviews_tab.click()
                            await page.wait_for_timeout(2000)
                            histogram_data = await page.evaluate('''() => {
                                let data = {5:0, 4:0, 3:0, 2:0, 1:0};
                                let elements = document.querySelectorAll('[aria-label*="stars,"]');
                                elements.forEach(el => {
                                    let match = el.getAttribute('aria-label').match(/(\d)\s*stars?,\s*([\d,]+)/i);
                                    if(match) data[parseInt(match[1])] = parseInt(match[2].replace(/,/g, ''));
                                });
                                return data;
                            }''')
                            r5, r4, r3, r2, r1 = histogram_data.get('5',0), histogram_data.get('4',0), histogram_data.get('3',0), histogram_data.get('2',0), histogram_data.get('1',0)
                    except: pass
                        
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
                        
                    # 🌟 Soft Delete Support: is_deleted_by_user = False
                    lead_data = {
                        'name': name, 'rating': rating, 'total_reviews': total_reviews,
                        'stars_5': r5, 'stars_4': r4, 'stars_3': r3, 'stars_2': r2, 'stars_1': r1,
                        'phone': phone, 'email': email, 'website': website, 'address': address,
                        'query': query, 'date': datetime.now().isoformat(),
                        'is_deleted_by_user': False 
                    }
                    ref.child(safe_key).set(lead_data)
                    leads_found += 1
                except: continue
            
            await browser.close()
            
    except Exception as e:
        await send_log(context, user_name, user_id, f"❌ স্ক্র্যাপিং এরর: {str(e)[:100]}")
        try: await context.bot.edit_message_text(chat_id=user_id, message_id=status_msg.message_id, text=f"❌ স্ক্র্যাপিং এরর।")
        except: pass
        if user_id in active_tasks: del active_tasks[user_id]
        return
    
    if user_id in active_tasks: del active_tasks[user_id]
    
    # 🌟 Update Lifetime Leads
    if not is_super_admin(user_id) and leads_found > 0:
        user_ref = db.reference(f'bot_users/{user_id}')
        current_leads = user_ref.child('lt_leads').get() or 0
        user_ref.update({'lt_leads': current_leads + leads_found})

    try:
        await context.bot.edit_message_text(
            chat_id=user_id, message_id=status_msg.message_id, 
            text=f"✅ **স্ক্র্যাপিং সম্পন্ন!**\n🎯 টার্গেট: `{query}`\n📥 নতুন লিড: **{leads_found}** টি।", parse_mode='Markdown'
        )
    except: pass
    await send_log(context, user_name, user_id, f"স্ক্র্যাপিং শেষ। নতুন লিড: {leads_found}")

# --- Menus ---
def get_main_menu(uid):
    keyboard = [[InlineKeyboardButton("🎯 টার্গেট সেট করুন", callback_data='set_target')],[InlineKeyboardButton("🚀 শুরু করুন", callback_data='start_scraping'), InlineKeyboardButton("🛑 বন্ধ করুন", callback_data='stop_scraping')],[InlineKeyboardButton("📥 আমার লিড ডাউনলোড", callback_data='download_leads')],[InlineKeyboardButton("🗑️ আমার প্যানেল ক্লিয়ার করুন", callback_data='soft_delete_leads')]
    ]
    
    # 🌟 Web App Button
    user_data = get_user_data(uid)
    lt_leads = user_data.get('lt_leads', 0) if user_data else 0
    lt_searches = user_data.get('lt_searches', 0) if user_data else 0
    sub_ends = user_data.get('sub_ends', '') if user_data else ''
    
    web_url = f"{WEB_APP_URL}?uid={uid}&name={urllib.parse.quote(user_data.get('name', 'User'))}&leads={lt_leads}&searches={lt_searches}&ends={sub_ends}"
    keyboard.insert(0,[InlineKeyboardButton("🌐 আমার প্রোফাইল ও স্ট্যাটাস (Web App)", web_app=WebAppInfo(url=web_url))])

    if is_super_admin(uid):
        keyboard.append([InlineKeyboardButton("👑 সুপার অ্যাডমিন প্যানেল", callback_data='super_admin_panel')])
    
    return InlineKeyboardMarkup(keyboard)

# --- Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    
    is_auth, sub_status = check_subscription(uid)
    if not is_auth:
        if sub_status == "not_found":
            await update.message.reply_text("⛔ আপনি অনুমোদিত নন। অ্যাডমিনের সাথে যোগাযোগ করুন।")
            return
        elif sub_status == "expired":
            packages = db.reference('bot_settings/packages').get() or "আপনার অ্যাকাউন্টের মেয়াদ শেষ। রিনিউ করতে অ্যাডমিনের সাথে যোগাযোগ করুন।"
            await update.message.reply_text(f"⚠️ **অ্যাকাউন্ট আপডেট করুন!**\n\nআপনার ট্রায়াল বা সাবস্ক্রিপশনের মেয়াদ শেষ হয়ে গেছে।\n\n{packages}", parse_mode='Markdown')
            return

    await update.message.reply_text("🗺️ **Google Maps Scraper Dashboard**\n\nআপনার কাজ শুরু করতে নিচের বাটন ব্যবহার করুন:", reply_markup=get_main_menu(uid))

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = str(update.effective_user.id)
    uname = update.effective_user.first_name
    await query.answer()
    
    # Check subscription on every button click
    is_auth, sub_status = check_subscription(uid)
    if not is_auth:
        packages = db.reference('bot_settings/packages').get() or "মেয়াদ শেষ। রিনিউ করুন।"
        await query.edit_message_text(f"⚠️ **অ্যাকাউন্ট আপডেট করুন!**\n\n{packages}", parse_mode='Markdown')
        return

    # --- Back to Main Menu ---
    if query.data == 'main_menu':
        await query.edit_message_text("🗺️ **Google Maps Scraper Dashboard**\n\nআপনার কাজ শুরু করতে নিচের বাটন ব্যবহার করুন:", reply_markup=get_main_menu(uid))
        return

    # --- Target Setting ---
    if query.data == 'set_target':
        keyboard = []
        row =[]
        for i, cat in enumerate(CATEGORIES):
            row.append(InlineKeyboardButton(cat, callback_data=f'cat_{cat}'))
            if len(row) == 2 or i == len(CATEGORIES) - 1:
                keyboard.append(row)
                row = []
        keyboard.append([InlineKeyboardButton("✍️ কাস্টম ক্যাটাগরি", callback_data='cat_custom')])
        keyboard.append([InlineKeyboardButton("🔙 ব্যাক", callback_data='main_menu')])
        await query.edit_message_text("📂 **প্রথমে একটি ক্যাটাগরি সিলেক্ট করুন:**", reply_markup=InlineKeyboardMarkup(keyboard))
        
    elif query.data.startswith('cat_'):
        selected_cat = query.data.split('cat_')[1]
        if selected_cat == 'custom':
            context.user_data['awaiting_custom_cat'] = True
            await query.message.reply_text("✍️ আপনার কাস্টম ক্যাটাগরি লিখে পাঠান (যেমন: Car Wash):")
        else:
            context.user_data['selected_category'] = selected_cat
            context.user_data['awaiting_location'] = True
            await query.message.reply_text(f"✅ ক্যাটাগরি: **{selected_cat}**\n🌍 **এবার জায়গার নাম লিখে পাঠান:**")

    # --- Scraping Controls ---
    elif query.data == 'start_scraping':
        target = context.user_data.get('target_query')
        if not target:
            await query.message.reply_text("⚠️ আগে '🎯 টার্গেট সেট করুন' এ ক্লিক করে ক্যাটাগরি ও লোকেশন দিন।")
            return
        if uid in active_tasks:
            await query.message.reply_text("⚠️ আপনার একটি কাজ অলরেডি চলছে!")
            return
        task = asyncio.create_task(scraper_worker(target, uid, uname, context))
        active_tasks[uid] = task
        
    elif query.data == 'stop_scraping':
        if uid in active_tasks:
            active_tasks[uid].cancel()
            del active_tasks[uid]
            await query.edit_message_text("🛑 স্ক্র্যাপিং বন্ধ করা হয়েছে।", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ব্যাক", callback_data='main_menu')]]))
            
    # --- Soft Delete (User Panel Clear) ---
    elif query.data == 'soft_delete_leads':
        leads_ref = db.reference(f'gmaps_leads/{uid}')
        leads = leads_ref.get() or {}
        count = 0
        for key, val in leads.items():
            if not val.get('is_deleted_by_user'):
                leads_ref.child(key).update({'is_deleted_by_user': True})
                count += 1
        await query.edit_message_text(f"🗑️ আপনার প্যানেল থেকে **{count}** টি লিড ক্লিয়ার করা হয়েছে। (এগুলো আর ডাউনলোডে আসবে না)", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ব্যাক", callback_data='main_menu')]]))
            
    # --- Download (Only Active Leads) ---
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

    # --- SUPER ADMIN PANEL ---
    elif query.data == 'super_admin_panel':
        if not is_super_admin(uid): return
        btns =[
            [InlineKeyboardButton("➕ ইউজার অ্যাড (24h Trial)", callback_data='sa_add_user')],
            [InlineKeyboardButton("⏳ ইউজার রিনিউ (30 Days)", callback_data='sa_renew_user')],[InlineKeyboardButton("📝 প্যাকেজ টেক্সট সেট করুন", callback_data='sa_set_package')],[InlineKeyboardButton("👥 ইউজার লিস্ট ও হার্ড ডিলিট", callback_data='sa_view_users')],[InlineKeyboardButton("🔙 ব্যাক", callback_data='main_menu')]
        ]
        await query.edit_message_text("👑 **Super Admin Control**", reply_markup=InlineKeyboardMarkup(btns))
        
    elif query.data == 'sa_add_user':
        if not is_super_admin(uid): return
        context.user_data['awaiting_new_user_data'] = True
        await query.message.reply_text("✍️ **নতুন ইউজারের আইডি এবং নাম লিখে পাঠান:**\nফরম্যাট: `User_ID Name`\n(ইউজার ২৪ ঘণ্টার ফ্রি ট্রায়াল পাবে)")

    elif query.data == 'sa_renew_user':
        if not is_super_admin(uid): return
        users = db.reference('bot_users').get() or {}
        keyboard =[]
        for u_id, u_data in users.items():
            keyboard.append([InlineKeyboardButton(f"⏳ রিনিউ: {u_data.get('name')} (30 Days)", callback_data=f'renew_usr_{u_id}')])
        keyboard.append([InlineKeyboardButton("🔙 ব্যাক", callback_data='super_admin_panel')])
        await query.edit_message_text("যাকে ৩০ দিনের জন্য রিনিউ করতে চান ক্লিক করুন:", reply_markup=InlineKeyboardMarkup(keyboard))

    elif query.data.startswith('renew_usr_'):
        if not is_super_admin(uid): return
        target_uid = query.data.split('renew_usr_')[1]
        new_date = (datetime.now() + timedelta(days=30)).isoformat()
        db.reference(f'bot_users/{target_uid}').update({'sub_ends': new_date})
        await query.edit_message_text(f"✅ ইউজার `{target_uid}` কে ৩০ দিনের জন্য রিনিউ করা হয়েছে।", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ব্যাক", callback_data='super_admin_panel')]]))

    elif query.data == 'sa_set_package':
        if not is_super_admin(uid): return
        context.user_data['awaiting_package_text'] = True
        await query.message.reply_text("✍️ **আপনার প্যাকেজ লিস্ট, দাম এবং পেমেন্ট নাম্বার লিখে পাঠান:**\n(এটি মেয়াদ শেষ হওয়া ইউজাররা দেখবে)")

    elif query.data == 'sa_view_users':
        if not is_super_admin(uid): return
        users = db.reference('bot_users').get() or {}
        if not users:
            await query.edit_message_text("কোনো টিম মেম্বার নেই।", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ব্যাক", callback_data='super_admin_panel')]]))
            return
        
        for u_id, u_data in users.items():
            leads = db.reference(f'gmaps_leads/{u_id}').get() or {}
            btn = [[InlineKeyboardButton("🗑️ হার্ড ডিলিট (সব লিড মুছুন)", callback_data=f'hard_del_{u_id}')]]
            await query.message.reply_text(f"👤 **{u_data.get('name')}** (`{u_id}`)\n📥 ডাটাবেসে মোট লিড: {len(leads)}", reply_markup=InlineKeyboardMarkup(btn))

    elif query.data.startswith('hard_del_'):
        if not is_super_admin(uid): return
        target_uid = query.data.split('hard_del_')[1]
        db.reference(f'gmaps_leads/{target_uid}').delete()
        await query.edit_message_text(f"✅ ইউজার `{target_uid}` এর সব লিড ডাটাবেস থেকে চিরতরে ডিলিট করা হয়েছে।", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ব্যাক", callback_data='super_admin_panel')]]))

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    text = update.message.text.strip()
    
    # --- Super Admin Actions ---
    if context.user_data.get('awaiting_new_user_data') and is_super_admin(uid):
        parts = text.split(maxsplit=1)
        if len(parts) >= 1:
            new_id = parts[0]
            name = parts[1] if len(parts) > 1 else "User"
            trial_ends = (datetime.now() + timedelta(days=1)).isoformat() # 24 Hours Trial
            db.reference(f'bot_users/{new_id}').set({"name": name, "sub_ends": trial_ends, "lt_searches": 0, "lt_leads": 0})
            await update.message.reply_text(f"✅ ইউজার `{new_id}` ({name}) ২৪ ঘণ্টার ট্রায়ালসহ যুক্ত হয়েছে।")
        context.user_data['awaiting_new_user_data'] = False
        return

    if context.user_data.get('awaiting_package_text') and is_super_admin(uid):
        db.reference('bot_settings/packages').set(text)
        await update.message.reply_text("✅ প্যাকেজ লিস্ট সেভ করা হয়েছে।")
        context.user_data['awaiting_package_text'] = False
        return

    # --- User Actions ---
    if not is_authorized(uid): return
    
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

def main():
    app = Application.builder().token(TOKEN).build()
    app.job_queue.run_once(keep_alive_task, 5)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    logger.info("🤖 SaaS Maps Bot is running...")
    if RENDER_URL: app.run_webhook(listen="0.0.0.0", port=PORT, url_path=TOKEN[-10:], webhook_url=f"{RENDER_URL}/{TOKEN[-10:]}")
    else: app.run_polling()

if __name__ == "__main__":
    main()
