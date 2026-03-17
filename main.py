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
from datetime import datetime
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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
AI_MONITOR_CHAT_ID = os.environ.get('AI_MONITOR_CHAT_ID')
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')
FB_JSON = os.environ.get('FIREBASE_CREDENTIALS_JSON')
FB_URL = os.environ.get('FIREBASE_DATABASE_URL')
RENDER_URL = os.environ.get('RENDER_EXTERNAL_URL')
PORT = int(os.environ.get('PORT', '8080'))

# --- Global State ---
active_tasks = {} # user_id: task
recent_logs =[] # For AI Analysis

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

def is_authorized(uid):
    if is_super_admin(uid): return True
    user = db.reference(f'bot_users/{uid}').get()
    return bool(user)

async def send_log(context, user_name, user_id, action):
    """টিমের কাজের লগ নির্দিষ্ট গ্রুপে পাঠাবে এবং এআই এর জন্য সেভ রাখবে"""
    log_text = f"👤 **{user_name}** (`{user_id}`)\n📌 অ্যাকশন: {action}\n🕒 সময়: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    
    # Save to memory for AI
    recent_logs.append(log_text)
    if len(recent_logs) > 50: recent_logs.pop(0)
    
    # Send to Log Group
    if LOG_GROUP_ID:
        try:
            await context.bot.send_message(chat_id=LOG_GROUP_ID, text=log_text, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Log send error: {e}")

async def analyze_with_ai(context):
    """Groq AI দিয়ে লগ বিশ্লেষণ করে বাংলায় রিপোর্ট দেবে"""
    if not GROQ_API_KEY or not AI_MONITOR_CHAT_ID: return
    
    if not recent_logs:
        await context.bot.send_message(chat_id=AI_MONITOR_CHAT_ID, text="🤖 এআই রিপোর্ট: বিশ্লেষণ করার মতো কোনো নতুন লগ নেই।")
        return

    logs_text = "\n".join(recent_logs[-30:]) # Last 30 logs
    
    prompt = (
        "তুমি একজন Expert AI System Analyst. নিচে একটি Google Maps B2B Lead Scraper বটের সাম্প্রতিক ইউজার লগ দেওয়া হলো। "
        "আমার টিমের মেম্বাররা এই বট ব্যবহার করে লিড কালেক্ট করছে। লগগুলো বিশ্লেষণ করে বাংলায় একটি সুন্দর রিপোর্ট তৈরি করো। "
        "রিপোর্টে যা থাকবে:\n"
        "১. টিমের পারফরম্যান্স কেমন (কে বেশি কাজ করছে)।\n"
        "২. কোনো এরর বা অস্বাভাবিক কিছু হচ্ছে কি না।\n"
        "৩. সিস্টেম বা কাজের উন্নতি করার জন্য তোমার প্রফেশনাল সাজেশন।\n\n"
        f"লগ ডেটা:\n{logs_text}"
    )

    try:
        url = "https://api.groq.com/openai/v1/chat/completions"
        headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
        payload = {"model": "llama-3.3-70b-versatile", "messages":[{"role": "user", "content": prompt}], "temperature": 0.7}
        
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        if response.status_code == 200:
            ai_reply = response.json()['choices'][0]['message']['content']
            await context.bot.send_message(chat_id=AI_MONITOR_CHAT_ID, text=f"🧠 **AI System Analysis:**\n\n{ai_reply}", parse_mode='Markdown')
            recent_logs.clear() # Clear after analysis
    except Exception as e:
        logger.error(f"AI Error: {e}")

async def extract_email(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    html = await response.text()
                    emails = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', html)
                    valid_emails =[e for e in emails if not any(x in e.lower() for x in ['.png', '.jpg', 'sentry', 'example'])]
                    return valid_emails[0] if valid_emails else "N/A"
    except: pass
    return "N/A"

# --- Scraper Engine ---
async def scraper_worker(query, user_id, user_name, context):
    await send_log(context, user_name, user_id, f"স্ক্র্যাপিং শুরু করেছে: `{query}`")
    
    ref = db.reference(f'gmaps_leads/{user_id}')
    leads_found = 0

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
            page = await browser.new_page()
            
            search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
            await page.goto(search_url, timeout=60000)
            await page.wait_for_timeout(5000)
            
            scrollable_div = await page.query_selector('div[role="feed"]')
            if scrollable_div:
                for _ in range(5):
                    if user_id not in active_tasks: break # Stopped
                    await page.evaluate('(element) => element.scrollBy(0, 2000)', scrollable_div)
                    await page.wait_for_timeout(2000)
            
            places = await page.query_selector_all('a[href*="/maps/place/"]')
            place_urls =[]
            for place in places:
                url = await place.get_attribute('href')
                if url and url not in place_urls: place_urls.append(url)
            
            await context.bot.send_message(user_id, f"🔍 **{len(place_urls)}** টি বিজনেস পাওয়া গেছে। ফিল্টার করা হচ্ছে...")

            for url in place_urls:
                if user_id not in active_tasks: break # Stopped
                try:
                    await page.goto(url, timeout=30000)
                    await page.wait_for_timeout(2000)
                    
                    name_el = await page.query_selector('h1')
                    name = await name_el.inner_text() if name_el else "Unknown"
                    
                    rating_el = await page.query_selector('div[aria-label*="stars"]')
                    rating_text = await rating_el.get_attribute('aria-label') if rating_el else ""
                    rating = 0.0
                    if rating_text:
                        match = re.search(r'([\d\.]+)\s*stars', rating_text)
                        if match: rating = float(match.group(1))
                    
                    # Bad Rating Filter
                    if rating == 0.0 or rating > 4.0: continue
                        
                    phone_el = await page.query_selector('button[data-item-id^="phone:"]')
                    phone = await phone_el.get_attribute('aria-label') if phone_el else "N/A"
                    if phone != "N/A": phone = phone.replace("Phone:", "").strip()
                    
                    web_el = await page.query_selector('a[data-item-id="authority"]')
                    website = await web_el.get_attribute('href') if web_el else "N/A"
                    
                    email = await extract_email(website) if website != "N/A" else "N/A"
                    
                    # Duplicate Check (Per User)
                    safe_key = re.sub(r'\D', '', phone) if phone != "N/A" else re.sub(r'[^a-zA-Z0-9]', '', name)
                    if not safe_key: safe_key = str(int(time.time()))
                    
                    if ref.child(safe_key).get(): continue
                        
                    lead_data = {
                        'name': name, 'rating': rating, 'phone': phone, 
                        'email': email, 'website': website, 'query': query, 
                        'date': datetime.now().isoformat()
                    }
                    ref.child(safe_key).set(lead_data)
                    leads_found += 1
                except: continue
            
            await browser.close()
            
    except Exception as e:
        await send_log(context, user_name, user_id, f"❌ এরর খেয়েছে: {str(e)[:100]}")
        await context.bot.send_message(user_id, f"❌ স্ক্র্যাপিং এরর: {e}")
    
    if user_id in active_tasks: del active_tasks[user_id]
    await context.bot.send_message(user_id, f"✅ **স্ক্র্যাপিং সম্পন্ন!**\nনতুন লিড: **{leads_found}** টি।")
    await send_log(context, user_name, user_id, f"স্ক্র্যাপিং শেষ করেছে। নতুন লিড: {leads_found}")

# --- Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not is_authorized(uid):
        await update.message.reply_text("⛔ আপনি এই বট ব্যবহারের জন্য অনুমোদিত নন।")
        return

    keyboard = [[InlineKeyboardButton("🎯 টার্গেট সেট করুন", callback_data='set_target')],[InlineKeyboardButton("🚀 শুরু করুন", callback_data='start_scraping'), InlineKeyboardButton("🛑 বন্ধ করুন", callback_data='stop_scraping')],[InlineKeyboardButton("📊 আমার লিড চেক", callback_data='check_stats'), InlineKeyboardButton("📥 ডাউনলোড", callback_data='download_leads')]
    ]
    
    # Super Admin Panel
    if is_super_admin(uid):
        keyboard.append([InlineKeyboardButton("👑 সুপার অ্যাডমিন প্যানেল", callback_data='super_admin_panel')])

    await update.message.reply_text("🗺️ **Google Maps Scraper Dashboard**\n\nআপনার কাজ শুরু করতে নিচের বাটন ব্যবহার করুন:", reply_markup=InlineKeyboardMarkup(keyboard))

async def add_user_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not is_super_admin(uid): return
    try:
        new_id = context.args[0]
        name = " ".join(context.args[1:]) if len(context.args) > 1 else "Team Member"
        db.reference(f'bot_users/{new_id}').set({"name": name, "added_at": str(datetime.now())})
        await update.message.reply_text(f"✅ ইউজার `{new_id}` ({name}) সফলভাবে যুক্ত হয়েছে।")
        await send_log(context, "Super Admin", uid, f"নতুন ইউজার অ্যাড করেছে: {new_id}")
    except:
        await update.message.reply_text("❌ ব্যবহার: `/add_user <user_id> <name>`")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = str(update.effective_user.id)
    uname = update.effective_user.first_name
    await query.answer()
    
    if not is_authorized(uid): return

    if query.data == 'set_target':
        context.user_data['awaiting_target'] = True
        await query.message.reply_text("✍️ **ক্যাটাগরি এবং লোকেশন লিখে পাঠান:**\n(যেমন: `Plumbers in New York`)")
        
    elif query.data == 'start_scraping':
        target = context.user_data.get('target_query')
        if not target:
            await query.message.reply_text("⚠️ আগে '🎯 টার্গেট সেট করুন' এ ক্লিক করুন।")
            return
        if uid in active_tasks:
            await query.message.reply_text("⚠️ আপনার একটি কাজ অলরেডি চলছে!")
            return
            
        task = asyncio.create_task(scraper_worker(target, uid, uname, context))
        active_tasks[uid] = task
        await query.edit_message_text(f"🚀 **{target}** এর জন্য স্ক্র্যাপিং শুরু হচ্ছে...")
        
    elif query.data == 'stop_scraping':
        if uid in active_tasks:
            active_tasks[uid].cancel()
            del active_tasks[uid]
            await query.edit_message_text("🛑 স্ক্র্যাপিং বন্ধ করা হয়েছে।")
            await send_log(context, uname, uid, "স্ক্র্যাপিং স্টপ করেছে।")
            
    elif query.data == 'check_stats':
        leads = db.reference(f'gmaps_leads/{uid}').get() or {}
        await query.message.reply_text(f"📊 **আপনার ডেটাবেস:**\nমোট লিড: **{len(leads)}** টি।")
        await send_log(context, uname, uid, "নিজের লিড স্ট্যাটাস চেক করেছে।")
            
    elif query.data == 'download_leads':
        await send_log(context, uname, uid, "লিড ডাউনলোড রিকোয়েস্ট করেছে।")
        leads = db.reference(f'gmaps_leads/{uid}').get() or {}
        if not leads:
            await query.message.reply_text("⚠️ আপনার ডেটাবেস খালি!")
            return
            
        si = io.StringIO()
        cw = csv.writer(si)
        cw.writerow(['Business Name', 'Rating', 'Phone', 'Email', 'Website', 'Query', 'Date'])
        for key, v in leads.items():
            cw.writerow([v.get('name',''), v.get('rating',''), v.get('phone',''), v.get('email',''), v.get('website',''), v.get('query',''), v.get('date','')])
            
        output = io.BytesIO(si.getvalue().encode('utf-8'))
        output.name = f"My_Leads_{datetime.now().strftime('%Y%m%d')}.csv"
        await context.bot.send_document(uid, output, caption=f"✅ আপনার মোট {len(leads)} টি লিড।")

    # --- SUPER ADMIN PANEL ---
    elif query.data == 'super_admin_panel':
        if not is_super_admin(uid): return
        btns = [[InlineKeyboardButton("👥 টিম মেম্বারদের লিড দেখুন", callback_data='sa_view_users')],[InlineKeyboardButton("🧠 AI System Analysis", callback_data='sa_ai_analyze')]
        ]
        await query.message.reply_text("👑 **Super Admin Control**", reply_markup=InlineKeyboardMarkup(btns))
        
    elif query.data == 'sa_view_users':
        if not is_super_admin(uid): return
        users = db.reference('bot_users').get() or {}
        if not users:
            await query.message.reply_text("কোনো টিম মেম্বার নেই।")
            return
        
        for u_id, u_data in users.items():
            leads = db.reference(f'gmaps_leads/{u_id}').get() or {}
            btn = [[InlineKeyboardButton("🗑️ এই ইউজারের সব লিড মুছুন", callback_data=f'del_leads_{u_id}')]]
            await query.message.reply_text(f"👤 **{u_data.get('name')}** (`{u_id}`)\n📥 মোট লিড: {len(leads)}", reply_markup=InlineKeyboardMarkup(btn))

    elif query.data.startswith('del_leads_'):
        if not is_super_admin(uid): return
        target_uid = query.data.split('del_leads_')[1]
        db.reference(f'gmaps_leads/{target_uid}').delete()
        await query.message.reply_text(f"✅ ইউজার `{target_uid}` এর সব লিড ডিলিট করা হয়েছে।")
        await send_log(context, "Super Admin", uid, f"ইউজার {target_uid} এর লিড ডিলিট করেছে।")

    elif query.data == 'sa_ai_analyze':
        if not is_super_admin(uid): return
        await query.message.reply_text("🧠 এআই লগ বিশ্লেষণ করছে... দয়া করে অপেক্ষা করুন।")
        await analyze_with_ai(context)

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not is_authorized(uid): return
    
    if context.user_data.get('awaiting_target'):
        context.user_data['target_query'] = update.message.text.strip()
        context.user_data['awaiting_target'] = False
        await update.message.reply_text(f"✅ **টার্গেট সেট হয়েছে:** `{context.user_data['target_query']}`\nএখন '🚀 শুরু করুন' এ ক্লিক করুন।")
        await send_log(context, update.effective_user.first_name, uid, f"নতুন টার্গেট সেট করেছে: {context.user_data['target_query']}")

def main():
    app = Application.builder().token(TOKEN).build()
    app.job_queue.run_once(keep_alive_task, 5)
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("add_user", add_user_cmd)) # Super Admin Command
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    logger.info("🤖 Enterprise Maps Bot is running...")
    if RENDER_URL:
        app.run_webhook(listen="0.0.0.0", port=PORT, url_path=TOKEN[-10:], webhook_url=f"{RENDER_URL}/{TOKEN[-10:]}")
    else:
        app.run_polling()

if __name__ == "__main__":
    main()
