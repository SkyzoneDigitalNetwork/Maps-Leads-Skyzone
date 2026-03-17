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
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')
FB_JSON = os.environ.get('FIREBASE_CREDENTIALS_JSON')
FB_URL = os.environ.get('FIREBASE_DATABASE_URL')
RENDER_URL = os.environ.get('RENDER_EXTERNAL_URL')
PORT = int(os.environ.get('PORT', '8080'))

# --- Global State ---
active_tasks = {} # user_id: task
recent_logs =[] # For Autonomous AI Analysis

# --- Popular Categories ---
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

def is_authorized(uid):
    if is_super_admin(uid): return True
    user = db.reference(f'bot_users/{uid}').get()
    return bool(user)

async def keep_alive_task(context: ContextTypes.DEFAULT_TYPE):
    """রেন্ডার সার্ভারকে ২৪ ঘণ্টা সজাগ রাখার জন্য পিং করবে"""
    if not RENDER_URL: return
    while True:
        try:
            requests.get(RENDER_URL, timeout=10)
        except Exception: pass
        await asyncio.sleep(600)

async def analyze_with_ai(context, is_error=False):
    """Groq AI দিয়ে স্বয়ংক্রিয়ভাবে লগ বিশ্লেষণ করে গ্রুপে রিপোর্ট দেবে"""
    if not GROQ_API_KEY or not LOG_GROUP_ID: return
    if not recent_logs: return

    logs_text = "\n".join(recent_logs)
    recent_logs.clear() # Clear immediately to prevent duplicate triggers
    
    if is_error:
        prompt = (
            "তুমি একজন Expert AI System Analyst. নিচে একটি Google Maps B2B Lead Scraper বটের লগ দেওয়া হলো যেখানে একটি 'ERROR' বা ক্র্যাশ হয়েছে। "
            "লগটি বিশ্লেষণ করে বাংলায় একটি ইমার্জেন্সি রিপোর্ট তৈরি করো। "
            "রিপোর্টে যা থাকবে:\n"
            "১. ঠিক কী এরর হয়েছে এবং কেন হয়েছে।\n"
            "২. এটি সমাধানের জন্য ডেভেলপারকে কী করতে হবে (Step-by-step)।\n\n"
            f"লগ ডেটা:\n{logs_text}"
        )
    else:
        prompt = (
            "তুমি একজন Expert AI System Analyst. নিচে একটি Google Maps B2B Lead Scraper বটের সাম্প্রতিক ইউজার লগ দেওয়া হলো। "
            "আমার টিমের মেম্বাররা এই বট ব্যবহার করে লিড কালেক্ট করছে। লগগুলো বিশ্লেষণ করে বাংলায় একটি সুন্দর রিপোর্ট তৈরি করো। "
            "রিপোর্টে যা থাকবে:\n"
            "১. টিমের পারফরম্যান্স কেমন (কে বেশি কাজ করছে, কোন ক্যাটাগরিতে কাজ হচ্ছে)।\n"
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
            header = "🚨 **AI EMERGENCY ALERT:**" if is_error else "🧠 **AI Auto Analysis Report:**"
            await context.bot.send_message(chat_id=LOG_GROUP_ID, text=f"{header}\n\n{ai_reply}", parse_mode='Markdown')
    except Exception as e:
        logger.error(f"AI Error: {e}")

async def send_log(context, user_name, user_id, action):
    """টিমের কাজের লগ নির্দিষ্ট গ্রুপে পাঠাবে এবং এআই এর জন্য সেভ রাখবে"""
    log_text = f"👤 **{user_name}** (`{user_id}`)\n📌 অ্যাকশন: {action}\n🕒 সময়: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    
    # Send to Log Group
    if LOG_GROUP_ID:
        try:
            await context.bot.send_message(chat_id=LOG_GROUP_ID, text=log_text, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Log send error: {e}")

    # Save to memory for Autonomous AI
    recent_logs.append(log_text)
    
    # 🌟 AUTONOMOUS AI TRIGGER: এরর হলে সাথে সাথে, অথবা ১৫টি কাজের পর বিশ্লেষণ করবে
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
        error_msg = str(e)
        await send_log(context, user_name, user_id, f"❌ স্ক্র্যাপিং এরর খেয়েছে:\n`{error_msg[:200]}`")
        await context.bot.send_message(user_id, f"❌ স্ক্র্যাপিং এরর: দয়া করে কিছুক্ষণ পর আবার চেষ্টা করুন।")
        if user_id in active_tasks: del active_tasks[user_id]
        return # Stop execution here
    
    if user_id in active_tasks: del active_tasks[user_id]
    await context.bot.send_message(user_id, f"✅ **স্ক্র্যাপিং সম্পন্ন!**\nনতুন লিড: **{leads_found}** টি।")
    await send_log(context, user_name, user_id, f"স্ক্র্যাপিং শেষ করেছে। নতুন লিড: {leads_found}")

# --- Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not is_authorized(uid):
        await update.message.reply_text("⛔ আপনি এই বট ব্যবহারের জন্য অনুমোদিত নন। অ্যাডমিনের সাথে যোগাযোগ করুন।")
        return

    keyboard =[[InlineKeyboardButton("🎯 টার্গেট সেট করুন (ক্যাটাগরি)", callback_data='set_target')],[InlineKeyboardButton("🚀 শুরু করুন", callback_data='start_scraping'), InlineKeyboardButton("🛑 বন্ধ করুন", callback_data='stop_scraping')],[InlineKeyboardButton("📊 আমার লিড চেক", callback_data='check_stats'), InlineKeyboardButton("📥 ডাউনলোড", callback_data='download_leads')]
    ]
    
    if is_super_admin(uid):
        keyboard.append([InlineKeyboardButton("👑 সুপার অ্যাডমিন প্যানেল", callback_data='super_admin_panel')])

    await update.message.reply_text("🗺️ **Google Maps Scraper Dashboard**\n\nআপনার কাজ শুরু করতে নিচের বাটন ব্যবহার করুন:", reply_markup=InlineKeyboardMarkup(keyboard))

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = str(update.effective_user.id)
    uname = update.effective_user.first_name
    await query.answer()
    
    if not is_authorized(uid): return

    # --- Target Setting (Category -> Location) ---
    if query.data == 'set_target':
        # Show Categories
        keyboard =[]
        row =[]
        for i, cat in enumerate(CATEGORIES):
            row.append(InlineKeyboardButton(cat, callback_data=f'cat_{cat}'))
            if len(row) == 2 or i == len(CATEGORIES) - 1:
                keyboard.append(row)
                row =[]
        keyboard.append([InlineKeyboardButton("✍️ কাস্টম ক্যাটাগরি লিখব", callback_data='cat_custom')])
        
        await query.message.reply_text("📂 **প্রথমে একটি ক্যাটাগরি সিলেক্ট করুন:**", reply_markup=InlineKeyboardMarkup(keyboard))
        
    elif query.data.startswith('cat_'):
        selected_cat = query.data.split('cat_')[1]
        if selected_cat == 'custom':
            context.user_data['awaiting_custom_cat'] = True
            await query.message.reply_text("✍️ আপনার কাস্টম ক্যাটাগরি লিখে পাঠান (যেমন: Car Wash):")
        else:
            context.user_data['selected_category'] = selected_cat
            context.user_data['awaiting_location'] = True
            await query.message.reply_text(f"✅ ক্যাটাগরি সিলেক্ট হয়েছে: **{selected_cat}**\n\n🌍 **এবার জায়গার নাম লিখে পাঠান:**\n(যেমন: `Dhaka` বা `New York`)", parse_mode='Markdown')

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

    # --- SUPER ADMIN PANEL (Button Based) ---
    elif query.data == 'super_admin_panel':
        if not is_super_admin(uid): return
        btns = [[InlineKeyboardButton("➕ ইউজার অ্যাড করুন", callback_data='sa_add_user')],[InlineKeyboardButton("➖ ইউজার রিমুভ করুন", callback_data='sa_remove_user_list')],[InlineKeyboardButton("👥 ইউজার লিস্ট ও লিড", callback_data='sa_view_users')]
        ]
        await query.message.reply_text("👑 **Super Admin Control**\n(এখান থেকে আপনি ইউজার ও তাদের লিড কন্ট্রোল করতে পারবেন)", reply_markup=InlineKeyboardMarkup(btns))
        
    elif query.data == 'sa_add_user':
        if not is_super_admin(uid): return
        context.user_data['awaiting_new_user_data'] = True
        await query.message.reply_text("✍️ **নতুন ইউজারের আইডি এবং নাম লিখে পাঠান:**\nফরম্যাট: `User_ID Name`\nউদাহরণ: `123456789 Rahim`", parse_mode='Markdown')

    elif query.data == 'sa_remove_user_list':
        if not is_super_admin(uid): return
        users = db.reference('bot_users').get() or {}
        if not users:
            await query.message.reply_text("কোনো টিম মেম্বার নেই।")
            return
        
        keyboard =[]
        for u_id, u_data in users.items():
            keyboard.append([InlineKeyboardButton(f"❌ রিমুভ: {u_data.get('name')} ({u_id})", callback_data=f'rm_usr_{u_id}')])
        await query.message.reply_text("যাকে রিমুভ করতে চান তার নামের ওপর ক্লিক করুন:", reply_markup=InlineKeyboardMarkup(keyboard))

    elif query.data.startswith('rm_usr_'):
        if not is_super_admin(uid): return
        target_uid = query.data.split('rm_usr_')[1]
        db.reference(f'bot_users/{target_uid}').delete()
        await query.message.reply_text(f"✅ ইউজার `{target_uid}` কে সফলভাবে রিমুভ করা হয়েছে। সে আর বট ব্যবহার করতে পারবে না।")
        await send_log(context, "Super Admin", uid, f"ইউজার রিমুভ করেছে: {target_uid}")

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

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    text = update.message.text.strip()
    
    # --- Super Admin Adding User ---
    if context.user_data.get('awaiting_new_user_data') and is_super_admin(uid):
        parts = text.split(maxsplit=1)
        if len(parts) >= 1:
            new_id = parts[0]
            name = parts[1] if len(parts) > 1 else "Team Member"
            db.reference(f'bot_users/{new_id}').set({"name": name, "added_at": str(datetime.now())})
            await update.message.reply_text(f"✅ ইউজার `{new_id}` ({name}) সফলভাবে যুক্ত হয়েছে।")
            await send_log(context, "Super Admin", uid, f"নতুন ইউজার অ্যাড করেছে: {new_id}")
        context.user_data['awaiting_new_user_data'] = False
        return

    if not is_authorized(uid): return
    
    # --- Target Setting Logic ---
    if context.user_data.get('awaiting_custom_cat'):
        context.user_data['selected_category'] = text
        context.user_data['awaiting_custom_cat'] = False
        context.user_data['awaiting_location'] = True
        await update.message.reply_text(f"✅ ক্যাটাগরি: **{text}**\n🌍 **এবার জায়গার নাম লিখে পাঠান:**\n(যেমন: `Dhaka` বা `New York`)", parse_mode='Markdown')
        return

    if context.user_data.get('awaiting_location'):
        location = text
        category = context.user_data.get('selected_category', 'Businesses')
        
        # Combine Category and Location
        context.user_data['target_query'] = f"{category} in {location}"
        context.user_data['awaiting_location'] = False
        
        keyboard = [[InlineKeyboardButton("🚀 স্ক্র্যাপিং শুরু করুন", callback_data='start_scraping')]]
        await update.message.reply_text(
            f"✅ **ফাইনাল টার্গেট সেট হয়েছে:** `{context.user_data['target_query']}`\nএখন স্ক্র্যাপিং শুরু করতে পারেন।", 
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        await send_log(context, update.effective_user.first_name, uid, f"টার্গেট সেট করেছে: {context.user_data['target_query']}")

def main():
    app = Application.builder().token(TOKEN).build()
    app.job_queue.run_once(keep_alive_task, 5)
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    logger.info("🤖 Enterprise Maps Bot is running...")
    if RENDER_URL:
        app.run_webhook(listen="0.0.0.0", port=PORT, url_path=TOKEN[-10:], webhook_url=f"{RENDER_URL}/{TOKEN[-10:]}")
    else:
        app.run_polling()

if __name__ == "__main__":
    main()
