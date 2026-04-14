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

# WEB_APP_URL - Safe trailing slash handling
WEB_APP_URL = RENDER_URL.rstrip('/') if RENDER_URL else f"http://localhost:{PORT}"

# --- Global State ---
active_tasks = {} 
recent_logs =[] 
BOT_USERNAME = "" 

# CORS Headers 
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, GET, OPTIONS, PUT, DELETE",
    "Access-Control-Allow-Headers": "Content-Type",
}

CATEGORIES =[
    "Restaurants", "IT Companies", "Hospitals", "Real Estate", 
    "Plumbers", "Gyms", "Hotels", "Coffee Shops", "Car Repair", "Dentists",
    "Lawyers", "Electricians", "Salons", "Pharmacies", "Supermarkets",
    "Travel Agencies", "Banks", "Mechanics", "Schools", "Architects"
]

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

async def keep_alive_task(context: ContextTypes.DEFAULT_TYPE=None):
    if not RENDER_URL: return
    while True:
        try: requests.get(RENDER_URL, timeout=10)
        except: pass
        await asyncio.sleep(600)

async def analyze_with_ai(bot, is_error=False):
    if not GROQ_API_KEY or not LOG_GROUP_ID: return
    if not recent_logs: return

    logs_text = "\n".join(recent_logs)
    recent_logs.clear()
    
    if is_error:
        prompt = f"Expert AI System Analyst. Analyze this Google Maps Scraper bot ERROR log and give an emergency English report:\n{logs_text}"
    else:
        prompt = f"Expert AI System Analyst. Analyze these B2B Scraper logs and give a short English report on team performance and errors:\n{logs_text}"

    try:
        url = "https://api.groq.com/openai/v1/chat/completions"
        headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
        payload = {"model": "llama-3.3-70b-versatile", "messages":[{"role": "user", "content": prompt}], "temperature": 0.7}
        
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        if response.status_code == 200:
            ai_reply = response.json()['choices'][0]['message']['content']
            header = "🚨 **AI EMERGENCY ALERT:**" if is_error else "🧠 **AI Auto Analysis Report:**"
            await bot.send_message(chat_id=LOG_GROUP_ID, text=f"{header}\n\n{ai_reply}", parse_mode='Markdown')
    except Exception as e:
        logger.error(f"AI Error: {e}")

async def send_log(bot, user_name, user_id, action):
    log_text = f"👤 **{user_name}** (`{user_id}`)\n📌 Action: {action}\n🕒 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    if LOG_GROUP_ID:
        try: await bot.send_message(chat_id=LOG_GROUP_ID, text=log_text, parse_mode='Markdown')
        except: pass

    recent_logs.append(log_text)
    if "Error" in action or "Error" in action or "Executable doesn't exist" in action or "❌" in action: 
        asyncio.create_task(analyze_with_ai(bot, is_error=True))
    elif len(recent_logs) >= 15: 
        asyncio.create_task(analyze_with_ai(bot, is_error=False))

async def extract_email(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    html = await response.text()
                    emails = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', html)
                    valid_emails =[e for e in emails if not any(x in e.lower() for x in['.png', '.jpg', 'sentry', 'example', 'wix'])]
                    return valid_emails[0] if valid_emails else "N/A"
    except: pass
    return "N/A"

# --- Scraper Engine ---
async def scraper_worker(query, user_id, user_name, bot):
    await send_log(bot, user_name, user_id, f"Started scraping: `{query}`")
    
    if not is_super_admin(user_id):
        user_ref = db.reference(f'bot_users/{user_id}')
        current_searches = user_ref.child('lt_searches').get() or 0
        user_ref.update({'lt_searches': current_searches + 1})

    ref = db.reference(f'gmaps_leads/{user_id}')
    leads_found = 0
    session_leads =[] 

    status_msg = await bot.send_message(
        chat_id=user_id, 
        text=f"🚀 Searching maps for **{query}**...\nPlease wait.", 
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
                text=f"🔍 **{total_places}** businesses found.\nSaving data...", parse_mode='Markdown'
            )

            for idx, url in enumerate(place_urls):
                if user_id not in active_tasks: break
                if idx % 3 == 0 or idx == 0:
                    try: 
                        await bot.edit_message_text(
                            chat_id=user_id, message_id=status_msg.message_id, 
                            text=f"⏳ **Live Scraping...**\n🎯 Target: `{query}`\n🔍 Found: **{total_places}**\n🔄 Checked: **{idx}**\n✅ Saved: **{leads_found}**", 
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

                    # --- Deep Extraction: Reviews Tab ---
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
                            
                            r5 = histogram_data.get('5', 0)
                            r4 = histogram_data.get('4', 0)
                            r3 = histogram_data.get('3', 0)
                            r2 = histogram_data.get('2', 0)
                            r1 = histogram_data.get('1', 0)
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
                        'gmaps_url': url, 
                        'query': query, 'date': datetime.now().isoformat(), 'is_deleted_by_user': False 
                    }
                    ref.child(safe_key).set(lead_data)
                    session_leads.append(lead_data) 
                    leads_found += 1
                except Exception: continue
            
            await browser.close()
            
    except Exception as e:
        await send_log(bot, user_name, user_id, f"❌ Error: {str(e)[:100]}")
        try: await bot.edit_message_text(chat_id=user_id, message_id=status_msg.message_id, text=f"❌ Scraping error. Please try again.")
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
            text=f"✅ **Scraping Completed!**\n🎯 Target: `{query}`\n📥 New Leads: **{leads_found}**.\n\nGenerating automatic file...", parse_mode='Markdown'
        )
    except: pass

    if session_leads:
        si = io.StringIO()
        cw = csv.writer(si)
        cw.writerow(['Business Name', 'Rating', 'Total Reviews', '5-Star', '4-Star', '3-Star', '2-Star', '1-Star', 'Phone', 'Email', 'Website', 'Address', 'Maps Link', 'Query', 'Date'])
        for v in session_leads:
            cw.writerow([
                v.get('name',''), v.get('rating',''), v.get('total_reviews',''),
                v.get('stars_5',''), v.get('stars_4',''), v.get('stars_3',''), v.get('stars_2',''), v.get('stars_1',''),
                v.get('phone',''), v.get('email',''), v.get('website',''), v.get('address',''), v.get('gmaps_url', ''), v.get('query',''), v.get('date','')
            ])
        output = io.BytesIO(si.getvalue().encode('utf-8'))
        output.name = f"Report_{query.replace(' ', '_')}_{datetime.now().strftime('%H%M%S')}.csv"
        await bot.send_document(user_id, output, caption=f"📊 **Auto Report:** `{query}`\nFound **{leads_found}** new leads in this session.")

    await send_log(bot, user_name, user_id, f"Scraping completed. New leads: {leads_found}")

# --- Menus ---
def get_main_menu(uid):
    user_data = get_user_data(uid)
    lt_leads = user_data.get('lt_leads', 0) if user_data else 0
    lt_searches = user_data.get('lt_searches', 0) if user_data else 0
    sub_ends = user_data.get('sub_ends', '') if user_data else ''
    team_limit = user_data.get('team_limit', 0) if user_data else 0
    team_added = user_data.get('team_added', 0) if user_data else 0
    role = 'admin' if is_super_admin(uid) else 'user'
    
    # Ensuring Web App URL is correctly formatted
    web_url = f"{WEB_APP_URL}/?uid={uid}&name={urllib.parse.quote(user_data.get('name', 'User') if user_data else 'User')}&leads={lt_leads}&searches={lt_searches}&ends={sub_ends}&role={role}&team_limit={team_limit}&team_added={team_added}&bot={BOT_USERNAME}"
    
    keyboard =[]
    
    if is_super_admin(uid):
        keyboard.append([InlineKeyboardButton("🌐 Admin Panel (Web App)", web_app=WebAppInfo(url=web_url))])
    else:
        keyboard.append([InlineKeyboardButton("🌐 Profile & Dashboard (Web App)", web_app=WebAppInfo(url=web_url))])
    
    hidden_btns = db.reference('bot_settings/hidden_buttons').get() or {}
    
    if is_super_admin(uid) or not hidden_btns.get('btn_target'):
        keyboard.append([InlineKeyboardButton("🎯 Set Target (Category)", callback_data='set_target')])
    if is_super_admin(uid) or not hidden_btns.get('btn_scrape'):
        keyboard.append([InlineKeyboardButton("🚀 Start", callback_data='start_scraping'), InlineKeyboardButton("🛑 Stop", callback_data='stop_scraping')])
    if is_super_admin(uid) or not hidden_btns.get('btn_download'):
        keyboard.append([InlineKeyboardButton("📥 Download My Leads", callback_data='download_leads')])
    if is_super_admin(uid) or not hidden_btns.get('btn_clear'):
        keyboard.append([InlineKeyboardButton("🗑️ Clear Panel", callback_data='soft_delete_leads')])
        
    if team_limit > 0 and (is_super_admin(uid) or not hidden_btns.get('btn_team')):
        keyboard.append([InlineKeyboardButton("➕ Add Member (Team)", callback_data='tl_add_member')])

    keyboard.append([InlineKeyboardButton("🔄 Refresh (Restart)", callback_data='refresh_bot')])

    if is_super_admin(uid):
        keyboard.append([InlineKeyboardButton("👑 Super Admin Control", callback_data='super_admin_panel')])
    
    return InlineKeyboardMarkup(keyboard)

def get_expired_menu(uid):
    user_data = get_user_data(uid)
    web_url = f"{WEB_APP_URL}/?uid={uid}&name={urllib.parse.quote(user_data.get('name', 'User') if user_data else 'User')}&ends=expired&role=user&bot={BOT_USERNAME}"
    return InlineKeyboardMarkup([[InlineKeyboardButton("🌐 Web App (Packages & Payment)", web_app=WebAppInfo(url=web_url))]])

async def show_toggle_menu(message_obj, uid):
    hidden = db.reference('bot_settings/hidden_buttons').get() or {}
    def get_icon(key): return "❌" if hidden.get(key) else "✅"

    btns = [[InlineKeyboardButton(f"{get_icon('btn_target')} Set Target", callback_data='tgl_btn_target')],[InlineKeyboardButton(f"{get_icon('btn_scrape')} Start/Stop", callback_data='tgl_btn_scrape')],[InlineKeyboardButton(f"{get_icon('btn_download')} Download Leads", callback_data='tgl_btn_download')],[InlineKeyboardButton(f"{get_icon('btn_clear')} Clear Panel", callback_data='tgl_btn_clear')],[InlineKeyboardButton(f"{get_icon('btn_team')} Add Team Member", callback_data='tgl_btn_team')],[InlineKeyboardButton("🔙 Back", callback_data='super_admin_panel')]
    ]
    if hasattr(message_obj, 'edit_text'):
        await message_obj.edit_text("👁️ **Button Hide/Show Control:**\n(❌ = Hidden, ✅ = Visible)", reply_markup=InlineKeyboardMarkup(btns))
    else:
        await message_obj.reply_text("👁️ **Button Hide/Show Control:**\n(❌ = Hidden, ✅ = Visible)", reply_markup=InlineKeyboardMarkup(btns))

# --- Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    uname = update.effective_user.first_name
    text = update.message.text.strip() if update.message else ""
    
    # 🌟 100% BULLETPROOF DEEP LINKING (ALL BUTTONS NOW WORK)
    if text.startswith('/start '):
        payload = text.replace('/start ', '').strip()

        if payload == 'do_scrape':
            req = db.reference(f'pending_requests/{uid}').get()
            if req and req.get('action') == 'scrape':
                query = req.get('query')
                db.reference(f'pending_requests/{uid}').delete() 
                
                is_auth, sub_status = check_subscription(uid)
                if not is_auth:
                    await update.message.reply_text("⚠️ **Your account has expired!**\n\nClick the '🌐 Web App' button to renew.", reply_markup=get_expired_menu(uid))
                    return
                    
                if uid in active_tasks:
                    await update.message.reply_text("⚠️ You already have a running task!")
                    return
                task = asyncio.create_task(scraper_worker(query, uid, uname, context.bot))
                active_tasks[uid] = task
            return

        elif payload == 'do_payment':
            req = db.reference(f'pending_requests/{uid}').get()
            if req and req.get('action') == 'payment':
                plan = req.get('plan', '')
                price = req.get('price', '')
                sender = req.get('sender', '')
                trxid = req.get('trxid', '')
                db.reference(f'pending_requests/{uid}').delete() 
                
                admin_msg = f"💰 **New Payment Received!**\n\n👤 **User:** {uname} (`{uid}`)\n📦 **Package:** {plan}\n💵 **Amount:** {price}\n📱 **Sender No:** `{sender}`\n🔢 **TrxID:** `{trxid}`\n\n✅ To approve, copy this command and send:\n`/approve_sub {uid} 30`"
                await context.bot.send_message(chat_id=SUPER_ADMIN_ID, text=admin_msg, parse_mode='Markdown')
                await update.message.reply_text("✅ Your payment request has been sent to the admin. Your account will be updated soon.")
            return

        # Handle Web App Admin Commands
        elif payload.startswith('admin_cmd_') and is_super_admin(uid):
            cmd = payload.replace('admin_cmd_', '')
            if cmd == 'add_user':
                context.user_data['add_user_step'] = 'name'
                context.user_data['add_user_is_tl'] = False
                await update.message.reply_text("✍️ **Enter new user's name:**")
            elif cmd == 'add_team_leader':
                context.user_data['add_user_step'] = 'name'
                context.user_data['add_user_is_tl'] = True
                await update.message.reply_text("✍️ **Enter Team Leader's name:**")
            elif cmd == 'manage_users':
                users = db.reference('bot_users').get() or {}
                if not users:
                    await update.message.reply_text("No users found.")
                    return
                keyboard =[[InlineKeyboardButton(f"👤 {u_data.get('name')} ({u_id})", callback_data=f'sa_usr_{u_id}')] for u_id, u_data in users.items()]
                await update.message.reply_text("Click a name to view user profile:", reply_markup=InlineKeyboardMarkup(keyboard))
            elif cmd == 'edit_packages':
                context.user_data['awaiting_package_text'] = True
                await update.message.reply_text("✍️ **Send your package text, prices, and payment numbers:**")
            elif cmd == 'toggle_bot_buttons':
                await show_toggle_menu(update.message, uid)
            return

    # Normal /start behavior
    context.user_data.clear()
    if uid in active_tasks:
        active_tasks[uid].cancel()
        del active_tasks[uid]

    is_auth, sub_status = check_subscription(uid)
    if not is_auth:
        if sub_status == "not_found": 
            btn = [[InlineKeyboardButton("📩 Request Access", callback_data='req_access')]]
            await update.message.reply_text(f"⛔ You are not authorized.\n\nYour UID: `{uid}`\nClick the button below to request access from the admin.", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(btn))
        elif sub_status == "expired": 
            await update.message.reply_text("⚠️ **Your account has expired!**\n\nClick the '🌐 Web App' button to renew.", reply_markup=get_expired_menu(uid))
        return

    await update.message.reply_text("🗺️ **Google Maps Scraper Dashboard**\n\nUse the buttons below to start:", reply_markup=get_main_menu(uid))

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
            await update.message.reply_text(f"✅ User {target_uid} extended by {days} days.")
            await context.bot.send_message(chat_id=target_uid, text=f"🎉 **Congratulations!**\nYour payment is verified and your account is extended by {days} days.\nYou can now start working.")
        else:
            await update.message.reply_text("❌ User not found.")
    except Exception as e:
        await update.message.reply_text("❌ Usage: `/approve_sub <user_id> <days>`")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = str(update.effective_user.id)
    uname = update.effective_user.first_name
    
    try: await query.answer()
    except: pass 
    
    await send_log(context.bot, uname, uid, f"Button Click: {query.data}")

    if query.data == 'req_access':
        admin_msg = f"🔔 **New Access Request!**\n\n👤 **Name:** {uname}\n🆔 **UID:** `{uid}`\n\n✅ Click to approve:"
        btn = [[InlineKeyboardButton("✅ Give 24h Trial", callback_data=f'sa_add_trial_{uid}')]]
        await context.bot.send_message(chat_id=SUPER_ADMIN_ID, text=admin_msg, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(btn))
        await query.edit_message_text("✅ Your request has been sent to the admin. Please wait.")
        return
        
    elif query.data.startswith('sa_add_trial_'):
        if not is_super_admin(uid): return
        target_uid = query.data.split('sa_add_trial_')[1]
        trial_ends = (datetime.now() + timedelta(days=1)).isoformat()
        db.reference(f'bot_users/{target_uid}').set({"name": "User", "sub_ends": trial_ends, "lt_searches": 0, "lt_leads": 0, "team_limit": 0, "team_added": 0})
        await query.edit_message_text(f"✅ User `{target_uid}` granted 24h trial.")
        await context.bot.send_message(chat_id=target_uid, text="🎉 Admin approved your request! Send /start to begin.")
        return

    is_auth, sub_status = check_subscription(uid)
    if not is_auth and query.data not in ['main_menu', 'refresh_bot']:
        await query.edit_message_text("⚠️ **Your account has expired!**", reply_markup=get_expired_menu(uid))
        return

    if query.data == 'refresh_bot':
        if uid in active_tasks:
            active_tasks[uid].cancel()
            del active_tasks[uid]
        context.user_data.clear()
        await query.edit_message_text("🔄 Everything has been reset. Start over:", reply_markup=get_main_menu(uid))
        return

    if query.data == 'main_menu':
        await query.edit_message_text("🗺️ **Google Maps Scraper Dashboard**", reply_markup=get_main_menu(uid))
        return

    if query.data == 'set_target':
        keyboard = []
        row =[]
        for i, cat in enumerate(CATEGORIES):
            row.append(InlineKeyboardButton(cat, callback_data=f'cat_{cat}'))
            if len(row) == 2 or i == len(CATEGORIES) - 1:
                keyboard.append(row)
                row = []
        keyboard.append([InlineKeyboardButton("✍️ Custom Category", callback_data='cat_custom')])
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data='main_menu')])
        await query.edit_message_text("📂 **First select a category:**", reply_markup=InlineKeyboardMarkup(keyboard))
        
    elif query.data == 'cat_custom':
        context.user_data['awaiting_custom_cat'] = True
        await query.message.reply_text("✍️ Send your custom category (e.g., Car Wash):")
        
    elif query.data.startswith('cat_'):
        selected_cat = query.data.split('cat_')[1]
        context.user_data['selected_category'] = selected_cat
        context.user_data['awaiting_location'] = True
        await query.message.reply_text(f"✅ Category: **{selected_cat}**\n🌍 **Now send the Location Name:**")

    elif query.data == 'start_scraping':
        target = context.user_data.get('target_query')
        if not target: 
            await query.message.reply_text("⚠️ Set target first!")
            return
        if uid in active_tasks: 
            await query.message.reply_text("⚠️ You already have a running task!")
            return
        task = asyncio.create_task(scraper_worker(target, uid, uname, context.bot))
        active_tasks[uid] = task
        
    elif query.data == 'stop_scraping':
        if uid in active_tasks:
            active_tasks[uid].cancel()
            del active_tasks[uid]
            await query.edit_message_text("🛑 Scraping has been stopped.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data='main_menu')]]))
            
    elif query.data == 'soft_delete_leads':
        leads_ref = db.reference(f'gmaps_leads/{uid}')
        leads = leads_ref.get() or {}
        count = 0
        for key, val in leads.items():
            if not val.get('is_deleted_by_user'):
                leads_ref.child(key).update({'is_deleted_by_user': True})
                count += 1
        await query.edit_message_text(f"🗑️ **{count}** leads cleared from your panel.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data='main_menu')]]))
            
    elif query.data == 'download_leads':
        leads = db.reference(f'gmaps_leads/{uid}').get() or {}
        active_leads = {k: v for k, v in leads.items() if not v.get('is_deleted_by_user')}
        if not active_leads:
            await query.message.reply_text("⚠️ No new leads available to download.")
            return
            
        si = io.StringIO()
        cw = csv.writer(si)
        cw.writerow(['Business Name', 'Rating', 'Total Reviews', '5-Star', '4-Star', '3-Star', '2-Star', '1-Star', 'Phone', 'Email', 'Website', 'Address', 'Maps Link', 'Query', 'Date'])
        for key, v in active_leads.items():
            cw.writerow([
                v.get('name',''), v.get('rating',''), v.get('total_reviews',''),
                v.get('stars_5',''), v.get('stars_4',''), v.get('stars_3',''), v.get('stars_2',''), v.get('stars_1',''),
                v.get('phone',''), v.get('email',''), v.get('website',''), v.get('address',''), v.get('gmaps_url', ''), v.get('query',''), v.get('date','')
            ])
        output = io.BytesIO(si.getvalue().encode('utf-8'))
        output.name = f"My_Leads_{datetime.now().strftime('%Y%m%d')}.csv"
        await context.bot.send_document(uid, output, caption=f"✅ Total {len(active_leads)} leads downloaded.")

    elif query.data == 'tl_add_member':
        user_data = get_user_data(uid)
        limit = user_data.get('team_limit', 0)
        added = user_data.get('team_added', 0)
        if added >= limit:
            await query.edit_message_text("⚠️ Your team limit is reached.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data='main_menu')]]))
            return
        
        context.user_data['add_user_step'] = 'name'
        context.user_data['add_user_is_tl'] = False
        context.user_data['add_user_parent'] = uid
        await query.message.reply_text(f"✍️ **Enter new member's name:**\n(Remaining limit: {limit - added})")

    elif query.data == 'super_admin_panel':
        if not is_super_admin(uid): return
        btns = [[InlineKeyboardButton("➕ Add User (24h)", callback_data='sa_add_user'), InlineKeyboardButton("👥 Add Team Leader", callback_data='sa_add_tl')],[InlineKeyboardButton("👥 User List & Control", callback_data='sa_view_users')],[InlineKeyboardButton("👁️ Toggle Buttons", callback_data='sa_toggle_menu')],[InlineKeyboardButton("🔙 Back", callback_data='main_menu')]
        ]
        await query.edit_message_text("👑 **Super Admin Control**", reply_markup=InlineKeyboardMarkup(btns))
        
    elif query.data == 'sa_add_user':
        if not is_super_admin(uid): return
        context.user_data['add_user_step'] = 'name'
        context.user_data['add_user_is_tl'] = False
        await query.message.reply_text("✍️ **Enter new user's name:**")

    elif query.data == 'sa_add_tl':
        if not is_super_admin(uid): return
        context.user_data['add_user_step'] = 'name'
        context.user_data['add_user_is_tl'] = True
        await query.message.reply_text("✍️ **Enter Team Leader's name:**")

    elif query.data == 'sa_toggle_menu':
        if not is_super_admin(uid): return
        await show_toggle_menu(query.message, uid)

    elif query.data.startswith('tgl_'):
        if not is_super_admin(uid): return
        key = query.data.split('tgl_')[1]
        current = db.reference(f'bot_settings/hidden_buttons/{key}').get()
        db.reference(f'bot_settings/hidden_buttons/{key}').set(not current)
        await show_toggle_menu(query.message, uid)

    elif query.data == 'sa_view_users':
        if not is_super_admin(uid): return
        users = db.reference('bot_users').get() or {}
        if not users:
            await query.edit_message_text("No users found.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data='super_admin_panel')]]))
            return
        
        keyboard =[]
        for u_id, u_data in users.items():
            keyboard.append([InlineKeyboardButton(f"👤 {u_data.get('name')} ({u_id})", callback_data=f'sa_usr_{u_id}')])
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data='super_admin_panel')])
        await query.edit_message_text("Click a name to view user profile:", reply_markup=InlineKeyboardMarkup(keyboard))

    elif query.data.startswith('sa_usr_'):
        if not is_super_admin(uid): return
        target_uid = query.data.split('sa_usr_')[1]
        user_data = db.reference(f'bot_users/{target_uid}').get()
        if not user_data: return
        
        leads = db.reference(f'gmaps_leads/{target_uid}').get() or {}
        sub_end_str = user_data.get('sub_ends', 'N/A')
        if sub_end_str != 'N/A':
            sub_end_dt = datetime.fromisoformat(sub_end_str)
            status = "✅ Active" if datetime.now() < sub_end_dt else "❌ Expired"
            sub_end_formatted = sub_end_dt.strftime('%Y-%m-%d')
        else:
            sub_end_formatted = "N/A"
            status = "Unknown"

        profile_text = (
            f"👤 **User Profile:** {user_data.get('name')}\n"
            f"🆔 **ID:** `{target_uid}`\n"
            f"📊 **Status:** {status}\n"
            f"⏳ **Expiry:** {sub_end_formatted}\n"
            f"👥 **Team Limit:** {user_data.get('team_added',0)} / {user_data.get('team_limit',0)}\n\n"
            f"🔍 **Lifetime Searches:** {user_data.get('lt_searches', 0)}\n"
            f"📥 **Lifetime Leads:** {user_data.get('lt_leads', 0)}\n"
            f"📂 **Leads in Database:** {len(leads)}"
        )

        btns = [[InlineKeyboardButton("⏳ Edit Time (+/- Days)", callback_data=f'sa_add_days_{target_uid}')],[InlineKeyboardButton("🗑️ Hard Delete Leads", callback_data=f'hard_del_{target_uid}')],[InlineKeyboardButton("❌ Remove User", callback_data=f'rm_usr_{target_uid}')],[InlineKeyboardButton("🔙 Back", callback_data='sa_view_users')]
        ]
        await query.edit_message_text(profile_text, reply_markup=InlineKeyboardMarkup(btns), parse_mode='Markdown')

    elif query.data.startswith('sa_add_days_'):
        if not is_super_admin(uid): return
        target_uid = query.data.split('sa_add_days_')[1]
        context.user_data['awaiting_days_for'] = target_uid
        await query.message.reply_text("✍️ **How many days to add/remove?**\n(E.g., 30 to add, -10 to remove):")

    elif query.data.startswith('hard_del_'):
        if not is_super_admin(uid): return
        target_uid = query.data.split('hard_del_')[1]
        db.reference(f'gmaps_leads/{target_uid}').delete()
        await query.edit_message_text(f"✅ All leads of user `{target_uid}` permanently deleted.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f'sa_usr_{target_uid}')]]))

    elif query.data.startswith('rm_usr_'):
        if not is_super_admin(uid): return
        target_uid = query.data.split('rm_usr_')[1]
        db.reference(f'bot_users/{target_uid}').delete()
        db.reference(f'gmaps_leads/{target_uid}').delete() 
        await query.edit_message_text(f"✅ User `{target_uid}` removed successfully.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data='sa_view_users')]]))

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    text = update.message.text.strip()
    
    if context.user_data.get('add_user_step') == 'name':
        context.user_data['add_user_name'] = text
        context.user_data['add_user_step'] = 'uid'
        await update.message.reply_text("✍️ **Now enter User Telegram ID (UID):**")
        return
        
    elif context.user_data.get('add_user_step') == 'uid':
        uid_input = text
        name = context.user_data['add_user_name']
        is_tl = context.user_data.get('add_user_is_tl', False)
        parent_id = context.user_data.get('add_user_parent', None)
        
        if is_tl:
            context.user_data['add_user_uid'] = uid_input
            context.user_data['add_user_step'] = 'limit'
            await update.message.reply_text("✍️ **Now enter Team Member Limit (Number):**")
            return
        elif parent_id:
            user_data = get_user_data(parent_id)
            sub_ends = user_data.get('sub_ends')
            added = user_data.get('team_added', 0)
            db.reference(f'bot_users/{uid_input}').set({"name": name, "sub_ends": sub_ends, "lt_searches": 0, "lt_leads": 0, "parent_id": parent_id})
            db.reference(f'bot_users/{parent_id}').update({"team_added": added + 1})
            await update.message.reply_text(f"✅ Team member `{uid_input}` ({name}) added.")
            context.user_data.clear()
            return
        else:
            trial_ends = (datetime.now() + timedelta(days=1)).isoformat() 
            db.reference(f'bot_users/{uid_input}').set({"name": name, "sub_ends": trial_ends, "lt_searches": 0, "lt_leads": 0, "team_limit": 0, "team_added": 0})
            await update.message.reply_text(f"✅ User `{uid_input}` ({name}) added with 24h trial.")
            context.user_data.clear()
            return
            
    elif context.user_data.get('add_user_step') == 'limit':
        try:
            limit = int(text)
            uid_input = context.user_data['add_user_uid']
            name = context.user_data['add_user_name']
            trial_ends = (datetime.now() + timedelta(days=30)).isoformat() 
            db.reference(f'bot_users/{uid_input}').set({"name": name, "sub_ends": trial_ends, "lt_searches": 0, "lt_leads": 0, "team_limit": limit, "team_added": 0})
            await update.message.reply_text(f"✅ Team Leader `{uid_input}` ({name}) added. Limit: {limit}.")
            context.user_data.clear()
        except ValueError:
            await update.message.reply_text("❌ Please enter numbers only.")
        return

    if context.user_data.get('awaiting_days_for') and is_super_admin(uid):
        target_uid = context.user_data['awaiting_days_for']
        try:
            days_to_add = int(text) 
            user_ref = db.reference(f'bot_users/{target_uid}')
            user_data = user_ref.get()
            if user_data:
                current_end_str = user_data.get('sub_ends')
                if current_end_str:
                    current_end = datetime.fromisoformat(current_end_str)
                    if current_end < datetime.now() and days_to_add > 0: current_end = datetime.now() 
                else:
                    current_end = datetime.now()
                
                new_end = current_end + timedelta(days=days_to_add)
                user_ref.update({'sub_ends': new_end.isoformat()})
                await update.message.reply_text(f"✅ User `{target_uid}` expiry updated. New date: {new_end.strftime('%Y-%m-%d')}")
            else:
                await update.message.reply_text("❌ User not found.")
        except ValueError:
            await update.message.reply_text("❌ Please enter numbers only (e.g., 30 or -10).")
        context.user_data['awaiting_days_for'] = None
        return

    is_auth, sub_status = check_subscription(uid)
    if not is_auth: return
    
    if context.user_data.get('awaiting_custom_cat'):
        context.user_data['selected_category'] = text
        context.user_data['awaiting_custom_cat'] = False
        context.user_data['awaiting_location'] = True
        await update.message.reply_text(f"✅ Category: **{text}**\n🌍 **Now send the Location Name:**")
        return

    if context.user_data.get('awaiting_location'):
        location = text
        category = context.user_data.get('selected_category', 'Businesses')
        context.user_data['target_query'] = f"{category} in {location}"
        context.user_data['awaiting_location'] = False
        
        keyboard = [[InlineKeyboardButton("🚀 Start", callback_data='start_scraping')],[InlineKeyboardButton("🔙 Back", callback_data='main_menu')]]
        await update.message.reply_text(f"✅ **Target:** `{context.user_data['target_query']}`\nYou can start now.", reply_markup=InlineKeyboardMarkup(keyboard))

# --- SERVER FOR HTML ---
async def serve_index(request):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Check both root and templates folder to prevent 404 Error!
    path1 = os.path.join(base_dir, 'index.html')
    path2 = os.path.join(base_dir, 'templates', 'index.html')
    
    file_path = None
    if os.path.exists(path1): file_path = path1
    elif os.path.exists(path2): file_path = path2
        
    if file_path:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            return web.Response(text=content, content_type='text/html')
        except Exception as e:
            logger.error(f"HTML read error: {e}")
            return web.Response(text=f"<h1>Error reading file</h1><p>{str(e)}</p>", status=500, content_type='text/html')
    else:
        logger.error("404 Error: index.html not found!")
        return web.Response(text="<h1>404 Not Found!</h1><p>index.html file is missing! Please upload it to GitHub in the main folder or templates folder.</p>", status=404, content_type='text/html')

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

    asyncio.create_task(keep_alive_task())
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("approve_sub", approve_sub_cmd))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    
    if RENDER_URL:
        webhook_path = f"/{TOKEN[-10:]}"
        await app.bot.set_webhook(url=f"{RENDER_URL}{webhook_path}")
        
        async def telegram_webhook(request):
            data = await request.json()
            await app.update_queue.put(Update.de_json(data=data, bot=app.bot))
            return web.Response()

        web_app = web.Application()
        web_app.router.add_post(webhook_path, telegram_webhook)
        web_app.router.add_get("/", serve_index)
        
        runner = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
        
        logger.info(f"🌐 Server hosting HTML on port {PORT}")
        await app.start()
        await asyncio.Event().wait()
    else:
        logger.info("🤖 Bot running on polling mode...")
        app.run_polling()

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
