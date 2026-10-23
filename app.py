import os
import time
import random
import threading
import psycopg2
import psycopg2.extras
from flask import Flask, render_template_string, request
from flask_socketio import SocketIO, emit
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'btc-game-secret-2026')
socketio = SocketIO(app, cors_allowed_origins="*")

DATABASE_URL = os.getenv('DATABASE_URL')

def get_db():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

# --- Database Initialization & Schema Updates ---
def init_db():
    if not DATABASE_URL:
        print("WARNING: DATABASE_URL not set!")
        return
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            cash DOUBLE PRECISION DEFAULT 100000.0,
            btc DOUBLE PRECISION DEFAULT 0.0,
            total_trades INTEGER DEFAULT 0,
            winning_trades INTEGER DEFAULT 0,
            total_volume DOUBLE PRECISION DEFAULT 0.0,
            highest_net_worth DOUBLE PRECISION DEFAULT 100000.0
        );
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            order_type TEXT NOT NULL,
            action TEXT NOT NULL,
            target_price DOUBLE PRECISION NOT NULL,
            usd_val DOUBLE PRECISION NOT NULL
        );
        CREATE TABLE IF NOT EXISTS trades_history (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            action TEXT NOT NULL,
            price DOUBLE PRECISION NOT NULL,
            amount_usd DOUBLE PRECISION NOT NULL,
            btc_amount DOUBLE PRECISION NOT NULL,
            timestamp TEXT NOT NULL
        );
    ''')
    conn.commit()
    conn.close()

if DATABASE_URL:
    init_db()

# --- Market State ---
BTC_PRICE = 65000.0
CANDLE_HISTORY = []
ACTIVE_PLAYERS = {}  
TRADE_FEED = []

def seed_initial_candles():
    global BTC_PRICE, CANDLE_HISTORY
    base_time = int(time.time()) - (30 * 15)
    temp_price = 65000.0
    for i in range(30):
        c_open = temp_price
        change = random.uniform(-0.02, 0.02)
        c_close = round(max(100.0, c_open * (1 + change)), 2)
        c_high = round(max(c_open, c_close) * (1 + random.uniform(0.001, 0.005)), 2)
        c_low = round(min(c_open, c_close) * (1 - random.uniform(0.001, 0.005)), 2)
        
        CANDLE_HISTORY.append({
            'time': base_time + (i * 15),
            'open': c_open, 'high': c_high, 'low': c_low, 'close': c_close
        })
        temp_price = c_close
    BTC_PRICE = temp_price

seed_initial_candles()

NEWS_EVENTS = [
    {"headline": "🚨 Federal Reserve cuts interest rates unexpectedly!", "impact": 0.08},
    {"headline": "💥 Major crypto exchange halts withdrawals citing insolvency!", "impact": -0.12},
    {"headline": "🚀 Tech titan announces $2B Bitcoin purchase for corporate treasury!", "impact": 0.10},
    {"headline": "⚠️ Regulatory body announces strict crackdown on digital assets!", "impact": -0.09},
    {"headline": "🟢 Global energy grid adopts Bitcoin mining for surplus power!", "impact": 0.06},
    {"headline": "📉 Inflation data comes in higher than expected!", "impact": -0.05}
]

def add_feed_entry(text, entry_type="info"):
    entry = {"time": time.strftime('%H:%M:%S'), "text": text, "type": entry_type}
    TRADE_FEED.insert(0, entry)
    if len(TRADE_FEED) > 20:
        TRADE_FEED.pop()

# --- Postgres Helpers ---
def save_user_portfolio(user_id, cash, btc, net_worth):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE users 
        SET cash = %s, btc = %s, highest_net_worth = GREATEST(highest_net_worth, %s) 
        WHERE id = %s
    ''', (cash, btc, net_worth, user_id))
    conn.commit()
    conn.close()

def db_log_trade(user_id, action, price, usd_val, btc_amount, is_win=False):
    conn = get_db()
    cursor = conn.cursor()
    now = time.strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute('''
        INSERT INTO trades_history (user_id, action, price, amount_usd, btc_amount, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
    ''', (user_id, action, price, usd_val, btc_amount, now))
    
    win_inc = 1 if is_win else 0
    cursor.execute('''
        UPDATE users 
        SET total_trades = total_trades + 1,
            winning_trades = winning_trades + %s,
            total_volume = total_volume + %s
        WHERE id = %s
    ''', (win_inc, usd_val, user_id))
    conn.commit()
    conn.close()

def db_get_analytics(user_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT cash, btc, total_trades, winning_trades, total_volume, highest_net_worth 
        FROM users WHERE id = %s
    ''', (user_id,))
    u = cursor.fetchone()
    
    cursor.execute('''
        SELECT action, price, amount_usd, btc_amount, timestamp 
        FROM trades_history WHERE user_id = %s ORDER BY id DESC LIMIT 10
    ''', (user_id,))
    history = cursor.fetchall()
    conn.close()

    if not u: return {}
    cash, btc, total_trades, winning_trades, total_volume, highest_nw = u
    win_rate = round((winning_trades / total_trades * 100), 1) if total_trades > 0 else 0.0

    return {
        'cash': cash, 'btc': btc, 'total_trades': total_trades,
        'winning_trades': winning_trades, 'win_rate': win_rate,
        'total_volume': round(total_volume, 2), 'highest_net_worth': round(highest_nw, 2),
        'history': [{'action': h[0], 'price': h[1], 'usd': h[2], 'btc': h[3], 'time': h[4]} for h in history]
    }

def db_add_order(user_id, order_type, action, target_price, usd_val):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO orders (user_id, order_type, action, target_price, usd_val)
        VALUES (%s, %s, %s, %s, %s) RETURNING id
    ''', (user_id, order_type, action, target_price, usd_val))
    order_id = cursor.fetchone()[0]
    conn.commit()
    conn.close()
    return order_id

def db_remove_order(order_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM orders WHERE id = %s', (order_id,))
    conn.commit()
    conn.close()

def db_get_orders(user_id):
    conn = get_db()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute('SELECT id, order_type, action, target_price, usd_val FROM orders WHERE user_id = %s', (user_id,))
    rows = cursor.fetchall()
    conn.close()
    return [{'id': r['id'], 'type': r['order_type'], 'action': r['action'], 'price': r['target_price'], 'usd_val': r['usd_val']} for r in rows]

# --- Background Market Simulation Thread ---
def btc_price_simulation():
    global BTC_PRICE, CANDLE_HISTORY
    while True:
        time.sleep(15.0)
        if not DATABASE_URL: continue
        
        active_news = None
        if random.random() < 0.20:
            news_item = random.choice(NEWS_EVENTS)
            percentage_change = news_item["impact"] + random.uniform(-0.01, 0.01)
            active_news = news_item["headline"]
            add_feed_entry(f"NEWS: {active_news}", "news")
        else:
            percentage_change = random.uniform(-0.025, 0.025)

        c_open = BTC_PRICE
        c_close = round(max(100.0, c_open * (1 + percentage_change)), 2)
        c_high = round(max(c_open, c_close) * (1 + random.uniform(0.001, 0.005)), 2)
        c_low = round(min(c_open, c_close) * (1 - random.uniform(0.001, 0.005)), 2)
        
        BTC_PRICE = c_close
        candle_time = int(time.time())
        
        new_candle = {'time': candle_time, 'open': c_open, 'high': c_high, 'low': c_low, 'close': c_close}
        CANDLE_HISTORY.append(new_candle)
        if len(CANDLE_HISTORY) > 60: CANDLE_HISTORY.pop(0)

        # Process Limit & Stop-Loss Orders
        for sid, p in list(ACTIVE_PLAYERS.items()):
            executed_indices = []
            for idx, order in enumerate(p['pending_orders']):
                target, action, order_type, usd_val = order['price'], order['action'], order['type'], order['usd_val']

                triggered = False
                if order_type == 'limit' and action == 'buy' and BTC_PRICE <= target: triggered = True
                elif order_type == 'limit' and action == 'sell' and BTC_PRICE >= target: triggered = True
                elif order_type == 'stop_loss' and action == 'sell' and BTC_PRICE <= target: triggered = True

                if triggered:
                    executed_indices.append(idx)
                    db_remove_order(order['id'])
                    
                    if action == 'buy' and p['cash'] >= usd_val:
                        btc_bought = round(usd_val / BTC_PRICE, 6)
                        p['cash'] -= usd_val
                        p['btc'] += btc_bought
                        p['buy_avg_price'] = BTC_PRICE
                        p['net_worth'] = round(p['cash'] + (p['btc'] * BTC_PRICE), 2)
                        save_user_portfolio(p['user_id'], p['cash'], p['btc'], p['net_worth'])
                        db_log_trade(p['user_id'], 'BUY', BTC_PRICE, usd_val, btc_bought)
                        add_feed_entry(f"🤖 {p['username']}'s {order_type.upper()} BUY executed @ ${BTC_PRICE:,.2f}", "buy")
                    elif action == 'sell' and p['btc'] > 0:
                        revenue = round(p['btc'] * BTC_PRICE, 2)
                        is_win = BTC_PRICE > p.get('buy_avg_price', 0)
                        p['cash'] += revenue
                        p['btc'] = 0.0
                        p['net_worth'] = round(p['cash'], 2)
                        save_user_portfolio(p['user_id'], p['cash'], p['btc'], p['net_worth'])
                        db_log_trade(p['user_id'], 'SELL', BTC_PRICE, revenue, p['btc'], is_win=is_win)
                        add_feed_entry(f"🤖 {p['username']}'s {order_type.upper()} SELL executed @ ${BTC_PRICE:,.2f}", "sell")

            for idx in sorted(executed_indices, reverse=True):
                p['pending_orders'].pop(idx)

        leaderboard = []
        for sid, p in ACTIVE_PLAYERS.items():
            p['net_worth'] = round(p['cash'] + (p['btc'] * BTC_PRICE), 2)
            leaderboard.append({'username': p['username'], 'cash': p['cash'], 'btc': p['btc'], 'net_worth': p['net_worth']})

        leaderboard.sort(key=lambda x: x['net_worth'], reverse=True)
        socketio.emit('price_update', {'price': BTC_PRICE, 'candle': new_candle, 'leaderboard': leaderboard, 'news': active_news, 'feed': TRADE_FEED})

threading.Thread(target=btc_price_simulation, daemon=True).start()

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
    <title>BTC Rival Trader - Pro Candlesticks & Analytics</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
    <style>.pb-safe { padding-bottom: max(1.25rem, env(safe-area-inset-bottom)); } html, body { overflow-x: hidden; touch-action: manipulation; }</style>
</head>
<body class="bg-gray-950 text-gray-100 min-h-[100dvh] flex flex-col font-sans antialiased selection:bg-amber-500 selection:text-black">
    <div id="auth-modal" class="fixed inset-0 bg-black/90 backdrop-blur-lg flex items-center justify-center z-50 p-4">
        <div class="bg-gray-900 border border-gray-800 p-6 md:p-8 rounded-2xl max-w-sm w-full shadow-2xl space-y-5">
            <div class="text-center">
                <h2 class="text-2xl font-black text-amber-500 tracking-wider">⚡ BTC RIVAL PRO</h2>
                <p class="text-xs text-gray-400 mt-1">Multiplayer Candlestick Trader</p>
            </div>
            <div id="auth-error" class="hidden bg-rose-950/80 border border-rose-800 text-rose-300 text-xs p-3 rounded-xl text-center"></div>
            <div class="space-y-3">
                <div><label class="text-[10px] font-bold text-gray-400 uppercase tracking-widest">Username</label><input id="auth-user" type="text" placeholder="Enter username" class="w-full mt-1 bg-gray-950 border border-gray-800 rounded-xl px-4 py-3 text-sm text-white focus:outline-none focus:border-amber-500"></div>
                <div><label class="text-[10px] font-bold text-gray-400 uppercase tracking-widest">Password</label><input id="auth-pass" type="password" placeholder="Enter password" class="w-full mt-1 bg-gray-950 border border-gray-800 rounded-xl px-4 py-3 text-sm text-white focus:outline-none focus:border-amber-500"></div>
            </div>
            <div class="flex gap-3 pt-1">
                <button onclick="handleAuth('login')" class="flex-1 bg-amber-600 active:bg-amber-700 text-white font-bold py-3 rounded-xl text-sm transition">LOG IN</button>
                <button onclick="handleAuth('register')" class="flex-1 bg-gray-800 active:bg-gray-700 border border-gray-700 text-gray-200 font-bold py-3 rounded-xl text-sm transition">REGISTER</button>
            </div>
        </div>
    </div>
    <div id="analytics-modal" class="hidden fixed inset-0 bg-black/80 backdrop-blur-md flex items-center justify-center z-50 p-4">
        <div class="bg-gray-900 border border-gray-800 p-6 rounded-2xl max-w-xl w-full max-h-[90vh] overflow-y-auto space-y-5 shadow-2xl">
            <div class="flex justify-between items-center border-b border-gray-800 pb-3">
                <h3 class="text-lg font-bold text-amber-400">📊 Your Trading Performance Analytics</h3>
                <button onclick="toggleAnalytics(false)" class="text-gray-400 hover:text-white font-bold text-xl">✕</button>
            </div>
            <div class="grid grid-cols-2 md:grid-cols-4 gap-3 text-center">
                <div class="bg-gray-950 p-3 rounded-xl border border-gray-800"><p class="text-[10px] text-gray-400 uppercase font-bold">Total Trades</p><p id="an-total-trades" class="text-base font-black text-white mt-1">0</p></div>
                <div class="bg-gray-950 p-3 rounded-xl border border-gray-800"><p class="text-[10px] text-gray-400 uppercase font-bold">Win Rate</p><p id="an-win-rate" class="text-base font-black text-emerald-400 mt-1">0.0%</p></div>
                <div class="bg-gray-950 p-3 rounded-xl border border-gray-800"><p class="text-[10px] text-gray-400 uppercase font-bold">Total Volume</p><p id="an-total-vol" class="text-base font-black text-amber-400 mt-1">$0</p></div>
                <div class="bg-gray-950 p-3 rounded-xl border border-gray-800"><p class="text-[10px] text-gray-400 uppercase font-bold">All-Time High NW</p><p id="an-high-nw" class="text-base font-black text-white mt-1">$100,000</p></div>
            </div>
            <div class="space-y-2">
                <h4 class="text-xs font-bold text-gray-400 uppercase tracking-wider">Recent Executed Orders</h4>
                <div id="an-trades-list" class="space-y-1.5 max-h-48 overflow-y-auto text-xs font-mono"><p class="text-gray-500 italic">No trade history recorded yet.</p></div>
            </div>
        </div>
    </div>
    <header class="bg-gray-900/90 backdrop-blur border-b border-gray-800/80 sticky top-0 z-30 px-4 py-3">
        <div class="max-w-7xl mx-auto flex justify-between items-center">
            <span class="text-xl font-black text-amber-500">⚡ BTC RIVAL</span>
            <div class="flex items-center gap-3">
                <button onclick="fetchAnalytics()" class="bg-gray-800 hover:bg-gray-700 text-xs font-bold px-3 py-1.5 rounded-full border border-gray-700 text-amber-400 transition">📊 Analytics</button>
                <div id="player-tag" class="text-xs font-bold bg-gray-950 px-3 py-1.5 rounded-full border border-gray-800 text-amber-400">Not Logged In</div>
            </div>
        </div>
    </header>
    <div id="news-banner" class="hidden bg-amber-950/90 border-b border-amber-800/60 px-4 py-2 text-center text-xs font-bold text-amber-300"><span id="news-text"></span></div>
    <main class="max-w-7xl mx-auto w-full p-3 md:p-6 flex-1 pb-28 md:pb-6">
        <div class="bg-gray-900 border border-gray-800 p-4 md:p-5 rounded-2xl shadow-lg mb-4 flex justify-between items-center">
            <div>
                <span class="text-[10px] text-gray-400 font-bold tracking-wider uppercase">Live BTC/USD Pro Chart</span>
                <div id="btc-price" class="text-2xl md:text-4xl font-extrabold text-amber-400 mt-0.5">$65,000.00</div>
            </div>
            <div class="text-right"><span id="price-change" class="text-xs md:text-sm font-bold text-gray-400 block">0.00%</span><span class="text-[10px] text-gray-500">15s Candlestick</span></div>
        </div>
        <div class="grid grid-cols-1 lg:grid-cols-4 gap-6">
            <div id="tab-chart" class="mobile-tab lg:col-span-3 space-y-4 md:space-y-6">
                <div class="bg-gray-900 border border-gray-800 p-3 md:p-5 rounded-2xl shadow-lg"><div id="candlestick-chart" class="h-64 md:h-80 w-full"></div></div>
                <div class="bg-gray-900 border border-gray-800 p-4 md:p-6 rounded-2xl shadow-lg space-y-3">
                    <h2 class="text-sm md:text-lg font-bold border-b border-gray-800 pb-2 text-gray-300">Your Holdings</h2>
                    <div class="grid grid-cols-3 gap-2 text-center">
                        <div class="bg-gray-950 p-2.5 rounded-xl border border-gray-800/80"><p class="text-[10px] text-gray-400 uppercase font-bold">Cash</p><p id="usr-cash" class="text-xs md:text-base font-bold text-emerald-400 mt-1">$100,000</p></div>
                        <div class="bg-gray-950 p-2.5 rounded-xl border border-gray-800/80"><p class="text-[10px] text-gray-400 uppercase font-bold">BTC</p><p id="usr-btc" class="text-xs md:text-base font-bold text-amber-400 mt-1">0.0000</p></div>
                        <div class="bg-gray-950 p-2.5 rounded-xl border border-gray-800/80"><p class="text-[10px] text-gray-400 uppercase font-bold">Net Worth</p><p id="usr-net" class="text-xs md:text-base font-bold text-white mt-1">$100,000</p></div>
                    </div>
                </div>
            </div>
            <div id="tab-trade" class="mobile-tab hidden lg:block lg:col-span-2 space-y-4">
                <div class="bg-gray-900 border border-gray-800 p-5 md:p-6 rounded-2xl shadow-lg space-y-4">
                    <h2 class="text-sm md:text-lg font-bold border-b border-gray-800 pb-2 text-gray-300">Instant Trade Engine</h2>
                    <div class="space-y-3">
                        <div><label class="text-[10px] font-bold text-gray-400 uppercase">Trade Amount (USD)</label><input id="trade-amount" type="number" placeholder="Enter USD Amount" class="w-full mt-1 bg-gray-950 border border-gray-800 rounded-xl px-4 py-3 text-sm text-white focus:outline-none focus:border-amber-500"></div>
                        <div class="grid grid-cols-4 gap-2">
                            <button onclick="setPercent(0.25)" class="bg-gray-950 hover:bg-gray-800 border border-gray-800 py-2 rounded-lg text-xs font-bold text-gray-300">25%</button>
                            <button onclick="setPercent(0.50)" class="bg-gray-950 hover:bg-gray-800 border border-gray-800 py-2 rounded-lg text-xs font-bold text-gray-300">50%</button>
                            <button onclick="setPercent(0.75)" class="bg-gray-950 hover:bg-gray-800 border border-gray-800 py-2 rounded-lg text-xs font-bold text-gray-300">75%</button>
                            <button onclick="setPercent(1.00)" class="bg-gray-950 hover:bg-gray-800 border border-gray-800 py-2 rounded-lg text-xs font-bold text-amber-400">MAX</button>
                        </div>
                        <div class="grid grid-cols-2 gap-3 pt-2">
                            <button onclick="marketTrade('buy')" class="bg-emerald-600 active:bg-emerald-700 text-white font-bold py-3.5 rounded-xl text-sm transition">MARKET BUY</button>
                            <button onclick="marketTrade('sell')" class="bg-rose-600 active:bg-rose-700 text-white font-bold py-3.5 rounded-xl text-sm transition">SELL ALL BTC</button>
                        </div>
                    </div>
                </div>
            </div>
            <div id="tab-orders" class="mobile-tab hidden lg:block lg:col-span-1 space-y-4">
                <div class="bg-gray-900 border border-gray-800 p-5 md:p-6 rounded-2xl shadow-lg space-y-4">
                    <h2 class="text-sm md:text-lg font-bold border-b border-gray-800 pb-2 text-gray-300">Limit & Stop-Loss</h2>
                    <div class="space-y-3">
                        <div><label class="text-[10px] font-bold text-gray-400 uppercase">Target Price ($)</label><input id="order-target" type="number" placeholder="e.g. 60000" class="w-full mt-1 bg-gray-950 border border-gray-800 rounded-xl px-4 py-2.5 text-sm text-white focus:outline-none focus:border-amber-500"></div>
                        <div class="grid grid-cols-3 gap-2">
                            <button onclick="placeOrder('limit', 'buy')" class="bg-emerald-950 border border-emerald-800 text-emerald-300 font-bold py-2.5 rounded-lg text-xs">LIMIT BUY</button>
                            <button onclick="placeOrder('limit', 'sell')" class="bg-blue-950 border border-blue-800 text-blue-300 font-bold py-2.5 rounded-lg text-xs">LIMIT SELL</button>
                            <button onclick="placeOrder('stop_loss', 'sell')" class="bg-rose-950 border border-rose-800 text-rose-300 font-bold py-2.5 rounded-lg text-xs">STOP LOSS</button>
                        </div>
                        <div class="pt-2"><p class="text-[10px] font-bold text-gray-400 uppercase mb-2">Pending Orders:</p><div id="pending-orders-list" class="space-y-2 max-h-36 overflow-y-auto text-xs"><p class="text-gray-500 italic">No active orders</p></div></div>
                    </div>
                </div>
            </div>
            <div id="tab-rankings" class="mobile-tab hidden lg:block lg:col-span-4 space-y-4">
                <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div class="bg-gray-900 border border-gray-800 p-4 rounded-2xl shadow-lg"><h2 class="text-sm font-bold border-b border-gray-800 pb-2 mb-3 text-gray-300">Wall Street Rankings</h2><div id="leaderboard" class="space-y-2 max-h-56 overflow-y-auto"><p class="text-xs text-gray-500 text-center py-2">Waiting for updates...</p></div></div>
                    <div class="bg-gray-900 border border-gray-800 p-4 rounded-2xl shadow-lg"><h2 class="text-sm font-bold border-b border-gray-800 pb-2 mb-3 text-gray-300">Live Trade Feed</h2><div id="trade-feed" class="space-y-1.5 max-h-56 overflow-y-auto text-xs font-mono"><p class="text-gray-500 italic">No recent activity</p></div></div>
                    <div class="bg-gray-900 border border-gray-800 p-4 rounded-2xl shadow-lg flex flex-col h-72 md:h-auto">
                        <h2 class="text-sm font-bold border-b border-gray-800 pb-2 mb-2 text-gray-300">💬 Global Trader Chat</h2>
                        <div id="chat-messages" class="flex-1 overflow-y-auto space-y-2 text-xs p-1"><p class="text-gray-500 italic">Welcome to global chat!</p></div>
                        <div class="flex gap-2 pt-2 border-t border-gray-800 mt-2">
                            <input id="chat-input" type="text" placeholder="Type a message..." class="flex-1 bg-gray-950 border border-gray-800 rounded-xl px-3 py-1.5 text-xs text-white focus:outline-none focus:border-amber-500">
                            <button onclick="sendChatMessage()" class="bg-amber-600 hover:bg-amber-500 text-xs font-bold px-3 py-1.5 rounded-xl text-white">Send</button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </main>
    <nav class="lg:hidden fixed bottom-0 inset-x-0 bg-gray-900/95 backdrop-blur-xl border-t border-gray-800/80 z-40 pb-safe">
        <div class="grid grid-cols-4 h-14">
            <button onclick="switchTab('chart')" id="nav-chart" class="nav-btn text-amber-500 flex flex-col items-center justify-center gap-1"><svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18"></path></svg><span class="text-[10px] font-bold">Chart</span></button>
            <button onclick="switchTab('trade')" id="nav-trade" class="nav-btn text-gray-400 flex flex-col items-center justify-center gap-1"><svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg><span class="text-[10px] font-bold">Trade</span></button>
            <button onclick="switchTab('orders')" id="nav-orders" class="nav-btn text-gray-400 flex flex-col items-center justify-center gap-1"><svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"></path></svg><span class="text-[10px] font-bold">Orders</span></button>
            <button onclick="switchTab('rankings')" id="nav-rankings" class="nav-btn text-gray-400 flex flex-col items-center justify-center gap-1"><svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6"></path></svg><span class="text-[10px] font-bold">Social</span></button>
        </div>
    </nav>
    <script>
        const socket = io();
        let currentCash = 100000.0, chart, candleSeries;
        const chartElement = document.getElementById('candlestick-chart');
        chart = LightweightCharts.createChart(chartElement, {
            layout: { backgroundColor: '#111827', textColor: '#9ca3af' },
            grid: { vertLines: { color: '#1f2937' }, horzLines: { color: '#1f2937' } },
            timeScale: { timeVisible: true, secondsVisible: true }
        });
        candleSeries = chart.addCandlestickSeries({ upColor: '#10b981', downColor: '#f43f5e', borderUpColor: '#10b981', borderDownColor: '#f43f5e', wickUpColor: '#10b981', wickDownColor: '#f43f5e' });
        window.addEventListener('resize', () => { chart.applyOptions({ width: chartElement.clientWidth, height: chartElement.clientHeight }); });

        function switchTab(t) {
            if (window.innerWidth < 1024) {
                document.querySelectorAll('.mobile-tab').forEach(el => el.classList.add('hidden'));
                document.getElementById('tab-' + t).classList.remove('hidden');
                document.querySelectorAll('.nav-btn').forEach(b => { b.classList.remove('text-amber-500'); b.classList.add('text-gray-400'); });
                document.getElementById('nav-' + t).classList.remove('text-gray-400');
                document.getElementById('nav-' + t).classList.add('text-amber-500');
            }
        }
        function handleAuth(t) {
            const u = document.getElementById('auth-user').value.trim(), p = document.getElementById('auth-pass').value.trim();
            if (!u || !p) { showAuthError('Username and password required!'); return; }
            socket.emit(t === 'login' ? 'login' : 'register', { username: u, password: p });
        }
        function showAuthError(m) { const e = document.getElementById('auth-error'); e.innerText = m; e.classList.remove('hidden'); }
        socket.on('auth_success', (d) => {
            document.getElementById('auth-modal').classList.add('hidden');
            document.getElementById('player-tag').innerText = "Trader: " + d.username;
            currentCash = d.cash; updatePortfolio(d.cash, d.btc, d.cash); updatePendingOrdersUI(d.pending_orders || []);
            if (d.history_candles) candleSeries.setData(d.history_candles);
        });
        socket.on('auth_error', (d) => { showAuthError(d.message); });
        socket.on('price_update', (d) => {
            const pEl = document.getElementById('btc-price'), prev = parseFloat(pEl.innerText.replace('$', '').replace(',', '')) || d.price;
            pEl.innerText = '$' + d.price.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
            const diff = d.price - prev, cEl = document.getElementById('price-change');
            if (diff > 0) { cEl.className = "text-xs md:text-sm font-bold text-emerald-400"; cEl.innerText = "+" + ((diff/prev)*100).toFixed(2) + "%"; }
            else if (diff < 0) { cEl.className = "text-xs md:text-sm font-bold text-rose-400"; cEl.innerText = ((diff/prev)*100).toFixed(2) + "%"; }
            if (d.candle) candleSeries.update(d.candle);
            const b = document.getElementById('news-banner');
            if (d.news) { document.getElementById('news-text').innerText = d.news; b.classList.remove('hidden'); } else b.classList.add('hidden');
            document.getElementById('leaderboard').innerHTML = d.leaderboard.map((p, r) => `<div class="flex items-center justify-between p-2 rounded-xl bg-gray-950 border border-gray-800"><span class="text-xs font-bold text-gray-300">#${r + 1} ${p.username}</span><span class="text-xs font-bold text-emerald-400">$${p.net_worth.toLocaleString(undefined, {minimumFractionDigits: 2})}</span></div>`).join('');
            if (d.feed) document.getElementById('trade-feed').innerHTML = d.feed.map(i => `<div class="${i.type === 'buy' ? 'text-emerald-400' : i.type === 'sell' ? 'text-rose-400' : i.type === 'news' ? 'text-amber-400 font-bold' : 'text-gray-400'}">[${i.time}] ${i.text}</div>`).join('');
        });
        socket.on('chat_message', (d) => {
            const c = document.getElementById('chat-messages');
            c.innerHTML += `<div class="bg-gray-950 p-2 rounded-lg border border-gray-800/80"><span class="font-bold text-amber-400">${d.username}:</span> <span class="text-gray-200">${d.msg}</span></div>`;
            c.scrollTop = c.scrollHeight;
        });
        function sendChatMessage() { const i = document.getElementById('chat-input'), m = i.value.trim(); if (m) { socket.emit('send_chat', { message: m }); i.value = ''; } }
        socket.on('trade_result', (d) => { if (d.success) { currentCash = d.cash; updatePortfolio(d.cash, d.btc, d.net_worth); if (d.pending_orders !== undefined) updatePendingOrdersUI(d.pending_orders); } else alert(d.message); });
        function fetchAnalytics() { socket.emit('get_analytics'); }
        socket.on('analytics_data', (d) => {
            document.getElementById('an-total-trades').innerText = d.total_trades;
            document.getElementById('an-win-rate').innerText = d.win_rate + '%';
            document.getElementById('an-total-vol').innerText = '$' + d.total_volume.toLocaleString();
            document.getElementById('an-high-nw').innerText = '$' + d.highest_net_worth.toLocaleString();
            const l = document.getElementById('an-trades-list');
            l.innerHTML = (!d.history || d.history.length === 0) ? '<p class="text-gray-500 italic">No trade history recorded yet.</p>' : d.history.map(h => `<div class="flex justify-between bg-gray-950 p-2 rounded border border-gray-800"><span class="${h.action === 'BUY' ? 'text-emerald-400' : 'text-rose-400'} font-bold">${h.action} @ $${h.price.toLocaleString()}</span><span class="text-gray-400">$${h.usd.toLocaleString()}</span></div>`).join('');
            toggleAnalytics(true);
        });
        function toggleAnalytics(s) { const m = document.getElementById('analytics-modal'); if (s) m.classList.remove('hidden'); else m.classList.add('hidden'); }
        function setPercent(p) { document.getElementById('trade-amount').value = Math.floor(currentCash * p); }
        function updatePortfolio(c, b, n) { document.getElementById('usr-cash').innerText = '$' + c.toLocaleString(undefined, {minimumFractionDigits: 2}); document.getElementById('usr-btc').innerText = b.toFixed(4); document.getElementById('usr-net').innerText = '$' + n.toLocaleString(undefined, {minimumFractionDigits: 2}); }
        function updatePendingOrdersUI(o) { const l = document.getElementById('pending-orders-list'); if (!o || o.length === 0) { l.innerHTML = '<p class="text-gray-500 italic">No active orders</p>'; return; } l.innerHTML = o.map(i => `<div class="flex justify-between items-center bg-gray-950 p-2 rounded-lg border border-gray-800"><span class="font-bold text-gray-300">${i.type.toUpperCase()} ${i.action.toUpperCase()} @ $${i.price.toLocaleString()}</span><button onclick="cancelOrder(${i.id})" class="text-rose-500 hover:text-rose-400 font-bold px-2 py-0.5">✕</button></div>`).join(''); }
        function marketTrade(a) { socket.emit('execute_trade', { action: a, amount: parseFloat(document.getElementById('trade-amount').value) || 0 }); }
        function placeOrder(t, a) { const target = parseFloat(document.getElementById('order-target').value), val = parseFloat(document.getElementById('trade-amount').value) || currentCash; if (!target || target <= 0) { alert("Please enter a valid target price!"); return; } socket.emit('place_order', { type: t, action: a, price: target, usd_val: val }); }
        function cancelOrder(id) { socket.emit('cancel_order', { order_id: id }); }
    </script>
</body>
</html>
"""

# --- Health Ping Endpoint to Prevent Render Sleeping ---
@app.route('/health')
def health_check():
    return "OK", 200

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@socketio.on('register')
def handle_register(data):
    username, password, sid = data.get('username', '').strip(), data.get('password', '').strip(), request.sid
    if not username or not password: emit('auth_error', {'message': 'Username and password required.'}); return
    
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT id FROM users WHERE username = %s', (username,))
    if cursor.fetchone(): conn.close(); emit('auth_error', {'message': 'Username is taken!'}); return

    pass_hash = generate_password_hash(password)
    cursor.execute('INSERT INTO users (username, password_hash) VALUES (%s, %s) RETURNING id', (username, pass_hash))
    user_id = cursor.fetchone()[0]
    conn.commit()
    conn.close()

    ACTIVE_PLAYERS[sid] = {'user_id': user_id, 'username': username, 'cash': 100000.0, 'btc': 0.0, 'net_worth': 100000.0, 'pending_orders': []}
    add_feed_entry(f"🎉 New trader registered: {username}")
    emit('auth_success', {'username': username, 'cash': 100000.0, 'btc': 0.0, 'pending_orders': [], 'history_candles': CANDLE_HISTORY})

@socketio.on('login')
def handle_login(data):
    username, password, sid = data.get('username', '').strip(), data.get('password', '').strip(), request.sid
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT id, password_hash, cash, btc FROM users WHERE username = %s', (username,))
    row = cursor.fetchone()
    conn.close()

    if not row or not check_password_hash(row[1], password): emit('auth_error', {'message': 'Invalid username or password!'}); return

    user_id, _, cash, btc = row
    orders = db_get_orders(user_id)
    ACTIVE_PLAYERS[sid] = {'user_id': user_id, 'username': username, 'cash': cash, 'btc': btc, 'net_worth': round(cash + (btc * BTC_PRICE), 2), 'pending_orders': orders}
    add_feed_entry(f"🔑 Trader logged in: {username}")
    emit('auth_success', {'username': username, 'cash': cash, 'btc': btc, 'pending_orders': orders, 'history_candles': CANDLE_HISTORY})

@socketio.on('send_chat')
def handle_send_chat(data):
    sid, p, msg = request.sid, ACTIVE_PLAYERS.get(request.sid), data.get('message', '').strip()
    if p and msg: socketio.emit('chat_message', {'username': p['username'], 'msg': msg})

@socketio.on('get_analytics')
def handle_get_analytics():
    p = ACTIVE_PLAYERS.get(request.sid)
    if p: emit('analytics_data', db_get_analytics(p['user_id']))

@socketio.on('disconnect')
def handle_disconnect():
    if request.sid in ACTIVE_PLAYERS: del ACTIVE_PLAYERS[request.sid]

@socketio.on('execute_trade')
def handle_trade(data):
    sid, p = request.sid, ACTIVE_PLAYERS.get(request.sid)
    if not p: emit('trade_result', {'success': False, 'message': 'Please log in first!'}); return
    action, amount = data.get('action'), data.get('amount', 0)

    if action == 'buy':
        if amount <= 0: emit('trade_result', {'success': False, 'message': 'Enter a valid amount!'}); return
        if p['cash'] >= amount:
            btc_bought = round(amount / BTC_PRICE, 6)
            p['cash'] -= amount
            p['btc'] += btc_bought
            p['buy_avg_price'] = BTC_PRICE
            p['net_worth'] = round(p['cash'] + (p['btc'] * BTC_PRICE), 2)
            save_user_portfolio(p['user_id'], p['cash'], p['btc'], p['net_worth'])
            db_log_trade(p['user_id'], 'BUY', BTC_PRICE, amount, btc_bought)
            add_feed_entry(f"🟢 {p['username']} BOUGHT ${amount:,.0f} worth of BTC @ ${BTC_PRICE:,.2f}", "buy")
            emit('trade_result', {'success': True, 'cash': p['cash'], 'btc': p['btc'], 'net_worth': p['net_worth']})
        else: emit('trade_result', {'success': False, 'message': 'Insufficient cash!'})
    elif action == 'sell':
        if p['btc'] > 0:
            revenue = round(p['btc'] * BTC_PRICE, 2)
            is_win = BTC_PRICE > p.get('buy_avg_price', 0)
            p['cash'] += revenue
            p['btc'] = 0.0
            p['net_worth'] = round(p['cash'], 2)
            save_user_portfolio(p['user_id'], p['cash'], p['btc'], p['net_worth'])
            db_log_trade(p['user_id'], 'SELL', BTC_PRICE, revenue, p['btc'], is_win=is_win)
            add_feed_entry(f"🔴 {p['username']} SOLD ALL BTC for ${revenue:,.2f} @ ${BTC_PRICE:,.2f}", "sell")
            emit('trade_result', {'success': True, 'cash': p['cash'], 'btc': p['btc'], 'net_worth': p['net_worth']})
        else: emit('trade_result', {'success': False, 'message': 'No BTC available!'})

@socketio.on('place_order')
def handle_place_order(data):
    p = ACTIVE_PLAYERS.get(request.sid)
    if not p: return
    order_type, action, target_price = data.get('type'), data.get('action'), float(data.get('price'))
    usd_val = float(data.get('usd_val', p['cash']))

    order_id = db_add_order(p['user_id'], order_type, action, target_price, usd_val)
    p['pending_orders'].append({'id': order_id, 'type': order_type, 'action': action, 'price': target_price, 'usd_val': usd_val})
    add_feed_entry(f"⚙️ {p['username']} set {order_type.upper()} {action.upper()} @ ${target_price:,.2f}")
    emit('trade_result', {'success': True, 'cash': p['cash'], 'btc': p['btc'], 'net_worth': p['net_worth'], 'pending_orders': p['pending_orders']})

@socketio.on('cancel_order')
def handle_cancel_order(data):
    p = ACTIVE_PLAYERS.get(request.sid)
    order_id = data.get('order_id')
    if p:
        db_remove_order(order_id)
        p['pending_orders'] = [o for o in p['pending_orders'] if o['id'] != order_id]
        emit('trade_result', {'success': True, 'cash': p['cash'], 'btc': p['btc'], 'net_worth': p['net_worth'], 'pending_orders': p['pending_orders']})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host='0.0.0.0', port=port, allow_unsafe_werkzeug=True)
