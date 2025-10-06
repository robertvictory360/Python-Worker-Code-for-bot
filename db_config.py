# discord_unified_dm_bot.py
import os
import asyncio
import random
import time
from datetime import datetime, timedelta
import pymysql.cursors
import discord
from discord.ext import tasks, commands
from dotenv import load_dotenv
from db_config import get_db_connection, get_config 

# Load .env variables (These will be set as ENVIRONMENT VARIABLES in Render)
load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

# Bot intents 
intents = discord.Intents.default()
intents.guilds = True
intents.members = True 
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- MySQL Database Helpers (Synchronous) ----------

def execute_db_query(sql, params=None):
    """Utility to run synchronous DB queries."""
    conn = get_db_connection()
    if not conn: return None
        
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall() if sql.strip().upper().startswith("SELECT") else True
    except Exception as e:
        print(f"DB Error during execution: {e} | SQL: {sql}")
        return False
    finally:
        conn.close()

def db_get_next_pending_target():
    """Selects the oldest PENDING user from the queue."""
    sql = "SELECT discord_id, user_name FROM target_queue WHERE status = 'PENDING' ORDER BY id ASC LIMIT 1"
    result = execute_db_query(sql)
    return result[0] if result else None

def db_mark_sent_or_failed(user_id: str, status: str, message_text: str):
    """Updates the queue status and inserts into the log."""
    
    # 1. Update status in the target_queue
    execute_db_query(
        "UPDATE target_queue SET status = %s WHERE discord_id = %s", 
        (status, user_id)
    )
    
    # 2. Insert into message_log
    target_info = execute_db_query(
        "SELECT user_name FROM target_queue WHERE discord_id = %s", 
        (user_id,)
    )
    user_name = target_info[0]['user_name'] if target_info else 'Unknown'

    execute_db_query(
        "INSERT INTO message_log(user_id, user_name, message_sent, status) VALUES(%s, %s, %s, %s)",
        (user_id, user_name, message_text[:500], status)
    )
    
def db_get_sent_count_last_hour():
    """Counts DMs sent successfully in the last hour."""
    cutoff = datetime.utcnow() - timedelta(hours=1)
    sql = "SELECT COUNT(*) AS count FROM message_log WHERE status = 'SENT' AND timestamp >= %s"
    result = execute_db_query(sql, (cutoff.isoformat(),))
    return result[0]['count'] if result else 0

def db_sync_targets(target_ids: list):
    """Inserts/updates all targets from the config into the target_queue."""
    conn = get_db_connection()
    if not conn: return
    
    added_count = 0
    with conn.cursor() as cursor:
        for user_id in target_ids:
            try:
                # INSERT IGNORE ensures we skip users already present (SENT, FAILED, or PENDING)
                sql = "INSERT IGNORE INTO target_queue (discord_id, user_name, status) VALUES (%s, %s, %s)"
                cursor.execute(sql, (user_id, "Fetching...", "PENDING"))
                added_count += cursor.rowcount 
            except Exception:
                pass
        conn.commit()
    conn.close()
    return added_count


# ---------- Bot Startup and Initialization ----------

@bot.event
async def on_ready():
    print(f"Bot ready: {bot.user} (id: {bot.user.id}).")
    
    # Load config and sync targets on startup
    config = get_config()
    if config and config.get('targets'):
        # Run sync function in a separate thread because it's synchronous
        added = await asyncio.to_thread(db_sync_targets, config['targets'])
        print(f"[LOAD] Synced {len(config['targets'])} targets from dashboard. Added {added} new users to queue.")

    sender_loop.start()

# ---------- Sender loop (Sends DMs) ----------
@tasks.loop(seconds=30)
async def sender_loop():
    try:
        # Load config dynamically in the loop (picks up dashboard changes)
        config = get_config()
        if not config: return

        max_dms_per_hour = config.get('rate_limit_per_hr', 2)
        dm_templates = config.get('dm_templates', [])
        invite_link = config.get('channel_id', "") # Stores the invite link

        # 1. Check rate limit
        sent_count = await asyncio.to_thread(db_get_sent_count_last_hour)
        if sent_count >= max_dms_per_hour: return

        # 2. Get next target
        row = await asyncio.to_thread(db_get_next_pending_target)
        if not row: return

        user_id = row['discord_id']
        
        user = None
        try:
            user = await bot.fetch_user(int(user_id))
        except Exception:
            await asyncio.to_thread(db_mark_sent_or_failed, user_id, "INVALID_ID", "User fetch failed.")
            return

        # 3. Apply delay
        delay = random.randint(10, 90)
        await asyncio.sleep(delay)

        # Re-check rate limit after sleep
        sent_count_after_delay = await asyncio.to_thread(db_get_sent_count_last_hour)
        if sent_count_after_delay >= max_dms_per_hour: return

        # 4. Prepare and send message
        if not dm_templates: return
        template = random.choice(dm_templates)
        
        message_text = template.format_map(
            {
                'name': user.display_name, 
                'invite': invite_link,
                'default': ''
            }
        )
        
        try:
            await user.send(message_text)
            
            await asyncio.to_thread(db_mark_sent_or_failed, user_id, "SENT", message_text)
            print(f"Sent DM starter to {user} ({user.id})")
            
        except discord.Forbidden:
            await asyncio.to_thread(db_mark_sent_or_failed, user_id, "FORBIDDEN", "Forbidden: DMs closed.")
        except Exception as e:
            await asyncio.to_thread(db_mark_sent_or_failed, user_id, "ERROR", str(e))
            
    except Exception as e:
        print("Error in sender_loop:", e)

# ---------- Run bot ----------
if __name__ == "__main__":
    if not DISCORD_TOKEN:
        print("FATAL: Missing DISCORD_TOKEN. Check Render ENV vars.")
    else:
        bot.run(DISCORD_TOKEN)