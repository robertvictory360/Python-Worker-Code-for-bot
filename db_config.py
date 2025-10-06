# db_config.py (CORRECTED)
import os
import pymysql.cursors

# 🛑 NOTE: NO 'from db_config import...' line here. 

def get_db_connection():
    """Establishes a synchronous connection to the remote MySQL database."""
    try:
        conn = pymysql.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            database=os.getenv('DB_NAME'),
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )
        return conn
    except Exception as e:
        print(f"FATAL ERROR: Could not connect to MySQL database. Check ENV vars. Error: {e}")
        return None

def get_config():
    """Fetches and formats bot settings from the database."""
    conn = get_db_connection()
    if not conn: return None
        
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM bot_settings WHERE id = 1")
            config = cursor.fetchone()
            
            if config:
                # Processing logic (targets, templates)
                config['targets'] = [
                    uid.strip() for uid in config['target_user_ids'].split('\n') 
                    if uid.strip().isdigit()
                ]
                config['dm_templates'] = config['messages_to_send'].split('||')
            
            return config
            
    except Exception as e:
        print(f"Error fetching config from DB: {e}")
        return None
    finally:
        conn.close()