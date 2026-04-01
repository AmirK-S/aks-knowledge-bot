import os

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
GROQ_API_KEY = os.environ["GROQ_API_KEY"]
OPENROUTER_API_KEY = os.environ["OPENROUTER_API_KEY"]
APIFY_API_KEY = os.environ["APIFY_API_KEY"]
OWNER_CHAT_ID = int(os.environ.get("OWNER_CHAT_ID", "5275505039"))
DB_PATH = os.environ.get("DB_PATH", "/data/knowledge.db")
