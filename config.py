import os
from dotenv import load_dotenv

# Load environment variables from .env file
#load_dotenv("config.env")
load_dotenv()

DB_EVENTS = os.getenv("DB_EVENTS")
DB_TRANSACTIONS = os.getenv("DB_TRANSACTIONS")
UPDATE_INTERVAL = os.getenv("UPDATE_INTERVAL")
TIME_DELAY = os.getenv("TIME_DELAY")
OFFSET_BT_SCRIPTS = os.getenv("OFFSET_BT_SCRIPTS")

# Configurations for multiple apps
APPS_CONFIG = [
    {
        "app_name": os.getenv("APP1_Name"),
        "app_id": os.getenv("APP1_ID"),
        "api_url": os.getenv("APP1_API_URL"),
        "api_token": os.getenv("APP1_API_TOKEN")
    }
    ,
    {
       "app_name": os.getenv("APP2_Name"),
       "app_id": os.getenv("APP2_ID"),
       "api_url": os.getenv("APP2_API_URL"),
       "api_token": os.getenv("APP2_API_TOKEN")
    },
    {
       "app_name": os.getenv("APP3_Name"),
       "app_id": os.getenv("APP3_ID"),
       "api_url": os.getenv("APP3_API_URL"),
       "api_token": os.getenv("APP3_API_TOKEN")
    },
    {
       "app_name": os.getenv("APP4_Name"),
       "app_id": os.getenv("APP4_ID"),
       "api_url": os.getenv("APP4_API_URL"),
       "api_token": os.getenv("APP4_API_TOKEN")
    },
    {
       "app_name": os.getenv("APP5_Name"),
       "app_id": os.getenv("APP5_ID"),
       "api_url": os.getenv("APP5_API_URL"),
       "api_token": os.getenv("APP5_API_TOKEN")
    },
    {
       "app_name": os.getenv("APP6_Name"),
       "app_id": os.getenv("APP6_ID"),
       "api_url": os.getenv("APP6_API_URL"),
       "api_token": os.getenv("APP6_API_TOKEN")
    },
    {
       "app_name": os.getenv("APP7_Name"),
       "app_id": os.getenv("APP7_ID"),
       "api_url": os.getenv("APP7_API_URL"),
       "api_token": os.getenv("APP7_API_TOKEN")
    },
    {
       "app_name": os.getenv("APP8_Name"),
       "app_id": os.getenv("APP8_ID"),
       "api_url": os.getenv("APP8_API_URL"),
       "api_token": os.getenv("APP8_API_TOKEN")
    },
    
]

# Database configurations remain the same
DB_CREDENTIALS = {
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_DATABASE"),
    "sslmode": os.getenv("DB_SSLMODE")
}
