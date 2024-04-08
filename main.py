import time
import datetime
import requests
import pandas as pd
import logging
import psycopg2
from psycopg2 import sql
from config import APPS_CONFIG, DB_CREDENTIALS, DB_EVENTS, DB_TRANSACTIONS, UPDATE_INTERVAL, TIME_DELAY, OFFSET_BT_SCRIPTS
from apscheduler.schedulers.blocking import BlockingScheduler

# FULL EVENTS
#types: [
#            SUBSCRIPTION_CHARGE_ACTIVATED, SUBSCRIPTION_CHARGE_CANCELED,
#            RELATIONSHIP_INSTALLED, RELATIONSHIP_UNINSTALLED, RELATIONSHIP_REACTIVATED, RELATIONSHIP_DEACTIVATED,
#            CREDIT_APPLIED, CREDIT_FAILED, CREDIT_PENDING, ONE_TIME_CHARGE_ACCEPTED, ONE_TIME_CHARGE_ACTIVATED,
#            ONE_TIME_CHARGE_DECLINED, ONE_TIME_CHARGE_EXPIRED, SUBSCRIPTION_CAPPED_AMOUNT_UPDATED, SUBSCRIPTION_APPROACHING_CAPPED_AMOUNT,
#            SUBSCRIPTION_CHARGE_ACCEPTED, SUBSCRIPTION_CHARGE_DECLINED, SUBSCRIPTION_CHARGE_EXPIRED, SUBSCRIPTION_CHARGE_FROZEN,
#            SUBSCRIPTION_CHARGE_UNFROZEN, USAGE_CHARGE_APPLIED
#            ],

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s', handlers=[logging.StreamHandler()])

def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_CREDENTIALS)
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to the database: {str(e)}")
        return None
def fetch_installed_events():
    data_list_installed = []

    for app_config in APPS_CONFIG:
        logging.info(f"Processing app: {app_config['app_name']}")
        HEADERS = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": app_config["api_token"],
        }

        # Calculate the date range for the previous hour
        occurred_at_max = (datetime.datetime.utcnow() - datetime.timedelta(hours=int(UPDATE_INTERVAL))).replace(minute=0, second=0, microsecond=0).isoformat() + "Z"
        occurred_at_min = (datetime.datetime.utcnow() - datetime.timedelta(hours=(int(UPDATE_INTERVAL)*2))).replace(minute=0, second=0, microsecond=0).isoformat() + "Z"
        
        has_next_page = True
        cursor = None
        
        while has_next_page:
            logging.info(f"Fetching new batch of events...{app_config['app_name']}")
            query_installed = f"""
            {{
              app(id: "gid://partners/App/{app_config['app_id']}") {{  
                events(
                  {f'after: "{cursor}",' if cursor else ''}
                  occurredAtMin: "{occurred_at_min}",  
                  occurredAtMax: "{occurred_at_max}") {{
                  edges {{ 
                    cursor
                    node {{
                      occurredAt
                      type
                      shop {{
                        myshopifyDomain
                        name
                      }}
                    }}
                  }}
                  pageInfo{{hasNextPage}}
                }}
                name 
              }}
            }}
            """
            
            response_installed = requests.post(app_config["api_url"], headers=HEADERS, json={"query": query_installed})
            time.sleep(float(TIME_DELAY))
            if response_installed.status_code != 200:
                logging.error(f"Error: {response_installed.status_code}")
                continue
                
            data_installed = response_installed.json()
            if "data" not in data_installed or data_installed["data"] is None:
                logging.error("Unexpected response format.")
                continue

            events_data_installed = data_installed.get("data", {}).get("app", {}).get("events", {}).get("edges", [])
            logging.info(f"Fetched {len(events_data_installed)} events.")
            
            for event in events_data_installed:
                
                node = event["node"]
                
                occurred_at = node["occurredAt"]
                event_type = node["type"]
                shopify_domain = node["shop"]["myshopifyDomain"]
                data_list_installed.append({
                    "app_name": app_config["app_name"],
                    "event": event_type, 
                    "occurred_at": occurred_at, 
                    "shopify_domain": shopify_domain
                })

            # Check if there's a next page
            has_next_page = data_installed.get("data", {}).get("app", {}).get("events", {}).get("pageInfo", {}).get("hasNextPage", False)
            if has_next_page:
                # Update the cursor
                cursor = data_installed["data"]["app"]["events"]["edges"][-1]["cursor"]

    # Create a pandas DataFrame for the events
    df_installed = pd.DataFrame(data_list_installed)
    df_installed = df_installed[["event", "occurred_at", "shopify_domain", "app_name"]]

    # Mock display of the dataframe as we cannot execute requests in this environment
    df_installed.head() if not df_installed.empty else "DataFrame is empty."

    # Insert fetched data into the database
    insert_events_into_db(df_installed)

def fetch_transactions():
    data_list_app_subscription = []

    for app_config in APPS_CONFIG:
        logging.info(f"Processing app: {app_config['app_name']}")
        HEADERS = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": app_config["api_token"],
        }

        occurred_at_max = (datetime.datetime.utcnow() - datetime.timedelta(hours=int(UPDATE_INTERVAL))).replace(minute=0, second=0, microsecond=0).isoformat() + "Z"
        occurred_at_min = (datetime.datetime.utcnow() - datetime.timedelta(hours=(int(UPDATE_INTERVAL)*2))).replace(minute=0, second=0, microsecond=0).isoformat() + "Z"

        has_next_page = True
        cursor = None

        while has_next_page:
            logging.info(f"Fetching new batch of transactions...{app_config['app_name']}")
            
            # Include the cursor in the query if it exists
            cursor_str = f', after: "{cursor}"' if cursor else ''
            
            query_installed = f"""
            query appSubscription {{
            app(id: "gid://partners/App/{app_config['app_id']}") {{
                id
            }}
            transactions(
                createdAtMin: "{occurred_at_min}",
                createdAtMax: "{occurred_at_max}",
                types: [APP_SUBSCRIPTION_SALE, APP_SALE_CREDIT,APP_SALE_ADJUSTMENT,APP_USAGE_SALE],
                appId:"gid://partners/App/{app_config['app_id']}"
                {cursor_str}
            ) {{
                edges {{
                cursor
                node {{
                    id
                    createdAt
                    ... on AppSubscriptionSale {{
                    __typename
                    shop {{
                        name
                        myshopifyDomain
                    }}
                    grossAmount {{
                        amount
                    }}
                    netAmount {{
                        amount
                    }}
                    shopifyFee {{
                        amount
                    }}
                    }}
                    ... on AppSaleCredit {{
                    __typename
                    shop {{
                        name
                        myshopifyDomain
                    }}
                    grossAmount {{
                        amount
                    }}
                    netAmount {{
                        amount
                    }}
                    shopifyFee {{
                        amount
                    }}
                    }}
                    ... on AppSaleAdjustment {{
                    __typename
                    shop {{
                    name
                    myshopifyDomain
                    }}
                    grossAmount {{
                    amount
                    }}
                    netAmount {{
                    amount
                    }}
                    shopifyFee {{
                    amount
                    }}
                }}
                ... on AppUsageSale {{
                    __typename
                    shop {{
                    name
                    myshopifyDomain
                    }}
                    grossAmount {{
                    amount
                    }}
                    netAmount {{
                    amount
                    }}
                    shopifyFee {{
                    amount
                    }}
                }}
                }}
                }}
                pageInfo {{
                hasNextPage
                }}
            }}
            }}
            """
            
            response_app_subscription = requests.post(app_config["api_url"], headers=HEADERS, json={"query": query_installed})
            time.sleep(float(TIME_DELAY))
            if response_app_subscription.status_code != 200:
                logging.error(f"Error: {response_app_subscription.status_code}")
                continue

            data_app_subscription = response_app_subscription.json()
            
            if "data" not in data_app_subscription or data_app_subscription["data"] is None:
                logging.error("Unexpected response format.")
                continue

            transactions_data = data_app_subscription.get("data", {}).get("transactions", {}).get("edges", [])
            logging.info(f"Fetched {len(transactions_data)} transactions.")
            for transaction in transactions_data:
                node = transaction["node"]
                created_at = node["createdAt"]
                txn_type = node["__typename"]
                shop_name = node.get("shop", {}).get("name", None)
                shop_domain = node.get("shop", {}).get("myshopifyDomain", None)
                net_amount = node.get("netAmount", {}).get("amount", None)
                gross_amount = net_amount if node.get("grossAmount", {}) is None else  node.get("grossAmount", {}).get("amount", None)# Gross and net are the same for trans. before Jun-2020
                shopify_fee = 0 if node.get("shopifyFee", {}) is None else node.get("shopifyFee", {}).get("amount", None) # This fee is 0 for trans. before Jun-2020


                data_list_app_subscription.append({
                    "app_name": app_config["app_name"],
                    "created_at": created_at,
                    "shop_name": shop_name,
                    "shop_domain": shop_domain,
                    "gross_amount": gross_amount,
                    "net_amount": net_amount,
                    "shopify_fee": shopify_fee,
                    "txn_type": txn_type
                })

            has_next_page = data_app_subscription.get("data", {}).get("transactions", {}).get("pageInfo", {}).get("hasNextPage", False)
            
            if has_next_page:
                cursor = data_app_subscription["data"]["transactions"]["edges"][-1]["cursor"]
                logging.info(f"Has next page: {has_next_page}")
    
    df_app_subscription = pd.DataFrame(data_list_app_subscription)
    df_app_subscription.head() if not df_app_subscription.empty else "DataFrame is empty."
    insert_transactions_into_db(df_app_subscription)

def insert_events_into_db(df):
    i=1
    conn = connect_to_db()
    if conn is not None:
        cursor = conn.cursor()
        insert_query = sql.SQL(f"INSERT INTO {DB_EVENTS} (event, created_at, shopify_domain, app) VALUES (%s, %s, %s, %s)")
        for index, row in df.iterrows():
            cursor.execute(insert_query, (row['event'], row['occurred_at'], row['shopify_domain'], row['app_name']))
            logging.info(i)
            i+=1
        conn.commit()
        cursor.close()
        conn.close()
        print("Done!")
    else:
        logging.error("Failed to insert data into the database.")

def insert_transactions_into_db(df):
    i = 1
    conn = connect_to_db()
    if conn is not None:
        cursor = conn.cursor()
        insert_query = sql.SQL(f"INSERT INTO {DB_TRANSACTIONS} (app_name,created_at,shop_name,shop_domain,gross_amount,net_amount,shopify_fee,txn_type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
        for index, row in df.iterrows():
            cursor.execute(insert_query, (row['app_name'],row['created_at'],row['shop_name'],row['shop_domain'],row['gross_amount'],row['net_amount'],row['shopify_fee'],row['txn_type']))           
            logging.info(i)
            i += 1
        conn.commit()
        cursor.close()
        conn.close()
        print("Done!")
    else:
        print("Failed to insert data into the database.")

if __name__ == "__main__":
    logging.info("Starting main execution.")
    scheduler = BlockingScheduler()
    scheduler.add_job(fetch_installed_events, 'interval', hours= int(UPDATE_INTERVAL))
    time.sleep(int(OFFSET_BT_SCRIPTS)) # Wait for 5 minutes (Env var in seconds)
    scheduler.add_job(fetch_transactions, 'interval', hours= int(UPDATE_INTERVAL))
    scheduler.start()
