import pymysql
import redis
import json
import sys
import boto3

# -------- Fetch RDS credentials from Secrets Manager --------
def get_rds_credentials():
    secret_name = "rds!db-ef8fd696-d403-4483-a730-092599a959ae"         #Give your Secret_name
    region_name = "eu-north-1"                                          #Region

    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

secret = get_rds_credentials()

# RDS config (using Proxy endpoint)
RDS_HOST = "database-1-proxy.proxy-crma0qyygias.eu-north-1.rds.amazonaws.com"       #Give your RDS-Proxy endpoint
RDS_DB_NAME = "test"
RDS_USER = secret["username"]
RDS_PASSWORD = secret["password"]
TABLE_NAME = "users"


# -------- Redis config --------
redis_client = redis.Redis(
    host='database-1-cache-zfrbyv.serverless.eun1.cache.amazonaws.com',
    port=6379,
    ssl=True,
    decode_responses=True,
    socket_timeout=5
)

# -------- Fetch data from RDS through Proxy --------
def fetch_data_from_rds():
    try:
        connection = pymysql.connect(
            host=RDS_HOST,
            user=RDS_USER,
            password=RDS_PASSWORD,
            database=RDS_DB_NAME,
            ssl={"ssl": {}}   # REQUIRED for caching_sha2_password plugin
        )
        print("🔗 Connected to RDS Proxy")

        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {TABLE_NAME} LIMIT 10;")
            rows = cursor.fetchall()
            return rows

    except Exception as e:
        print("❌ RDS Error:", e)
        return None

    finally:
        if 'connection' in locals():
            connection.close()


# -------- Main Program --------
def main():
    cache_key = 'cached_table_data'
    bypass_cache = "--refresh" in sys.argv

    if not bypass_cache:
        cached_data = redis_client.get(cache_key)
        if cached_data:
            print("✅ Fetched from Redis cache:")
            print(json.loads(cached_data))
            return

    print("⚙️ No cache found or refresh requested. Fetching from RDS Proxy...")
    data = fetch_data_from_rds()

    if data:
        redis_client.set(cache_key, json.dumps(data), ex=90)
        print("📦 Cached in Redis:")
        print(data)
    else:
        print("⚠️ No data fetched from RDS.")

if __name__ == "__main__":
    main()
