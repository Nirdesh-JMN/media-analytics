import asyncpg
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

async def run_schema():
    conn = await asyncpg.connect(DATABASE_URL, ssl="require")

    try:
        with open("schema.sql", "r") as f:
            sql = f.read()

        await conn.execute(sql)
        print("✅ Schema applied successfully!")

    finally:
        await conn.close()

asyncio.run(run_schema())