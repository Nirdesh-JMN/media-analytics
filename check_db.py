import asyncpg
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

async def check():
    conn = await asyncpg.connect(DATABASE_URL, ssl="require")

    try:
        # check films
        films = await conn.fetch("SELECT id, title FROM films LIMIT 6;")
        print("\n🎬 FILMS:")
        for f in films:
            print(f["id"], f["title"])

        # count films
        count = await conn.fetchval("SELECT COUNT(*) FROM films;")
        print(f"\n✅ Total films in DB: {count}")

    finally:
        await conn.close()

asyncio.run(check())