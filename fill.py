import asyncpg,asyncio
import ujson, uuid
from dateparser import parse as dateparse

async def main():
    conn = await asyncpg.connect("postgresql://allie@localhost/eventually-dev")

    async with conn.transaction():
        with open("feed.json") as f:
            c = 1
            for line in f:
                print(c, "done")
                obj = ujson.loads(line)
                i = uuid.UUID(obj["id"])
                obj["created"]= int(dateparse(obj['created']).timestamp())
                await conn.execute("INSERT INTO documents (doc_id, object) VALUES ($1,$2)",i,ujson.dumps(obj))
                c += 1

    await conn.close()

asyncio.get_event_loop().run_until_complete(main())
