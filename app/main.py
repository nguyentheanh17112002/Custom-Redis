import asyncio
from functools import partial
from app.redis_database import RedisDatabase
from app.utils import handle_client


async def main():
    redis_db = RedisDatabase()
    server = await asyncio.start_server(partial(handle_client, redis_db = redis_db), 'localhost', 6379)
    print("Server started on localhost:6379")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
