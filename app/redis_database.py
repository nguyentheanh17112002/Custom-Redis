import asyncio


class RedisDatabase:
    """
    A simple in-memory Redis-like database that supports basic commands.
    """

    def __init__(self):
        self.data = dict()
        self.lock = asyncio.Lock()

    async def set(self, key: str, value: bytes):
        """
        Asynchronously set a key-value pair in the database.
        """
        async with self.lock:
            self.data[key] = value

    async def get(self, key: str) -> bytes:
        """
        Asynchronously get the value for a given key from the database.
        Returns None if the key does not exist.
        """
        async with self.lock:
            return self.data.get(key, None)

    async def delete(self, key: str):
        """
        Asynchronously delete a key from the database.
        """
        async with self.lock:
            if key in self.data:
                del self.data[key]

    async def exists(self, key: str) -> bool:
        """
        Asynchronously check if a key exists in the database.
        Returns True if the key exists, False otherwise.
        """
        async with self.lock:
            return key in self.data
