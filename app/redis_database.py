import asyncio


class RedisDatabase:
    """
    A simple in-memory Redis-like database that supports basic commands.
    """

    def __init__(self):
        self.data = dict()
        self.expires = dict()
        self.lock = asyncio.Lock()

    async def set(self, key: str, value: bytes, expire: float = None):
        """
        Asynchronously set a key-value pair in the database.
        """
        async with self.lock:
            self.data[key] = value
            if expire is not None:
                self.expires[key] = asyncio.get_event_loop().time() + expire
            else:
                if key in self.expires:
                    del self.expires[key]

    async def get(self, key: str) -> bytes:
        """
        Asynchronously get the value for a given key from the database.
        Returns None if the key does not exist.
        """
        async with self.lock:
            if key in self.expires:
                if asyncio.get_event_loop().time() > self.expires[key]:
                    del self.data[key]
                    del self.expires[key]
                    return None
            return self.data.get(key, None)

    async def delete(self, key: str):
        """
        Asynchronously delete a key from the database.
        """
        async with self.lock:
            if key in self.data:
                del self.data[key]
                if key in self.expires:
                    del self.expires[key]

    async def exists(self, key: str) -> bool:
        """
        Asynchronously check if a key exists in the database.
        Returns True if the key exists, False otherwise.
        """
        async with self.lock:
            return key in self.data and (key not in self.expires or asyncio.get_event_loop().time() <= self.expires[key])   
        
    
    def _is_expired(self, key: str) -> bool:
        """
        Check if a key is expired.
        This is a synchronous method since it does not require locking.
        """
        if key in self.expires:
            return asyncio.get_event_loop().time() > self.expires[key]
        return False
    

    async def remove_expired_keys(self, interval: float = 60.0):
        """
        Asynchronously remove expired keys from the database.
        This method should be called periodically to clean up expired keys.
        """
        while True:
            await asyncio.sleep(interval)
            async with self.lock:
                current_time = asyncio.get_event_loop().time()
                expired_keys = [key for key, expire_time in self.expires.items() if expire_time <= current_time]
                for key in expired_keys:
                    del self.data[key]
                    del self.expires[key]