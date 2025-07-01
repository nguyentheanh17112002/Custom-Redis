from app.parser import RESPParser


def process_ping():
    """
    Process the PING command.
    Returns:
        bytes: The response for the PING command.
    """
    return b"+PONG\r\n"


def process_echo(message: bytes):
    """
    Process the 'ECHO' Redis command.

    This function handles the Redis ECHO command, which returns the input message.

    Args:
        message (bytes): A list of bytes representing the command and its arguments.
            Expected format is [b'ECHO', b'message'].

    Returns:
        bytes: A Redis protocol formatted response:
            - If the command has the correct arguments, returns the echoed message
              in the Redis RESP (REdis Serialization Protocol) format.
            - If the command has incorrect number of arguments, returns an error message.

    Examples:
        >>> process_echo([b'ECHO', b'Hello'])
        b'$5\r\nHello\r\n'
        >>> process_echo([b'ECHO'])
        b'-ERR wrong number of arguments for \'echo\' command\r\n'
    """
    if len(message) >= 2:
        response = b"$" + str(len(message[1])).encode() + b"\r\n" + message[1] + b"\r\n"
    else:
        response = b"-ERR wrong number of arguments for 'echo' command\r\n"
    return response


async def process_quit(writer):
    """
    Process QUIT command for Redis protocol.

    This function handles the Redis QUIT command, which is used to close the connection
    to the server. It sends back an OK response before the connection is closed.

    Args:
        writer (asyncio.StreamWriter): Stream writer object for sending the response.

    Returns:
        None

    Note:
        The actual closing of the connection should be handled by the caller after
        this function returns.
    """
    response = b"+OK\r\n"
    writer.write(response)
    await writer.drain()


async def process_get(redis_db, message):
    """
    Processes a Redis GET command asynchronously.

    This function handles a Redis GET command by retrieving the value associated
    with the provided key from the Redis database.

    Args:
        redis_db: An asynchronous Redis client instance.
        message (list): A parsed Redis command where:
            - message[0] is the command name (GET)
            - message[1] is the key to retrieve

    Returns:
        bytes: A Redis protocol formatted response:
            - If key exists: "$<value_length>\r\n<value>\r\n"
            - If key doesn't exist: "$-1\r\n"
            - If wrong number of arguments: "-ERR wrong number of arguments for 'get' command\r\n"
    """
    if len(message) == 2:
        key = str(message[1].decode("utf-8"))
        value = await redis_db.get(key)
        if value is not None:
            response = b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"
        else:
            response = b"$-1\r\n"
    else:
        response = b"-ERR wrong number of arguments for 'get' command\r\n"
    return response


async def process_set(redis_db, message):
    """
    Processes a Redis SET command.

    This function handles a Redis SET command by setting the value for the provided key
    in the Redis database. It supports both simple and expiration options.

    Args:
        redis_db: An instance of the Redis database client.
        message (list): A parsed Redis command where:
            - message[0] is the command name (SET)
            - message[1] is the key to set
            - message[2] is the value to set
            - Optional: message[3] is the expiration type (PX or EX) and message[4] is the expiration time

    Returns:
        bytes: A Redis protocol formatted response:
            - "+OK\r\n" if the command was successful
            - "-ERR wrong number of arguments for 'set' command\r\n" if arguments are incorrect
    """
    if len(message) == 3:
        key = str(message[1].decode("utf-8"))
        value = message[2]
        await redis_db.set(key, value)
        response = b"+OK\r\n"
    elif len(message) == 5:
        key = str(message[1].decode("utf-8"))
        value = message[2]
        expire = float(message[4])
        if message[3].decode("utf-8").upper() == "PX":
            expire /= 1000  # Convert milliseconds to seconds
        elif message[3].decode("utf-8").upper() == "EX":
            pass
        await redis_db.set(key, value, expire=expire)
        response = b"+OK\r\n"
    else:
        response = b"-ERR wrong number of arguments for 'set' command\r\n"
    return response

async def process_del(redis_db, message):
    """
    Processes a Redis DEL command.

    This function handles a Redis DEL command by deleting the specified key from the Redis database.

    Args:
        redis_db: An instance of the Redis database client.
        message (list): A parsed Redis command where:
            - message[0] is the command name (DEL)
            - message[1:] are the keys to delete

    Returns:
        bytes: A Redis protocol formatted response:
            - ":<number_of_deleted_keys>\r\n" if the command was successful
            - "-ERR wrong number of arguments for 'del' command\r\n" if arguments are incorrect
    """
    if len(message) >= 2:
        count = 0
        for key in message[1:]:
            key_str = str(key.decode("utf-8"))
            if await redis_db.delete(key_str):
                count += 1
        response = f":{count}\r\n".encode()
    else:
        response = b"-ERR wrong number of arguments for 'del' command\r\n"
    return response


async def handle_client(reader, writer, redis_db):
    addr = writer.get_extra_info("peername")
    print(f"Connection from {addr} has been established.")
    while True:
        try:
            data = await reader.read(1024)
            if not data:
                print(f"Connection from {addr} has been closed.")
                break

            parser = RESPParser(data)
            res = parser.parse()

            if not res:
                response = b"-ERR Invalid command format\r\n"
            else:
                command = res[0].decode("utf-8").upper()
                if command == "PING":
                    response = process_ping()
                elif command == "ECHO":
                    response = process_echo(res)
                elif command == "GET":
                    response = await process_get(redis_db, res)
                elif command == "SET":
                    response = await process_set(redis_db, res)
                elif command == "DEL":
                    response = await process_del(redis_db, res)
                elif command == "QUIT":
                    await process_quit(writer)
                    break
                else:
                    response = b"-ERR Unknown command\r\n"

            writer.write(response)
            await writer.drain()
        except Exception as e:
            print(f"Error handling client: {e}")
            writer.write(f"-ERR Internal error: {str(e)}\r\n".encode())
            await writer.drain()
            break

    writer.close()
    await writer.wait_closed()


if __name__ == "__main__":
    # Example usage
    data = b"*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n:42\r\n"
    parser = RESPParser(data)
    res = parser.parse()  # Output: [b'foo', b'bar', 42]
    print(res)
    # This will parse a RESP array containing two bulk strings and one integer.
