from app.parser import RESPParser


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
                    response = b"+PONG\r\n"
                elif command == "ECHO":
                    if len(res) >= 2:
                        response = b"$" + str(len(res[1])).encode() + b"\r\n" + res[1] + b"\r\n"
                    else:
                        response = b"-ERR wrong number of arguments for 'echo' command\r\n"
                elif command == "QUIT":
                    response = b"+OK\r\n"
                    writer.write(response)
                    await writer.drain()
                    break
                elif command == "GET":
                    if len(res) == 2:
                        key = str(res[1].decode("utf-8"))
                        value = await redis_db.get(key)
                        if value is not None:
                            response = b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"
                        else:
                            response = b"$-1\r\n"
                    else:
                        response = b"-ERR wrong number of arguments for 'get' command\r\n"
                elif command == "SET":
                    if len(res) == 3:
                        key = str(res[1].decode("utf-8"))
                        value = res[2]
                        await redis_db.set(key, value)
                        response = b"+OK\r\n"
                    else:
                        response = b"-ERR wrong number of arguments for 'set' command\r\n"
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
