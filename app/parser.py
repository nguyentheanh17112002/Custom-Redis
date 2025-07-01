class RESPParser:
    """
    A simple RESP parser that can parse RESP commands and return them as a list.
    """

    def __init__(self, data: bytes):
        self.data = data
        self.index = 0

    def read_line(self):
        """
        Reads a line from the RESP data.
        A line is defined as a sequence of bytes ending
        with a CRLF sequence (\r\n).
        Returns:
            bytes: The line read from the RESP data.
        Raises:
            ValueError: If the line is incomplete or if the end of the data is reached.
        """
        end = self.data.find(b"\r\n", self.index)
        if end == -1:
            raise ValueError("Incomplete line in RESP data")
        line = self.data[self.index : end]
        self.index = end + 2  # Move past the \r\n
        return line

    def parse(self):
        """
        Parse a RESP (Redis Serialization Protocol) data element.
        This method determines the type of data element to parse based on the prefix
        character and delegates to the appropriate parsing method:
        - '*': Array
        - '$': Bulk string
        - '+': Simple string
        - '-': Error
        - ':': Integer
        Returns:
            The parsed data element or None if the end of data has been reached.
        Raises:
            ValueError: If an unknown RESP prefix is encountered.
        """
        if self.index >= len(self.data):
            return None

        prefix = chr(self.data[self.index])
        self.index += 1

        if prefix == "*":
            return self.parse_array()
        elif prefix == "$":
            return self.parse_bulk_string()
        elif prefix == "+":
            return self.parse_simple_string()
        elif prefix == "-":
            return self.parse_error()
        elif prefix == ":":
            return self.parse_integer()
        else:
            raise ValueError(f"Unknown RESP prefix: {prefix}")

    def parse_array(self):
        """
        Parse a RESP array.
        An array starts with '*' followed by the number of elements.
        Returns:
            list: A list of parsed elements in the array.
        Raises:
            ValueError: If the array length is invalid or if an element cannot be parsed.
        """
        length = int(self.read_line())
        if length < 0:
            return None
        array = []
        for _ in range(length):
            element = self.parse()
            if element is None:
                raise ValueError("Incomplete array element")
            array.append(element)

        assert len(array) == length, "Parsed array length does not match expected length"
        return array

    def parse_bulk_string(self):
        """
        Parse a RESP bulk string.
        A bulk string starts with '$' followed by the length of the string,
        and then the string itself, ending with CRLF.
        Returns:
            bytes: The parsed bulk string.
        Raises:
            ValueError: If the bulk string length is invalid or if the string cannot be read.
        """
        length = int(self.read_line())
        if length < 0:
            return None
        if self.index + length + 2 > len(self.data):
            raise ValueError("Incomplete bulk string")
        bulk_string = self.data[self.index : self.index + length]
        self.index += length + 2  # Move past the bulk string and CRLF
        return bulk_string

    def parse_simple_string(self):
        """
        Parse a RESP simple string.
        A simple string starts with '+' followed by the string itself, ending with CRLF.
        Returns:
            str: The parsed simple string.
        Raises:
            ValueError: If the simple string cannot be read.
        """
        line = self.read_line()
        return line.decode("utf-8")

    def parse_error(self):
        """
        Parse a RESP error.
        An error starts with '-' followed by the error message, ending with CRLF.
        Returns:
            str: The parsed error message.
        Raises:
            ValueError: If the error cannot be read.
        """
        line = self.read_line()
        return line.decode("utf-8")

    def parse_integer(self):
        """
        Parse a RESP integer.
        An integer starts with ':' followed by the integer value, ending with CRLF.
        Returns:
            int: The parsed integer.
        Raises:
            ValueError: If the integer cannot be read.
        """
        line = self.read_line()
        return int(line)
