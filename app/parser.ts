enum RESPType {
  String = '+',
  Integer = ':',
  Bulk = '$',
  Array = '*',
  Error = '-',
}

export interface RESPData {
  type: RESPType;
  value: string | number | null | (string | number | null)[];
}

export default class RESPParser {
  private buffer: string = '';

  parse(data: Buffer | string): RESPData | null {
    this.buffer += data.toString();

    if (!this.buffer.includes('\r\n')) {
      return null; // Not enough data
    }

    const firstChar = this.buffer[0];
    let result: RESPData | null = null;

    switch (firstChar) {
      case RESPType.String:
        result = this.parseSimpleString();
        break;
      case RESPType.Error:
        result = this.parseError();
        break;
      case RESPType.Integer:
        result = this.parseInteger();
        break;
      case RESPType.Bulk:
        result = this.parseBulkString();
        break;
      case RESPType.Array:
        result = this.parseArray();
        break;
    }

    return result;
  }
  private parseSimpleString(): RESPData {
    const endIndex = this.buffer.indexOf('\r\n');
    const value = this.buffer.slice(1, endIndex);
    this.buffer = this.buffer.slice(endIndex + 2);
    return { type: RESPType.String, value };
  }

  private parseError(): RESPData {
    const endIndex = this.buffer.indexOf('\r\n');
    const value = this.buffer.slice(1, endIndex);
    this.buffer = this.buffer.slice(endIndex + 2);
    return { type: RESPType.Error, value };
  }

  private parseInteger(): RESPData {
    return {
      type: RESPType.Integer,
      value: 0, // Stub implementation
    };
  }

  private parseBulkString(): RESPData {
    const lengthEnd = this.buffer.indexOf('\r\n');
    const numBytes = parseInt(this.buffer.substring(1, lengthEnd));

    if (numBytes === -1) {
      this.buffer = this.buffer.slice(lengthEnd + 2);
      return { type: RESPType.Bulk, value: null };
    }

    const valueStart = lengthEnd + 2; // advance past the \r\n
    const valueEnd = valueStart + numBytes;
    const value = this.buffer.slice(valueStart, valueEnd);

    // slice from the end of the value + past the \r\n to the end of the buffer
    this.buffer = this.buffer.slice(valueEnd + 2); // advance past the value and the \r\n

    return {
      type: RESPType.Bulk,
      value,
    };
  }

  private parseArray(): RESPData {
    const lengthEnd = this.buffer.indexOf('\r\n');
    const numberOfElements = parseInt(this.buffer.substring(1, lengthEnd));

    if (numberOfElements === -1 || numberOfElements === 0) {
      this.buffer = this.buffer.slice(lengthEnd + 2);
      return { type: RESPType.Array, value: null };
    }

    this.buffer = this.buffer.slice(lengthEnd + 2);

    const elements: (string | number | null)[] = [];

    for (let i = 0; i < numberOfElements; i++) {
      const elementType = this.buffer[0];
      let result: RESPData | null = null;

      switch (elementType) {
        case RESPType.String:
          result = this.parseSimpleString();
          break;
        case RESPType.Error:
          result = this.parseError();
          break;
        case RESPType.Integer:
          result = this.parseInteger();
          break;
        case RESPType.Bulk:
          result = this.parseBulkString();
          break;
        case RESPType.Array:
          result = this.parseArray();
          break;
      }

      if (!result) {
        return { type: RESPType.Array, value: null };
      }

      if (Array.isArray(result.value)) {
        elements.push(...result.value);
      } else {
        elements.push(result.value);
      }
    }

    return {
      type: RESPType.Array,
      value: elements,
    };
  }
}
