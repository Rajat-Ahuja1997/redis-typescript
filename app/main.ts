import * as net from 'net';
import RESPParser from './parser';

const server: net.Server = net.createServer((connection: net.Socket) => {
  const map = new Map<string, string>();
  const parser = new RESPParser();
  connection.on('data', (data: Buffer) => {
    const parsedInput = parser.parse(data);
    if (!parsedInput) {
      return;
    }

    const response = handleParsedInput(parsedInput, map);
    if (response) {
      // Send response in RESP format
      console.log(response);
      connection.write(response);
    }
  });
});

server.listen(6379, '127.0.0.1');

function handleParsedInput(
  parsedInput: any,
  map: Map<string, string>
): string | null {
  console.log('parsedInput', parsedInput);
  const parsedValue = parsedInput.value;
  if (parsedInput.type === '*' && Array.isArray(parsedValue)) {
    const command = parsedInput.value[0]?.toString().toUpperCase();

    switch (command) {
      case 'PING':
        return '+PONG\r\n';
      case 'ECHO':
        // return $3\r\nhey\r\n
        return `$${parsedValue[1]?.length}\r\n${parsedValue[1]}\r\n`;
      case 'SET': {
        const key = parsedValue[1]?.toString();
        const value = parsedValue[2]?.toString();
        map.set(key, value);
        return '+OK\r\n';
      }
      case 'GET': {
        const key = parsedValue[1]?.toString();
        return map.get(key)
          ? `$${map.get(key)?.length}\r\n${map.get(key)}\r\n`
          : '-1';
      }
      default:
    }
  }
  return null;
}
