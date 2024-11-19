import * as net from 'net';
import RESPParser, { type RESPData } from './parser';

const server: net.Server = net.createServer((connection: net.Socket) => {
  const map = new Map<string, string>();
  const args = process.argv;
  const parameters = args.slice(2);
  const dir = parameters[parameters.indexOf('--dir') + 1];
  const dbfilename = parameters[parameters.indexOf('--dbfilename') + 1];
  map.set('dir', dir);
  map.set('dbfilename', dbfilename);
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
  parsedInput: RESPData | null,
  map: Map<string, string>
): string | null {
  if (!parsedInput) {
    return null;
  }
  const parsedValue = parsedInput.value;
  if (parsedValue === null) {
    return null;
  }

  if (parsedInput.type === '*' && Array.isArray(parsedValue)) {
    const command = (parsedValue[0] ?? '').toString().toUpperCase();
    console.log('command', command);

    switch (command) {
      case 'PING':
        return '+PONG\r\n';
      case 'ECHO': {
        const echoValue = parsedValue[1]?.toString() ?? '';
        return `$${echoValue.length}\r\n${echoValue}\r\n`;
      }
      case 'SET': {
        const key = parsedValue[1]?.toString();
        const value = parsedValue[2]?.toString();
        if (!key || !value) {
          return '-ERR invalid arguments\r\n';
        }

        map.set(key, value);
        const timeoutArg = parsedValue[3]?.toString()?.toUpperCase();
        if (timeoutArg === 'PX') {
          const timeout = parseInt(parsedValue[4]?.toString() ?? '');
          if (isNaN(timeout)) {
            return '-ERR invalid timeout\r\n';
          }
          setTimeout(() => {
            map.delete(key);
          }, timeout);
        }

        return '+OK\r\n';
      }
      case 'GET': {
        const key = parsedValue[1]?.toString();

        if (!key) {
          return '-ERR invalid arguments\r\n';
        }

        const value = map.get(key);
        return value ? `$${value.length}\r\n${value}\r\n` : '$-1\r\n';
      }

      case 'CONFIG': {
        const nestedCommand = parsedValue[1]?.toString();
        if (!nestedCommand) {
          return '-ERR missing second argument for CONFIG\r\n';
        }
        if (nestedCommand === 'GET') {
          const parameter = parsedValue[2]?.toString();
          if (!parameter) {
            return '-ERR invalid arguments\r\n';
          }

          switch (parameter) {
            case 'dir':
              //*2\r\n$3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n
              return `*2\r\n$3\r\ndir\r\n$${
                map.get('dir')?.length
              }\r\n${map.get('dir')}\r\n`;
            case 'dbfilename':
              return `*2\r\n$9\r\ndbfilename\r\n$${
                map.get('dbfilename')?.length
              }\r\n${map.get('dbfilename')}\r\n`;
            default:
              console.log('unsupported parameter', parameter);
              return '-ERR unsupported parameter\r\n';
          }
        } else {
          console.log('unsupported nested command', nestedCommand);
          return '-ERR unsupported nested command for CONFIG\r\n';
        }
      }

      default:
        return '-ERR unknown command\r\n';
    }
  }
  return null;
}
