import * as net from 'net';
import RESPParser, { type RESPData } from './parser';
import type { TopLevelCommand } from './types';
import * as fs from 'fs';

const server: net.Server = net.createServer((connection: net.Socket) => {
  const map = new Map<string, string>();
  const parameters = process.argv.slice(2);
  const dirIndex = parameters.indexOf('--dir');
  const dbFilenameIndex = parameters.indexOf('--dbfilename');

  if (dirIndex !== -1) {
    map.set('dir', parameters[dirIndex + 1]);
  }

  if (dbFilenameIndex !== -1) {
    map.set('dbfilename', parameters[dbFilenameIndex + 1]);
  }

  connection.on('data', (data: Buffer) => {
    const parser = new RESPParser();
    const parsedInput = parser.parse(data);
    if (!parsedInput) {
      return;
    }

    const response = handleParsedInput(parsedInput, map);
    if (response) {
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
    const command = (parsedValue[0] ?? '')
      .toString()
      .toUpperCase() as TopLevelCommand;
    switch (command) {
      case 'PING':
        return '+PONG\r\n';
      case 'ECHO': {
        const echoValue = parsedValue[1]?.toString() ?? '';
        return `${_formatStringResponse(echoValue)}`;
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
        return value ? `${_formatStringResponse(value)}` : '$-1\r\n';
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
              return `*2\r\n$3\r\ndir\r\n${_formatStringResponse(
                map.get('dir')
              )}`;
            case 'dbfilename':
              return `*2\r\n$9\r\ndbfilename\r\n${_formatStringResponse(
                map.get('dbFileName')
              )}`;
            default:
              return '-ERR unsupported parameter\r\n';
          }
        } else {
          return '-ERR unsupported nested command for CONFIG\r\n';
        }
      }
      case 'KEYS': {
        const parameter = parsedValue[1]?.toString();
        if (!parameter) {
          return '-ERR invalid arguments\r\n';
        }

        if (parameter === '*') {
          const filepath = `${map.get('dir')}/${map.get('dbfilename')}`;
          const content = fs.readFileSync(filepath);
          const data = content.toString('hex');
          const dbKeys = data.slice(data.indexOf('fe'));
          console.log(`dbKeys: ${dbKeys}`);
          const numKeys = dbKeys.slice(6, 8);
          console.log(`numKeys: ${numKeys}`);
          const keyLength = dbKeys.slice(8 + 4, 8 + 4 + 2);
          const key = dbKeys.slice(
            8 + 4 + 2,
            8 + 4 + 2 + parseInt(keyLength) * 2
          );

          const formattedKey = Buffer.from(key, 'hex').toString();
          return `*1\r\n${_formatStringResponse(formattedKey)}`;
        }
        break;
      }

      default:
        return '-ERR unknown command\r\n';
    }
  }
  return null;
}

function _formatStringResponse(value: string | undefined): string {
  if (!value) {
    return '-1\r\n';
  }
  return `$${value.length}\r\n${value}\r\n`;
}
