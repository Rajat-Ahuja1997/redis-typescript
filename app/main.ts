import * as net from 'net';
import RESPParser, { type RESPData } from './parser';
import type { TopLevelCommand } from './types';
import * as fs from 'fs';

const parameters = Bun.argv.slice(2);
const dirIndex = parameters.indexOf('--dir');
const dbFilenameIndex = parameters.indexOf('--dbfilename');

const CONFIG = {
  dir: parameters[dirIndex + 1] ?? '',
  dbFileName: parameters[dbFilenameIndex + 1] ?? '',
};

const server: net.Server = net.createServer((connection: net.Socket) => {
  const map = new Map<string, string>();
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

  connection.on('close', () => {
    console.log('connection closed');
    connection.end();
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
            map.set(key, 'expired');
          }, timeout);
        }

        return '+OK\r\n';
      }
      case 'GET': {
        const key = parsedValue[1]?.toString();

        if (!key) {
          return '-ERR invalid arguments\r\n';
        }

        const mapVal = map.get(key);
        if (mapVal && mapVal !== 'expired') {
          return `${_formatStringResponse(mapVal)}`;
        } else if (mapVal === 'expired') {
          return '$-1\r\n';
        }

        const filepath = `${CONFIG.dir}/${CONFIG.dbFileName}`;
        console.log('path', filepath);
        const content = fs.readFileSync(filepath);
        const data = content.toString('hex');

        const hexKey = Buffer.from(key).toString('hex');

        const startIdx = data.indexOf(hexKey) + hexKey.length;
        const endIdx = data.indexOf('ff') + 2;
        const value = Buffer.from(
          data.slice(startIdx + 2, endIdx),
          'hex'
        ).toString();
        console.log('hexKey', hexKey);
        console.log('length ', hexKey.length);
        console.log('value', value);

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
              return `*2\r\n$3\r\ndir\r\n${_formatStringResponse(CONFIG.dir)}`;
            case 'dbfilename':
              return `*2\r\n$9\r\ndbfilename\r\n${_formatStringResponse(
                CONFIG.dbFileName
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
          const filepath = `${CONFIG.dir}/${CONFIG.dbFileName}`;
          console.log('path', filepath);
          const content = fs.readFileSync(filepath);
          const data = content.toString('hex');
          const dbKeys = data.slice(data.indexOf('fe'));
          console.log('dbKeys', dbKeys);
          const keyLength = dbKeys.slice(8 + 4, 8 + 4 + 2);
          console.log('keyLength', keyLength);
          const key = dbKeys.slice(
            8 + 4 + 2,
            8 + 4 + 2 + parseInt(keyLength) * 2
          );
          console.log('key', key);

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
