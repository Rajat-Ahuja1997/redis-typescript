import * as net from 'net';
import * as fs from 'fs';
import RESPParser, { type RESPData } from './parser';
import { LOCALHOST, REDIS_PORT, type TopLevelCommand } from './types';

const parameters = Bun.argv.slice(2);
const dirIndex = parameters.indexOf('--dir');
const dbFilenameIndex = parameters.indexOf('--dbfilename');

const CONFIG = {
  dir: parameters[dirIndex + 1] ?? '',
  dbFileName: parameters[dbFilenameIndex + 1] ?? '',
};

let globalKeyValueStore: Map<string, string> = new Map();

// Load RDB file at startup
try {
  const filepath = `${CONFIG.dir}/${CONFIG.dbFileName}`;
  const content = fs.readFileSync(filepath);
  const hexContent = content.toString('hex');
  const db = hexContent.slice(hexContent.indexOf('fe'));
  globalKeyValueStore = _loadRDBFile(db, globalKeyValueStore);
} catch (e) {
  console.log('Error reading initial RDB file', e);
}

const server: net.Server = net.createServer((connection: net.Socket) => {
  connection.on('data', (data: Buffer) => {
    const parser = new RESPParser();
    const parsedInput = parser.parse(data);
    if (!parsedInput) {
      return;
    }

    const response = handleParsedInput(parsedInput, globalKeyValueStore);
    if (response) {
      connection.write(response);
    }
  });

  connection.on('close', () => {
    console.log('connection closed');
    connection.end();
  });
});

server.listen(REDIS_PORT, LOCALHOST);

function handleParsedInput(
  parsedInput: RESPData | null,
  map: Map<string, string | undefined>
): string | null {
  if (!parsedInput || !parsedInput.value) {
    return null;
  }
  const parsedValue = parsedInput.value;

  if (parsedInput.type === '*' && Array.isArray(parsedValue)) {
    const command = (parsedValue[0] ?? '')
      .toString()
      .toUpperCase() as TopLevelCommand;
    switch (command) {
      case 'PING':
        return '+PONG\r\n';
      case 'ECHO': {
        return _formatStringResponse(parsedValue[1]?.toString());
      }
      case 'SET': {
        return handleSetCommand(parsedValue, map);
      }
      case 'GET': {
        return handleGetCommand(parsedValue, map);
      }
      case 'CONFIG': {
        return handleConfigCommand(parsedValue);
      }
      case 'KEYS': {
        return handleKeysCommand(parsedValue, map);
      }

      default:
        return '-ERR unknown command\r\n';
    }
  }
  return null;
}

/**
 * Parses a serialized RDB file and populates a map with the key-value pairs
 * @param data Serialized RDB file content representing one redis DB
 * @param map Map to populate with key-value pairs
 */
function _loadRDBFile(
  data: string,
  map: Map<string, string>
): Map<string, string> {
  const buf = Buffer.from(data, 'hex');
  let cursor = 0;
  if (buf[cursor] !== 0xfe) {
    throw new Error('Invalid RDB file');
  }
  cursor++;
  const dbIndex = buf[cursor++];
  const hashTableSize = buf[cursor++];
  const expireSize = buf[cursor++];
  console.log(
    `DB Index: ${dbIndex}, Hash Table Size: ${hashTableSize}, Expire Size: ${expireSize}`
  );
  while (cursor < buf.length) {
    if (buf[cursor] === 0xff) {
      console.log('Reached end of DB');
      break;
    }

    if (buf[cursor] === 0xfc) {
      // 0xfc is a special entry type that indicates key-value pairs with expiry
    }
    cursor++; // skip entry type; 00 is string
    const keyLength = buf[cursor++];
    const key = buf.subarray(cursor, cursor + keyLength);
    cursor += keyLength;
    const valueLength = buf[cursor++];
    const value = buf.slice(cursor, cursor + valueLength);
    cursor += valueLength;

    map.set(key.toString(), value.toString());
  }
  return map;
}

function handleSetCommand(
  parsedValue: any,
  map: Map<string, string | undefined>
) {
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
      map.set(key, undefined);
    }, timeout);
  }

  return '+OK\r\n';
}

function handleGetCommand(
  parsedValue: any,
  map: Map<string, string | undefined>
) {
  const key = parsedValue[1]?.toString();

  if (!key) {
    return '-ERR invalid arguments\r\n';
  }

  // Check in-process map
  if (map.has(key) && map.get(key) !== undefined) {
    return `${_formatStringResponse(map.get(key))}`;
  }

  return '$-1\r\n';
}

function handleConfigCommand(parsedValue: any) {
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
        const dirArr = ['dir', CONFIG.dir];
        return _formatArrResponse(dirArr);
      case 'dbfilename':
        const dbArr = ['dbfilename', CONFIG.dbFileName];
        return _formatArrResponse(dbArr);
      default:
        return '-ERR unsupported parameter\r\n';
    }
  } else {
    return '-ERR unsupported nested command for CONFIG\r\n';
  }
}

function handleKeysCommand(
  parsedValue: any,
  map: Map<string, string | undefined>
) {
  const parameter = parsedValue[1]?.toString();
  if (!parameter) {
    return '-ERR invalid arguments\r\n';
  }

  if (parameter === '*') {
    const entries = map.entries();
    return _formatArrResponse(Array.from(entries).map(([key]) => key));
  }
  return '-ERR not implemented\r\n';
}

function _formatStringResponse(value: string | undefined): string {
  if (!value) {
    return '-1\r\n';
  }
  return `$${value.length}\r\n${value}\r\n`;
}

function _formatArrResponse(data: string[]): string {
  let response = `*${data.length}\r\n`;
  data.forEach((d) => {
    response += `$${d.length}\r\n${d}\r\n`;
  });
  return response;
}
