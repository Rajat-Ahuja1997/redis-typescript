import * as net from 'net';
import * as fs from 'fs';
import RESPParser, { type RESPData } from './parser';
import { LOCALHOST, REDIS_PORT, RedisCommand } from './types';
import {
  handlePing,
  handleEchoCommand,
  handleKeysCommand,
  handleSetCommand,
  handleInfoCommand,
  handleConfigCommand,
  handleGetCommand,
  handleReplicaConnection,
  _formatArrResponse,
  handleReplConfCommand,
  _generateRandomString,
  handlePsyncCommand,
  _formatStringResponse,
} from './handlers';
import { EMPTY_RDB_HEX, REPL_ID, REPL_OFFSET } from './constants';

const parameters = Bun.argv.slice(2);
const dirIndex = parameters.indexOf('--dir');
const dbFilenameIndex = parameters.indexOf('--dbfilename');
const portIndex = parameters.indexOf('--port');
const replicationIndex = parameters.indexOf('--replicaof');

export const CONFIG = {
  dir: parameters[dirIndex + 1] ?? '',
  dbFileName: parameters[dbFilenameIndex + 1] ?? '',
  port: portIndex !== -1 ? parseInt(parameters[portIndex + 1]) : REDIS_PORT,
  replicaOf: replicationIndex !== -1 ? parameters[replicationIndex + 1] : null,
};

const connectedReplicas: Set<net.Socket> = new Set();

let globalKeyValueStore: Map<string, string | undefined> = new Map();
globalKeyValueStore.set(REPL_ID, _generateRandomString(40));
globalKeyValueStore.set(REPL_OFFSET, '0');

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
  connection.on('data', async (data: Buffer) => {
    const parser = new RESPParser();
    const parsedInput = parser.parse(data);
    if (!parsedInput) {
      return;
    }

    const response = await handleParsedInput(parsedInput, globalKeyValueStore);
    if (response) {
      for (const msg of response) {
        if (typeof msg === 'string' && msg === 'REPLICA') {
          connectedReplicas.add(connection);
        } else {
          connection.write(msg);
        }
      }
    }
  });

  connection.on('close', () => {
    console.log('connection closed');
    connectedReplicas.delete(connection);
    connection.end();
  });
});

server.listen(CONFIG.port, LOCALHOST); // start server

if (CONFIG.replicaOf) {
  const [host, port] = CONFIG.replicaOf.split(' ');
  const client = new net.Socket();
  const pingCommand = handleReplicaConnection();
  const replConf1 = ['REPLCONF', 'listening-port', CONFIG.port.toString()];
  const replConf2 = ['REPLCONF', 'capa', 'psync2'];
  const initialSyncCommand = ['PSYNC', '?', '-1'];

  console.log(`Connecting replica to ${host}:${port}`);

  client.connect(parseInt(port), host, () => {
    client.write(pingCommand);
  });
  let respCount = 0;

  client.on('data', (data: Buffer) => {
    const msg = Buffer.from(data).toString('utf-8');
    console.log('Received from master', msg);

    const parser = new RESPParser();
    const parsedInput = parser.parse(data);
    if (parsedInput?.value === 'PONG') {
      respCount++;
      client.write(_formatArrResponse(replConf1));
      client.write(_formatArrResponse(replConf2));
    } else if (parsedInput?.value === 'OK') {
      respCount++;
    }

    if (respCount === 3) {
      client.write(_formatArrResponse(initialSyncCommand));
      console.log('Initial sync complete');
    }
  });
}

async function handleParsedInput(
  parsedInput: RESPData | null,
  map: Map<string, string | undefined>
): Promise<(string | Buffer)[] | null> {
  if (!parsedInput || !parsedInput.value) {
    return null;
  }
  const parsedValue = parsedInput.value;

  const responses: (string | Buffer)[] = [];
  if (parsedInput.type === '*' && Array.isArray(parsedValue)) {
    const command = (parsedValue[0] ?? '')
      .toString()
      .toUpperCase() as RedisCommand;
    switch (command) {
      case RedisCommand.PING:
        responses.push(handlePing());
        break;
      case RedisCommand.ECHO:
        responses.push(handleEchoCommand(parsedValue));
        break;
      case RedisCommand.SET:
        responses.push(handleSetCommand(parsedValue, map, connectedReplicas));
        break;
      case RedisCommand.GET:
        responses.push(handleGetCommand(parsedValue, map));
        break;
      case RedisCommand.CONFIG:
        responses.push(handleConfigCommand(parsedValue));
        break;
      case RedisCommand.KEYS:
        responses.push(handleKeysCommand(parsedValue, map));
        break;
      case RedisCommand.INFO:
        responses.push(handleInfoCommand(parsedValue, map));
        break;
      case RedisCommand.REPLCONF:
        responses.push('REPLICA');
        responses.push(handleReplConfCommand(parsedValue));
        break;
      case RedisCommand.PSYNC:
        responses.push(handlePsyncCommand(map));
        const rdbBuffer = Buffer.from(EMPTY_RDB_HEX, 'hex');
        responses.push(`$${rdbBuffer.length.toString()}\r\n`);
        responses.push(rdbBuffer);
        break;
      default:
        responses.push('-ERR unknown command\r\n');
    }
  }
  return responses;
}

/**
 * Parses a serialized RDB file and populates a map with the key-value pairs
 * @param data Serialized RDB file content representing one redis DB
 * @param map Map to populate with key-value pairs
 */
function _loadRDBFile(
  data: string,
  map: Map<string, string | undefined>
): Map<string, string | undefined> {
  const buf = Buffer.from(data, 'hex');
  let cursor = 0;
  if (buf[cursor] !== 0xfe) {
    throw new Error('Invalid RDB file');
  }
  cursor++;
  const dbIndex = buf[cursor++];
  cursor++; // IMPORTANT: increment to skip over metadata length
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
      // advance cursor to the beginning of the expiry time
      cursor++;
      // next 8 bytes are the expiry time, in reverse order (little endian)
      const expiryTime = buf.subarray(cursor, cursor + 8);
      const expiryTimeInt = parseInt(expiryTime.reverse().toString('hex'), 16);
      const delay = expiryTimeInt - Date.now();

      cursor += 8;
      // next byte is key type, advance
      cursor++;

      // next byte is key length
      const keyLength = buf[cursor++];
      const key = buf.subarray(cursor, cursor + keyLength).toString();
      cursor += keyLength;

      // next byte is value length
      const valueLength = buf[cursor++];
      const value = buf.subarray(cursor, cursor + valueLength).toString();

      map.set(key, value);
      setTimeout(() => {
        map.set(key, undefined);
      }, delay);
      cursor += valueLength;
    } else {
      cursor++; // skip entry type; 00 is string

      const keyLength = buf[cursor++];
      const key = buf.subarray(cursor, cursor + keyLength);
      cursor += keyLength;

      const valueLength = buf[cursor++];
      const value = buf.slice(cursor, cursor + valueLength);
      cursor += valueLength;

      map.set(key.toString(), value.toString());
    }
  }
  return map;
}
