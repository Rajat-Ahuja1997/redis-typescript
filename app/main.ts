import * as net from 'net';
import * as fs from 'fs';
import RESPParser from './parser';
import { LOCALHOST, REDIS_PORT, type Config } from './types';
import {
  _formatArrResponse,
  _formatStringResponse,
  _generateRandomString,
  handleParsedInput,
  handleReplicaConnection,
} from './handlers';
import { MASTER_REPL_ID, MASTER_REPL_OFFSET } from './constants';

const parameters = Bun.argv.slice(2);
const dirIndex = parameters.indexOf('--dir');
const dbFilenameIndex = parameters.indexOf('--dbfilename');
const portIndex = parameters.indexOf('--port');
const replicationIndex = parameters.indexOf('--replicaof');

export const CONFIG: Config = {
  dir: parameters[dirIndex + 1] ?? '',
  dbFileName: parameters[dbFilenameIndex + 1] ?? '',
  port: portIndex !== -1 ? parseInt(parameters[portIndex + 1]) : REDIS_PORT,
  replicaOf: replicationIndex !== -1 ? parameters[replicationIndex + 1] : null,
};

const replicas: Set<net.Socket> = new Set();
let redisMap: Map<string, string | undefined> = new Map();
redisMap.set(MASTER_REPL_ID, _generateRandomString(40));
redisMap.set(MASTER_REPL_OFFSET, '0');

// Load RDB file at startup
try {
  const filepath = `${CONFIG.dir}/${CONFIG.dbFileName}`;
  const content = fs.readFileSync(filepath);
  const hexContent = content.toString('hex');
  const db = hexContent.slice(hexContent.indexOf('fe'));
  redisMap = _loadRDBFile(db, redisMap);
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

    const response = await handleParsedInput(parsedInput, redisMap, replicas);
    if (response) {
      for (const msg of response) {
        if (typeof msg === 'string' && msg === 'REPLICA') {
          replicas.add(connection);
        } else {
          connection.write(msg);
        }
      }
    }
  });

  connection.on('close', () => {
    console.log('connection closed');
    replicas.delete(connection);
    connection.end();
  });
});

server.listen(CONFIG.port, LOCALHOST); // start server

if (CONFIG.replicaOf) {
  processReplicaConnection();
}

function processReplicaConnection() {
  if (!CONFIG.replicaOf) {
    console.log('No replica connection');
    return;
  }
  const [host, port] = CONFIG.replicaOf.split(' ');
  const client = new net.Socket();
  const pingCommand = handleReplicaConnection();
  const replConf1 = ['REPLCONF', 'listening-port', CONFIG.port.toString()];
  const replConf2 = ['REPLCONF', 'capa', 'psync2'];
  const initialSyncCommand = ['PSYNC', '?', '-1'];

  client.connect(parseInt(port), host, () => {
    client.write(pingCommand);
  });
  let respCount = 0;

  client.on('data', async (data: Buffer) => {
    const parser = new RESPParser();
    const parsedInput = parser.parse(data);

    if (parsedInput?.value === 'PONG') {
      respCount++;
      client.write(_formatArrResponse(replConf1));
      client.write(_formatArrResponse(replConf2));
    } else if (parsedInput?.value === 'OK') {
      respCount++;
    } else {
      const dataStr = data.toString();
      if (dataStr.startsWith('+FULLRESYNC')) {
        // try to parse the data as a full sync
        const firstLineEnd = data.indexOf('\r\n');
        const rdbLengthEnd = data.indexOf('\r\n', firstLineEnd + 2);
        const rdbStart = rdbLengthEnd + 2;

        const rdbEndMarker = data.indexOf(0xff, rdbStart);
        // advance past market and 8 byte checksum
        const commandsStart = rdbEndMarker + 9;
        const remainingData = data.subarray(commandsStart);
        const decodedData = Buffer.from(remainingData).toString('utf-8');

        // Split RESP array commands, preserving the RESP format
        const lines = decodedData
          .split('*')
          .filter((line) => line.trim().length > 0)
          .map((line) => '*' + line);

        const parser = new RESPParser();
        for (const line of lines) {
          const parsedInput = parser.parse(line);
          await handleParsedInput(parsedInput, redisMap, replicas);
        }
      }
    }

    if (respCount === 3) {
      client.write(_formatArrResponse(initialSyncCommand));
      console.log('Initial sync complete');
    }
  });
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
