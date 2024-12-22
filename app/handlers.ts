import crypto from 'crypto';
import { CONFIG } from './main';
import {
  MASTER_ROLE,
  MASTER_REPL_ID,
  MASTER_REPL_OFFSET,
  SLAVE_ROLE,
  EMPTY_RDB_HEX,
} from './constants';
import * as net from 'net';
import { type RESPData, RedisCommand } from './types';

function handlePing(): string {
  return '+PONG\r\n';
}

export function handleReplicaConnection(): string {
  return _formatArrResponse(['PING']);
}

function handleEchoCommand(parsedValue: any): string {
  return _formatStringResponse(parsedValue[1]?.toString());
}

function handleSetCommand(
  parsedValue: any,
  map: Map<string, string | undefined>,
  replicas: Set<net.Socket>
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

  for (const replica of replicas) {
    replica.write(_formatArrResponse(['SET', key, value]));
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
    // filter out redis-specific keys
    const filteredEntries = Array.from(entries).filter(
      ([key]) => key !== MASTER_REPL_ID && key !== MASTER_REPL_OFFSET
    );
    return _formatArrResponse(filteredEntries.map(([key]) => key));
  }
  return '-ERR not implemented\r\n';
}

function handleInfoCommand(
  parsedValue: any,
  map: Map<string, string | undefined>
) {
  const nestedCommand = parsedValue[1]?.toString();
  if (!nestedCommand) {
    return '-ERR missing second argument for INFO\r\n';
  }
  switch (nestedCommand) {
    case 'replication':
      if (CONFIG.replicaOf) {
        return _formatStringResponse(SLAVE_ROLE);
      } else {
        const masterReplicationId = map.get(MASTER_REPL_ID);
        const offset = map.get(MASTER_REPL_OFFSET);
        return _formatStringResponseWithMultipleWords([
          MASTER_ROLE,
          `${MASTER_REPL_ID}:${masterReplicationId}`,
          `${MASTER_REPL_OFFSET}:${offset}`,
        ]);
      }
    default:
      return '-ERR unsupported nested command for INFO\r\n';
  }
}

function handleReplConfCommand(parsedValue: any) {
  return _formatStringResponse('OK');
}

function handlePsyncCommand(map: Map<string, string | undefined>) {
  const msg = `FULLRESYNC ${map.get(MASTER_REPL_ID)} ${map.get(
    MASTER_REPL_OFFSET
  )}`;
  return _formatStringResponse(msg);
}

function _formatStringResponse(value: string | undefined): string {
  if (!value) {
    return '-1\r\n';
  }
  return `$${value.length}\r\n${value}\r\n`;
}

function _formatStringResponseWithMultipleWords(words: string[]): string {
  if (!words.length) {
    return '-1\r\n';
  }
  let base = `$${words.join('\r\n').length}`;
  words.forEach((word) => {
    base += `\r\n${word}`;
  });
  base += '\r\n';
  return base;
}

function _formatArrResponse(data: string[]): string {
  let response = `*${data.length}\r\n`;
  data.forEach((d) => {
    response += `$${d.length}\r\n${d}\r\n`;
  });
  return response;
}

function _generateRandomString(length: number): string {
  return crypto.randomBytes(length).toString('hex').slice(0, length);
}

export async function handleParsedInput(
  parsedInput: RESPData | null,
  map: Map<string, string | undefined>,
  connectedReplicas: Set<net.Socket>
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
        if (CONFIG.replicaOf) {
          console.log(parsedValue);
        }
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

export { _formatArrResponse, _formatStringResponse, _generateRandomString };
