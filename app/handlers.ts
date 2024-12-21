import crypto from 'crypto';
import { CONFIG } from './main';
import {
  MASTER_ROLE,
  MASTER_REPL_ID,
  MASTER_REPL_OFFSET,
  SLAVE_ROLE,
} from './constants';
import * as net from 'net';

export function handlePing(): string {
  return '+PONG\r\n';
}

export function handleReplicaConnection(): string {
  return _formatArrResponse(['PING']);
}

export function handleEchoCommand(parsedValue: any): string {
  return _formatStringResponse(parsedValue[1]?.toString());
}

export function handleSetCommand(
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

export function handleGetCommand(
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

export function handleConfigCommand(parsedValue: any) {
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

export function handleKeysCommand(
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

export function handleInfoCommand(
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

export function handleReplConfCommand(parsedValue: any) {
  return _formatStringResponse('OK');
}

export function handlePsyncCommand(map: Map<string, string | undefined>) {
  const msg = `FULLRESYNC ${map.get(MASTER_REPL_ID)} ${map.get(
    MASTER_REPL_OFFSET
  )}`;
  return _formatStringResponse(msg);
}

export function _formatStringResponse(value: string | undefined): string {
  if (!value) {
    return '-1\r\n';
  }
  return `$${value.length}\r\n${value}\r\n`;
}

export function _formatStringResponseWithMultipleWords(
  words: string[]
): string {
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

export function _formatArrResponse(data: string[]): string {
  let response = `*${data.length}\r\n`;
  data.forEach((d) => {
    response += `$${d.length}\r\n${d}\r\n`;
  });
  return response;
}

export function _generateRandomString(length: number): string {
  return crypto.randomBytes(length).toString('hex').slice(0, length);
}
