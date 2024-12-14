import crypto from 'crypto';
import { CONFIG } from './main';

export function handlePing(): string {
  return '+PONG\r\n';
}

export function handleEchoCommand(parsedValue: any): string {
  return _formatStringResponse(parsedValue[1]?.toString());
}

export function handleSetCommand(
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
    return _formatArrResponse(Array.from(entries).map(([key]) => key));
  }
  return '-ERR not implemented\r\n';
}

export function handleInfoCommand(parsedValue: any) {
  const masterReplicationId = _generateRandomString(40);
  const offset = 0;
  const nestedCommand = parsedValue[1]?.toString();
  if (!nestedCommand) {
    return '-ERR missing second argument for INFO\r\n';
  }
  switch (nestedCommand) {
    case 'replication':
      if (CONFIG.replicaOf) {
        return _formatStringResponse('role:slave');
      } else {
        return _formatStringResponseWithMultipleWords([
          'role:master',
          `master_replid:${masterReplicationId}`,
          `master_repl_offset:${offset}`,
        ]);
      }
    default:
      return '-ERR unsupported nested command for INFO\r\n';
  }
}

function _formatStringResponse(value: string | undefined): string {
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
