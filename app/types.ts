// make a type of ping, echo, set, get, config, and a type of string, error, integer, bulk, and array
//
export enum RedisCommand {
  PING = 'PING',
  ECHO = 'ECHO',
  SET = 'SET',
  GET = 'GET',
  CONFIG = 'CONFIG',
  KEYS = 'KEYS',
  INFO = 'INFO',
}

export const REDIS_PORT = 6379;
export const LOCALHOST = '127.0.0.1';
