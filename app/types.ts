// Redis Commands
export enum RedisCommand {
  PING = 'PING',
  ECHO = 'ECHO',
  SET = 'SET',
  GET = 'GET',
  CONFIG = 'CONFIG',
  KEYS = 'KEYS',
  INFO = 'INFO',
  REPLCONF = 'REPLCONF',
  PSYNC = 'PSYNC',
}

// RESP Protocol Types
export enum RESPType {
  String = '+',
  Integer = ':',
  Bulk = '$',
  Array = '*',
  Error = '-',
}

export interface RESPData {
  type: RESPType;
  value: string | number | null | (string | number | null)[];
}

// Configuration
export interface Config {
  dir: string;
  dbFileName: string;
  port: number;
  replicaOf: string | null;
}

// Constants
export const REDIS_PORT = 6379;
export const LOCALHOST = '127.0.0.1';
