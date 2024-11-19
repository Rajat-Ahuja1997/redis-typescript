// make a type of ping, echo, set, get, config, and a type of string, error, integer, bulk, and array
//
export type TopLevelCommand =
  | 'PING'
  | 'ECHO'
  | 'SET'
  | 'GET'
  | 'CONFIG'
  | 'KEYS';
