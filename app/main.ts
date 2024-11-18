import * as net from 'net';
import RESPParser from './parser';

const server: net.Server = net.createServer((connection: net.Socket) => {
  const parser = new RESPParser();
  connection.on('data', (data: Buffer) => {
    const parsedInput = parser.parse(data);
    if (!parsedInput) {
      return;
    }

    const response = handleParsedInput(parsedInput);
    if (response) {
      // Send response in RESP format
      console.log(response);
      connection.write(`+${response}\r\n`);
    }
  });
});

server.listen(6379, '127.0.0.1');

function handleParsedInput(parsedInput: any) {
  console.log('parsedInput', parsedInput);
  if (parsedInput.type === '*' && Array.isArray(parsedInput.value)) {
    const command = parsedInput.value[0]?.toString().toUpperCase();

    switch (command) {
      case 'PING':
        return 'PONG';
      case 'ECHO':
        return parsedInput.value[1]?.toString() || '';
      default:
        return null;
    }
  }
  return null;
}
