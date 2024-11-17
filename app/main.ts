import * as net from 'net'; // net module allows you to create TCP servers

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log('Logs from your program will appear here!');

const server: net.Server = net.createServer((connection: net.Socket) => {
  connection.on('data', (data: Buffer) => {
    if (data.toString() === 'PING\r\n') {
      connection.write('+PONG\r\n');
    }
  });

  // Handle connection
});

server.listen(6379, '127.0.0.1'); // 6379 is the default port that Redis uses
