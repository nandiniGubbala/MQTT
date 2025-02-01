const WebSocket = require('ws');
const server = new WebSocket.Server({ port: 8083 });

server.on('connection', (socket) => {
  console.log('WebSocket client connected');

  // Store the client socket for broadcasting real-time data
  socket.on('message', (message) => {
    console.log('Received message from client:', message);
  });

  socket.on('close', () => {
    console.log('WebSocket client disconnected');
  });
});

function sendToFrontend(data) {
  server.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data)); // Send data to the connected client
    }
  });
}

module.exports = { sendToFrontend };
