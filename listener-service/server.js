// listener-service/server.js
const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const { Kafka } = require('kafkajs');
const cors = require('cors');

const app = express();
const server = createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

const port = process.env.PORT || 3002;
const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:9092';

app.use(cors());
app.use(express.json());

// Kafka setup
const kafka = new Kafka({
  clientId: 'listener-service',
  brokers: [kafkaBroker],
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
});

const consumer = kafka.consumer({ 
  groupId: 'message-display-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
});

let isConsumerConnected = false;
let connectedClients = 0;

// Socket.IO connection handling
io.on('connection', (socket) => {
  connectedClients++;
  console.log(`🔗 Client connected. Total clients: ${connectedClients}`);
  
  // Send connection confirmation
  socket.emit('connected', { 
    message: 'Connected to message stream',
    timestamp: new Date().toISOString()
  });

  socket.on('disconnect', () => {
    connectedClients--;
    console.log(`🔌 Client disconnected. Total clients: ${connectedClients}`);
  });

  // Handle ping from client
  socket.on('ping', () => {
    socket.emit('pong', { timestamp: new Date().toISOString() });
  });
});

// Kafka consumer setup
const startConsumer = async () => {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: 'messages-topic', fromBeginning: false });
    
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const messageData = JSON.parse(message.value.toString());
          
          console.log(`📥 Received message: ${messageData.username} - ${messageData.message}`);
          
          // Broadcast to all connected clients
          io.emit('new-message', messageData);
          
        } catch (error) {
          console.error('❌ Error processing message:', error);
        }
      },
    });
    
    isConsumerConnected = true;
    console.log('✅ Kafka consumer connected and listening');
    
  } catch (error) {
    console.error('❌ Failed to start consumer:', error);
    setTimeout(startConsumer, 5000); // Retry after 5 seconds
  }
};

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy',
    kafkaConnected: isConsumerConnected,
    connectedClients: connectedClients,
    timestamp: new Date().toISOString()
  });
});

// Server-Sent Events endpoint (alternative to WebSocket)
app.get('/api/events', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Cache-Control'
  });

  // Send initial connection event
  res.write(`data: ${JSON.stringify({ 
    type: 'connected', 
    message: 'Connected to SSE stream',
    timestamp: new Date().toISOString()
  })}\n\n`);

  // Listen for new messages and send via SSE
  const messageHandler = (data) => {
    res.write(`data: ${JSON.stringify({ 
      type: 'message', 
      ...data 
    })}\n\n`);
  };

  io.on('connection', (socket) => {
    socket.on('new-message', messageHandler);
  });

  // Handle client disconnect
  req.on('close', () => {
    console.log('📡 SSE client disconnected');
  });
});

// Message history endpoint (for testing)
app.get('/api/messages', (req, res) => {
  res.json({ 
    message: 'This endpoint could return message history from a database',
    note: 'Currently messages are only streamed in real-time'
  });
});

// Graceful shutdown
const gracefulShutdown = async () => {
  console.log('🔄 Shutting down gracefully...');
  try {
    if (isConsumerConnected) {
      await consumer.disconnect();
      console.log('✅ Kafka consumer disconnected');
    }
    server.close(() => {
      console.log('✅ HTTP server closed');
      process.exit(0);
    });
  } catch (error) {
    console.error('❌ Error during shutdown:', error);
    process.exit(1);
  }
};

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

// Start server
server.listen(port, async () => {
  console.log(`🚀 Listener service running on port ${port}`);
  console.log(`📡 Connecting to Kafka broker: ${kafkaBroker}`);
  console.log(`🔗 WebSocket server ready`);
  console.log(`📡 SSE endpoint available at /api/events`);
  await startConsumer();
});