// input-service/server.js
const express = require('express');
const { Kafka } = require('kafkajs');
const cors = require('cors');

const app = express();
const port = process.env.PORT || 3001;
const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:9092';

// Middleware
app.use(express.json());
app.use(cors());

// Kafka setup
const kafka = new Kafka({
  clientId: 'input-service',
  brokers: [kafkaBroker],
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
});

const producer = kafka.producer({
  maxInFlightRequests: 1,
  idempotent: false,
  transactionTimeout: 30000,
});

let isProducerConnected = false;

// Connect to Kafka producer
const connectProducer = async () => {
  try {
    await producer.connect();
    isProducerConnected = true;
    console.log('✅ Kafka producer connected successfully');
  } catch (error) {
    console.error('❌ Failed to connect Kafka producer:', error);
    setTimeout(connectProducer, 5000); // Retry after 5 seconds
  }
};

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    kafkaConnected: isProducerConnected,
    timestamp: new Date().toISOString()
  });
});

// Message submission endpoint
app.post('/api/messages', async (req, res) => {
  try {
    const { username, message } = req.body;
    
    // Validate input
    if (!username || !message) {
      return res.status(400).json({ 
        error: 'Username and message are required' 
      });
    }

    if (!isProducerConnected) {
      return res.status(503).json({ 
        error: 'Kafka producer not connected. Please try again later.' 
      });
    }

    // Create message payload
    const messagePayload = {
      username: username.trim(),
      message: message.trim(),
      timestamp: new Date().toISOString(),
      id: `msg_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
    };

    // Send to Kafka topic
    await producer.send({
      topic: 'messages-topic',
      messages: [
        {
          key: messagePayload.username,
          value: JSON.stringify(messagePayload),
          timestamp: Date.now().toString()
        }
      ]
    });

    console.log(`📤 Message sent to Kafka: ${messagePayload.username} - ${messagePayload.message}`);
    
    res.json({ 
      status: 'received',
      messageId: messagePayload.id,
      timestamp: messagePayload.timestamp
    });

  } catch (error) {
    console.error('❌ Error processing message:', error);
    res.status(500).json({ 
      error: 'Failed to process message',
      details: error.message 
    });
  }
});

// Graceful shutdown
const gracefulShutdown = async () => {
  console.log('🔄 Shutting down gracefully...');
  try {
    if (isProducerConnected) {
      await producer.disconnect();
      console.log('✅ Kafka producer disconnected');
    }
    process.exit(0);
  } catch (error) {
    console.error('❌ Error during shutdown:', error);
    process.exit(1);
  }
};

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

// Start server
app.listen(port, async () => {
  console.log(`🚀 Input service running on port ${port}`);
  console.log(`📡 Connecting to Kafka broker: ${kafkaBroker}`);
  await connectProducer();
});