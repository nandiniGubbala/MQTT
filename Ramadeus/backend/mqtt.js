const mqtt = require('mqtt');
const { sendToFrontend } = require('./websocketServer'); // Import WebSocket logic
const { processMqttMessage } = require('./controllers/mqttData'); // Existing logic for processing data

// Connect to the MQTT broker
const mqttClient = mqtt.connect('mqtt://103.165.95.149:1883');

let lastSentTime = 0; // Initialize a variable to throttle real-time data

mqttClient.on('connect', () => {
  console.log('Connected to MQTT Broker');
  mqttClient.subscribe('RAMADEUS/+', (err) => {
    if (err) {
      console.error('Error subscribing to topic:', err);
    } else {
      console.log('Subscribed to RAMADEUS/ subtopics');
    }
  });
});

// Handle incoming messages
mqttClient.on('message', async (topic, message) => {
  const currentTime = Date.now();

  // Real-time data throttling (10-second interval)
  if (currentTime - lastSentTime >= 10 * 1000) {
    try {
      const parsedData = {
        topic,
        deviceId: message.toString().split(',')[0], // Extract the device ID
        values: message.toString().split(',').slice(1), // Extract remaining values
        timestamp: new Date(),
      };

      console.log('Sent real-time data:', parsedData);

      // Send real-time data to WebSocket clients
      sendToFrontend(parsedData);

      // Update the throttle timestamp
      lastSentTime = currentTime;
    } catch (err) {
      console.error('Error parsing MQTT message:', err);
    }
  }

  // Continue processing MQTT messages as per existing logic
  if (topic.startsWith('RAMADEUS/')) {
    processMqttMessage(topic, message.toString()); // Call your existing processing function
  }
});

// Handle errors
mqttClient.on('error', (err) => {
  console.error('Error connecting to MQTT Broker:', err);
});

module.exports = mqttClient;
