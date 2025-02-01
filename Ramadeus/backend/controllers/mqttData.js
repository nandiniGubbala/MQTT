const MqttData = require('../models/mqttDataSchema');

// Temporary variable to store the latest incoming data
let latestMqttData = {};

// Add incoming data to the temporary variable
function processMqttMessage(topic, message) {
  const data = message.split(',').map((value) => (value.trim() === '' ? null : value.trim()));

  // Merge new data with the previous values in `latestMqttData`
  latestMqttData = {
    subtopic: topic,
    deviceId: data[0] || latestMqttData.deviceId || '0',
    runningHours: data[1] ? parseFloat(data[1]) : latestMqttData.runningHours || 0,
    NO2: data[2] ? parseFloat(data[2]) : latestMqttData.NO2 || 0,
    O2: data[3] ? parseFloat(data[3]) : latestMqttData.O2 || 0,
    CO: data[4] ? parseFloat(data[4]) : latestMqttData.CO || 0,
    CH2O: data[5] ? parseFloat(data[5]) : latestMqttData.CH2O || 0,
    O3: data[6] ? parseFloat(data[6]) : latestMqttData.O3 || 0,
    temperature: data[7] ? parseFloat(data[7]) : latestMqttData.temperature || 0,
    humidity: data[8] ? parseFloat(data[8]) : latestMqttData.humidity || 0,
    PM1: data[9] ? parseFloat(data[9]) : latestMqttData.PM1 || 0,
    PM2_5: data[10] ? parseFloat(data[10]) : latestMqttData.PM2_5 || 0,
    PM10: data[11] ? parseFloat(data[11]) : latestMqttData.PM10 || 0,
    CO2: data[12] ? parseFloat(data[12]) : latestMqttData.CO2 || 0,
    VOC: data[13] ? parseFloat(data[13]) : latestMqttData.VOC || 0,
    value1: data[14] ? parseFloat(data[14]) : latestMqttData.value1 || 0,
  };
}

// Save the latest data to the database at 2-minute intervals
setInterval(async () => {
  if (!latestMqttData || !latestMqttData.deviceId) {
    console.log('No data to save at this interval.');
    return;
  }

  console.log('Processing data for saving...');

  // Handle '0' or missing values by replacing them with the last non-zero value from the database
  for (const key of Object.keys(latestMqttData)) {
    if (latestMqttData[key] === 0 || latestMqttData[key] === null) {
      const nonZeroValue = await getLastNonZeroValue(latestMqttData.deviceId, key);
      if (nonZeroValue !== null) {
        latestMqttData[key] = nonZeroValue;
      }
    }
  }

  // Save the processed data to the database
  try {
    const mqttData = new MqttData(latestMqttData);
    await mqttData.save();
    console.log('Data saved to MongoDB:', mqttData);

    // Clear the latest data after saving
    latestMqttData = {};
  } catch (err) {
    console.error('Error saving data to MongoDB:', err);
  }
}, 2 * 60 * 1000); // 2 minutes interval

// Query the database for the last non-zero value for a given field
async function getLastNonZeroValue(deviceId, field) {
  try {
    const record = await MqttData.findOne({
      deviceId,
      [field]: { $ne: 0 }, // Look for non-zero values
      createdAt: { $gte: new Date(Date.now() - 60 * 60 * 1000) }, // Within the past 1 hour
    })
      .sort({ createdAt: -1 }) // Most recent first
      .exec();

    return record ? record[field] : null;
  } catch (err) {
    console.error('Error querying for non-zero value:', err);
    return null;
  }
}

module.exports = { processMqttMessage };
