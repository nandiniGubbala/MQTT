const bcrypt = require('bcrypt');
const deviceRegister = require('../models/device');
require('dotenv').config();

const createDeviceID = async (req, res) => {
  try {
    const { deviceID, password } = req.body;

    // Validating input
    if (!deviceID || !password) {
      return res.status(400).json({ message: 'DeviceID and password are required' });
    }

    // Checking if the deviceID already exists
    const existingDevice = await deviceRegister.findOne({ deviceID });
    if (existingDevice) {
      return res.status(400).json({ message: 'DeviceID already in use' });
    }

    // Hash the password
    const hashedPassword = await bcrypt.hash(password, 10);

    // Saving the device to the database
    const newDevice = new deviceRegister({ deviceID, password: hashedPassword });
    await newDevice.save();

    res.status(201).json({ message: 'Device registered successfully' });
  } catch (error) {
    console.error('Error saving DeviceID:', error.message);
    res.status(500).json({ message: 'Internal Server Error' });
  }
};

module.exports = { createDeviceID };
