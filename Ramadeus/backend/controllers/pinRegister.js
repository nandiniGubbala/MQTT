const bcrypt = require('bcrypt');
const pinRegister = require('../models/pin');
require('dotenv').config();

const createNewMPIN = async (req, res) => {
  try {
    const { userName, mpin } = req.body;

    if (!userName || !mpin) {
      return res.status(400).json({ message: 'Username and MPIN are required' });
    }

    const existinguserName = await pinRegister.findOne({ userName });
    if (existinguserName) {
      return res.status(400).json({ msg: 'User name already in use' });
    }

    // Hash the MPIN
    const hashedMpin = await bcrypt.hash(mpin, 10);

    // Save to database
    const newMpin = new pinRegister({ userName, mpin: hashedMpin });
    await newMpin.save();

    res.status(201).json({ message: 'MPIN set successfully' });
  } catch (error) {
    console.error('Error setting MPIN:', error.message);
    res.status(500).json({ message: 'Internal Server Error' });
  }
};

const login = async (req, res) => {
    try {
      const { userName, mpin } = req.body;
  
      if (!userName || !mpin) {
        return res.status(400).json({ message: 'Username and MPIN are required' });
      }
  
      // Find user by username
      const user = await pinRegister.findOne({ userName });
      if (!user) {
        return res.status(404).json({ message: 'User not found' });
      }
  
      // Compare MPIN with the hashed MPIN
      const isMatch = await bcrypt.compare(mpin, user.mpin); 
      if (!isMatch) {
        return res.status(401).json({ message: 'Invalid MPIN' });
      }
  
      res.status(200).json({ message: 'Login successful' });
    } catch (error) {
      console.error('Error during login:', error.message);
      res.status(500).json({ message: 'Internal Server Error' });
    }
  };
  

module.exports = { createNewMPIN, login };
