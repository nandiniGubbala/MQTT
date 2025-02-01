const express = require('express');
const { createDeviceID } = require('../controllers/deviceRegister');

const deviceRouter = express.Router();

// POST /api/devices/register
deviceRouter.post('/register', createDeviceID);

module.exports = deviceRouter;
