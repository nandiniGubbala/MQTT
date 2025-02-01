const express = require('express');
const {createNewMPIN, login } = require('../controllers/pinRegister');

const router = express.Router();

// Route to set MPIN
router.post('/set-mpin', createNewMPIN );

// Route to login with MPIN
router.post('/login', login);

module.exports = router;