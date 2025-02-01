const express = require('express');
const datarouter = express.Router();
const AggregatedData = require('../models/aggregateSchema');

// API Endpoint to fetch the latest averages
datarouter.get('/device-values', async (req, res) => {
  try {
    const latestAverages = await AggregatedData.find()
      .sort({ calculatedAt: -1 }) // Get the latest averages
      .limit(1); // Optional: Limit the number of results
    res.json(latestAverages);
  } catch (err) {
    console.error('Error fetching averages:', err);
    res.status(500).json({ error: 'Error fetching averages' });
  }
});

module.exports = datarouter;
