const express = require('express');
const dotenv = require('dotenv');
const connectDB = require('./config/db');
const userRoutes = require('./routes/userRoutes');
const deviceRoutes = require('./routes/deviceRoutes');
const averagesRouter = require('./routes/dataRoutes');

require('./mqtt');
require('./websocketServer'); 
const CalculateAndSaveAverage = require('./controllers/mqttAverageData');

dotenv.config();
connectDB();

CalculateAndSaveAverage();

const app = express();
app.use(express.json());

//Routes
app.use('/api/users', userRoutes);
app.use('/api/devices', deviceRoutes);
app.use('/api/data', averagesRouter);


const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});