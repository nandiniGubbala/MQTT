const mongoose = require('mongoose');

const mqttDataSchema = new mongoose.Schema({
  subtopic: {
    type: String,
    required: true,
  },  
  deviceId: {
        type: String,
        required: true,
    },
    
    runningHours: {
        type: Number,
        default: 0,
    },
    NO2: {
        type: Number,
        default: 0,
    },
    O2: {
        type: Number,
        default: 0,
    },
    CO: {
      type: Number,
      default: 0,
    },
    CH2O: {
        type: Number,
        default: 0,
    },
    O3: {
        type: Number,
        default: 0,
    },
    temperature: {
        type: Number,
        default: 0,
      },
      humidity: {
        type: Number,
        default: 0,
      },
      PM1: {
        type: Number,
        default: 0,
      },
      PM2_5: {
        type: Number,
        default: 0,
      },
      PM10: {
        type: Number,
        default: 0,
      },
      CO2: {
        type: Number,
        default: 0,
      },
      VOC: {
        type: Number,
        default: 0,
      },
      value1: {
        type: Number,
        default: 0,
      },
    }, { timestamps: true }
);
    
module.exports = mongoose.model('MqttData', mqttDataSchema);
    