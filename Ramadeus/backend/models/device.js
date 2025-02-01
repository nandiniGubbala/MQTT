const mongoose = require('mongoose');

const deviceSchema = new mongoose.Schema({
    deviceID: {
        type: String,
        required: true,
        unique: true
    },
    password: {
        type: String,
        required: true,
    },
},
    {timestamps: true}
)

module.exports = mongoose.model('id', deviceSchema);