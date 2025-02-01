const mongoose = require('mongoose');

const pinSchema = new mongoose.Schema({
    userName: {
        type: String,
        required: true,
        unique: true
    },
    mpin: {
        type: String,
        required: true,
    },
},
    {timestamps: true}
)

module.exports = mongoose.model('pin', pinSchema);