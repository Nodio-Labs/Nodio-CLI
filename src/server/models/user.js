const mongoose = require('mongoose');

const userSchema = new mongoose.Schema(
  {
    userId: { type: String, required: true, unique: true, index: true },
    email: { type: String, required: true, unique: true, index: true },
    passwordHash: { type: String, required: true },
    argon2Salt: { type: String, required: true },
    apiToken: { type: String, required: true, unique: true, index: true },
    createdAt: { type: Date, default: Date.now }
  },
  { timestamps: false }
);

module.exports = {
  UserModel: mongoose.model('User', userSchema)
};
