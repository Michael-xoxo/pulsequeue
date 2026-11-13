const mongoose = require('mongoose');

const failedJobSchema = new mongoose.Schema(
  {
    jobId: {
      type: String,
      required: true,
      unique: true,
    },
    userId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'User',
      required: true,
      index: true,
    },
    jobType: {
      type: String,
      default: 'BULK_INVOICE_SYNC',
    },
    payload: {
      type: Object,
      default: {},
    },
    failedReason: {
      type: String,
      default: '',
    },
    stacktrace: {
      type: [String],
      default: [],
    },
    attemptsMade: {
      type: Number,
      default: 0,
    },
    failedAt: {
      type: Date,
      default: Date.now,
    },
  },
  { timestamps: true }
);

module.exports = mongoose.models.FailedJob || mongoose.model('FailedJob', failedJobSchema);
