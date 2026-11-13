const express = require('express');
const authGuard = require('../middleware/authGuard');
const {
  enqueueBatchJob,
  listUserJobs,
  getDeadLetterJobs,
  getJobStatus,
} = require('../controllers/batchController');

const router = express.Router();

router.get('/', authGuard, listUserJobs);
router.post('/batch', authGuard, enqueueBatchJob);
router.get('/dlq', authGuard, getDeadLetterJobs);
router.get('/:id', authGuard, getJobStatus);

module.exports = router;
