const express = require('express');
const authGuard = require('../middleware/authGuard');
const { enqueueBatchJob, getJobStatus } = require('../controllers/batchController');

const router = express.Router();

router.post('/batch', authGuard, enqueueBatchJob);
router.get('/:id', authGuard, getJobStatus);

module.exports = router;
