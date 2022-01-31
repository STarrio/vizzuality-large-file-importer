import express from 'express';
import { router } from './router';
import bodyParser from 'body-parser';
import { ensureStatusTableInDB } from './lib';

require('dotenv').config();

const app = express();
const PORT = process.env.NODE_PORT;

app.use(bodyParser.json());

app.use('/files', router)

ensureStatusTableInDB();

app.listen(PORT, () => {
  console.log(`Listening on port ${PORT}.`);
});