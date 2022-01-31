import * as express from 'express';
import { importFileFromURL, getImportStatus, cancelImport } from './lib';

export const router = express.Router();

router.post('/import', async (req: any, res: any) => {
    await importFileFromURL(req.body.url, (status,msg) => {
        res.json({
            status: status,
            msg: msg
        });
    })
});

router.get('/status', async (req: any, res: any) => {

    await getImportStatus(req.body.url, (status) => {
        res.json({
            status: status
        });
    });
});

router.post('/cancel', async (req: any, res: any) => {
    await cancelImport(req.body.url, (status, msg) => {
        res.json({
            status: status,
            msg: msg
        });
    })
});