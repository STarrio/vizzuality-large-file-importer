import * as mongoose from 'mongoose';
import * as fs from 'fs';
import * as https from 'https';
import csv from 'csv-parser';
import * as mysql from 'mysql2';
const path = require('path');
require('dotenv').config();

const BATCH_SIZE = 50000;
const STATUS_TABLE_NAME = 'import_process_status';

// Create connection pool
const pool = mysql.createConnection({
    connectionLimit : 150, 
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    debug    :  false
});


/** Check if table import_process_status exists, and creates it if it doesn't. */
export async function ensureStatusTableInDB() {

    const sqlQuery =  'CREATE TABLE IF NOT EXISTS `' + STATUS_TABLE_NAME + '`'+ '(process_name VARCHAR(100) PRIMARY KEY, status VARCHAR(15));';

    pool.query(sqlQuery, (err) => {
        if (err) throw err;

        console.log('Table '+STATUS_TABLE_NAME+' exists.');
    });
}

/** Downloads and stores the file located at the url passed as argument.
 * @param url URL for the file to import
 */
export async function importFileFromURL(url: string, cb: any) {
    const fileName = url.split('/').pop();
    const parsedFilename = path.parse(fileName);

    // CSV data will be stored at table with the same name as file (without .csv)
    const tableName = parsedFilename.name;

    if(invalidFilename(parsedFilename)){
        // File name is not a valid .csv
        cb('FILE ERROR', 'File found at URL not valid.');
    } else {
        // Insert table name into import_process_status with IMPORTING status
        const insertProcessSql = 'REPLACE INTO `' + STATUS_TABLE_NAME + '`(process_name, status)'+ ' VALUES '+`('${tableName}', 'IMPORTING');`
        pool.query(insertProcessSql, (err) => {
            if (err) throw err;

            console.log('Import process created on table '+STATUS_TABLE_NAME);
        });

        console.log('Importing file...');

        // Stream the file from http.get to csv_parser
        https.get(url, (res) => {
        
            let batch = [];
            let batchCount = 0;
            
            
            const readable = res.pipe(csv());
            readable.on('headers', (headers) => {
                // First event emitted by csv(), reads headers from CSV file
                // and creates new table with them.
            
                // All fields are VARCHAR, since a defined schema for the CSV
                // is not provided.
                // TODO: A type inferer would be a nice addition.
                // TODO: This step could be replaced for creating table in first batch of data
                const headerFields = headers.map(h => '`' + h + '` VARCHAR(1000)').join(',');

                // Create table with name filePath and convert headers to fields
                const sqlQuery =  'CREATE TABLE IF NOT EXISTS `' + tableName + '`'+ `(${headerFields})`;


                pool.query(sqlQuery, (err) => {
                    if (err) throw err;

                    console.log('Table '+tableName+' created.');
                });

            })
            .on('data', async (row) => {
                // Event emitted for each row
                // Push row into batch, and asynchronously store batch into DB if batch.length == BATCH_SIZE
                batch.push(row);

                if(batch.length == BATCH_SIZE ){
                    // Pause read stream to avoid memory build-up
                    // This failed, I really don't know how to handle the memory issues.
                    res.pause();
        
                    batch.push(row);
                    insertBatch(tableName,batch,readable);

                    batchCount++;
                    batch = [];
                    res.resume();
                }
            })
            .on('end',() => {
                // Event emitted at EOF. 
                // Insert last batch, and update process status to FINISHED

                insertBatch(tableName,batch,readable);

                const updateProcessSql = 'UPDATE ' + STATUS_TABLE_NAME + ' '+`SET status = 'FINISHED' WHERE process_name = '${tableName}';`;
                pool.query(updateProcessSql, (err) => {
                    if (err) throw err;
                    console.log('Process finished');
                });
                
            })
        });

        // Return to client with message
        cb('IMPORTING FILE', 'Import process has started.');
    }
}

/** Function to check import status for file passed as url.
 * @param url Url for the file we are importing
 */
export async function getImportStatus(url: string, cb: any) {

    const fileName = url.split('/').pop();
    const tableName = path.parse(fileName).name;

    const statusSql = `SELECT status FROM ${STATUS_TABLE_NAME} where process_name = '${tableName}';`;

    pool.query(statusSql, (err,res) => {
        if (err) throw err;
        if(res[0]){
            cb(res[0].status);
        } else{
            cb('NOT FOUND');
        }
        
    });

}

/** Function to cancel an ongoing import process.
 * @param url Url for the file we are importing
 */
export async function cancelImport(url: string, cb: any) {

    const fileName = url.split('/').pop();
    const tableName = path.parse(fileName).name;

    const dropSql = `DROP TABLE IF EXISTS ${'`'+tableName+'`'};`;
    pool.query(dropSql, (err) => {
        if (err) {
            throw err;
        }
    });

    const updateProcessSql = 'UPDATE ' + STATUS_TABLE_NAME + ' '+`SET status = 'CANCELLED' WHERE process_name = '${tableName}';`;

    pool.query(updateProcessSql, (err) => {
        if (err) throw err;
        console.log('Import process for table '+tableName+' cancelled.');
    });

    cb('DATA IMPORT CANCELLED', 'Data import has been cancelled.');
}

/** Function to store a batch of data into DB
 * @param tableName Name of the table to store data to
 * @param batch Batch of data to store
 * @param readable Data reading stream
 */
async function insertBatch(tableName: string, batch: Object[], readable: any) {

    // Check status of current process
    checkStatus(tableName, async (status) => {
        if(status === 'CANCELLED') {
            // If status == CANCELLED, destroy reading stream to stop storing data
            await readable.destroy();
        } else {

            // Insert batch into DB

            // Get table fields from first row of batch
            const fields = Object.keys(batch[0]);
            const baseSql = `INSERT INTO ${'`'+tableName+'`'} (${fields.map(f => '`' + f + '`').join(',')}) \nVALUES\n`;

            const values = batch.map(e => '('+Object.values(e).map(v => `'${v}'`).join(',')+')').join(',\n');

            const fullSql = baseSql+values+';'

            pool.query(fullSql, (err) => {
                
                if (err && err.code === 'ER_NO_SUCH_TABLE') {
                    // Sometimes, when the process is cancelled and the table is dropped,
                    // some chunks of data are still being emitted, and they fail to be inserted

                    // TODO: find less sloppy way of dealing with this
                } else if (err) {
                    throw err;
                }
            });
        }
    });



}

/** Private function to check status of import process
 * @param tableName Name of the table for the import process
 * @cb Callback function
 */
async function checkStatus(tableName: string, cb: Function) {
    const statusSql = `SELECT status FROM ${STATUS_TABLE_NAME} where process_name = '${tableName}';`;

    pool.query(statusSql, (err,res) => {
        if (err) throw err;
        cb(res[0].status);
    });
}

/** Function to check validity of file passed to  importFileFromURL*/
function invalidFilename(parsedFilename: { ext: string, name: string } ): boolean {
    return parsedFilename.ext !== '.csv' ||
            parsedFilename.name === '' ||
            parsedFilename.name === null
}

