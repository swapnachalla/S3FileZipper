const AWS = require('aws-sdk');
const archiver = require('archiver');
const s3 = new AWS.S3();

const MAX_RETRIES = 3;

//Test Message format
/*{
    "inputFolder": "input-folder-path",
    "outputFolder": "output-folder-path",
    "outputFileName": "optional-output.zip"
  }
*/
//Create lambda layer for archiver 
//npm install aws-sdk archiver 
// This Lambda function processes messages from an SQS queue, creates a zip file from S3 objects,
exports.handler = async (event) => {
    try {
        const records = event.Records || [];
        const tasks = records.map(record => processMessage(record));
        await Promise.all(tasks); // Handle multiple messages concurrently
        console.log('All tasks completed successfully.');
    } catch (error) {
        console.error('Error processing messages:', error);
        throw error;
    }
};
async function processMessage(record) {
    const messageBody = JSON.parse(record.body);
    const inputFolder = messageBody.inputFolder;
    const outputFolder = messageBody.outputFolder;
    const outputFileName = messageBody.outputFileName || 'output.zip'; // Default output file name

    if (!inputFolder || !outputFolder) {
        throw new Error('Invalid message: Both inputFolder and outputFolder paths are required.');
    }

    const outputKey = `${outputFolder}/${outputFileName}`;
    let retryCount = 0;

    while (retryCount <= MAX_RETRIES) {
        try {
            await createZipFile(inputFolder, outputKey);
            console.log(`Successfully processed message: ${record.messageId}`);
            return;
        } catch (error) {
            retryCount++;
            console.error(`Error processing message (Attempt ${retryCount}/${MAX_RETRIES}):`, error);

            if (retryCount > MAX_RETRIES) {
                throw new Error(`Failed after ${MAX_RETRIES} retries: ${error.message}`);
            }
        }
    }
}
async function createZipFile(inputFolder, outputKey) {
    const bucketName = process.env.BUCKET_NAME;
    if (!bucketName) {
        throw new Error('Environment variable BUCKET_NAME is not set.');
    }
    // Retrieve the compression level from an environment variable
    const compressionLevel = parseInt(process.env.ZLIB_COMPRESSION_LEVEL, 10) || 9; // Default to 9 if not set

    // Create a pass-through stream for the zip file
    const archive = archiver('zip', { zlib: { level: compressionLevel } });  // High compression
    const uploadStream = s3.upload({
        Bucket: bucketName,
        Key: outputKey,
        Body: archive,
    }).promise();

    archive.on('error', (err) => {
        throw new Error(`Archiver error: ${err.message}`);
    });

    const objects = await listS3Objects(bucketName, inputFolder);

    for (const object of objects) {
        const fileStream = s3.getObject({ Bucket: bucketName, Key: object.Key }).createReadStream();
        archive.append(fileStream, { name: object.Key.replace(`${inputFolder}/`, '') });
    }

    await archive.finalize(); // Finalize the archive
    await uploadStream; // Wait for the upload to complete
    console.log(`Zip file created successfully at ${outputKey}`);
}

async function listS3Objects(bucketName, folderPath) {
    const params = {
        Bucket: bucketName,
        Prefix: folderPath.endsWith('/') ? folderPath : `${folderPath}/`,
    };

    let objects = [];
    let continuationToken;

    do {
        const response = await s3.listObjectsV2({
            ...params,
            ContinuationToken: continuationToken,
        }).promise();

        objects = objects.concat(response.Contents || []);
        continuationToken = response.NextContinuationToken;
    } while (continuationToken);

    return objects.filter(obj => obj.Size > 0); // Exclude folders
}
