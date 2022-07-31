const functions = require('@google-cloud/functions-framework');
const {PubSub} = require('@google-cloud/pubsub');
const {Storage} = require('@google-cloud/storage');
const {Datastore} = require('@google-cloud/datastore');
const crypto = require('crypto');
const MD5 = require("crypto-js/md5");

const storage = new Storage();
const pubsub = new PubSub();
const datastore = new Datastore();

const topicName = 'tasks';
const bucketName = 'imagestorage-iq';
const imagemagick = require("imagemagick-stream");

/**
 * Create a task. It receives the original image path.
 *
 * @param {Object} req Function request.
 * @param {Object} res Function response.
 * 
 * gcloud functions deploy task --runtime nodejs14 --trigger-http --allow-unauthenticated
 */
functions.http('task', (req, res) => {
  // Retrieve original image path from request
  var originalImagePath = req.body.path;
  console.log('Original Image Path:' + originalImagePath);

  // Get topic to publish the task request
  const topic = pubsub.topic(topicName);
  // Prepare message to publish
  const messageObject = {
    data: {
      message: originalImagePath,
    },
  };
  const messageBuffer = Buffer.from(JSON.stringify(messageObject), 'utf8');

  // Publish message to be process
  topic.publishMessage({data: messageBuffer}).then(
    function(result) {
      taskId = crypto.randomUUID();
      res.status(200).send({taskId: taskId, status: 'SUBMITTED'});
      saveTask(originalImagePath, taskId);
    }, function(error) {
      res.status(500).send('Cannot process your request. Retry in a moment.');
    }
  );
  
});

async function saveTask(path, uuid) {
  const taskKey = datastore.key('Task');
  const task = {
    taskId: uuid,
    status: 'SUBMITTED',
    originalImagePath: path,
    createdAt: new Date(),
    lastModifiedAt: new Date()
  };

  const entity = {
    key: taskKey,
    data: task,
  };

  await datastore.upsert(entity);
  // Task inserted successfully.
}

/**
* Background Cloud Function to be triggered by Pub/Sub.
* This function is exported by index.js, and executed when
* the trigger topic receives a message.
*
* @param {object} message The Pub/Sub message.
* @param {object} context The event metadata.
*/
exports.helloPubSub = (message, context) => {
  const messageBody = message.data
   ? Buffer.from(message.data, 'base64').toString():'';
   const messageObj = JSON.parse(messageBody);
   const path = messageObj.data.message;
  console.log(path);
  resizeImage(path, 1024);
  resizeImage(path, 800);
};

async function resizeImage(path, size) {
  const bucket = storage.bucket(bucketName);
  const file = bucket.file(path);
  console.log('File Name:' + file.name);
  
  let fileName = file.name;
  const splitFileName = fileName.split(".");
  let fileNoExt = splitFileName[0];
  let fileExt = splitFileName[1];
  let srcStream = file.createReadStream();
  
  let resize = imagemagick().resize(size + 'x' + size);
  let newFilename = 'output/' + fileNoExt + '/' + size + '/' + 'pending-md5' + '.' +fileExt;
  console.log('New File:' + newFilename);
  let gcsNewObject = bucket.file(newFilename);
  let dstStream = gcsNewObject.createWriteStream();
  srcStream.pipe(resize).pipe(dstStream);

  return new Promise((resolve, reject) => {
      dstStream
      .on("error", (err) => {
        console.log(`Error: ${err}`);
        reject(err);
      })
      .on("finish", () => {
        console.log(`Success: ${fileName} → ${newFilename}`);
        
        gcsNewObject.setMetadata(
        {
            contentType: 'image/'+ 'jpg'
        }, function(err, apiResponse) {});
        renameFile(newFilename);
      });
  });
}

async function renameFile(srcFileName) {
  // renames the file
  const file = storage.bucket(bucketName).file(srcFileName);
  const md5Value = MD5(file).toString();
  const destFileName = srcFileName.replace('pending-md5', md5Value)
  await file.rename(destFileName);

  console.log(
    `gs://${bucketName}/${srcFileName} renamed to gs://${bucketName}/${destFileName}.`
  );
}