const functions = require('@google-cloud/functions-framework');
const {PubSub} = require('@google-cloud/pubsub');
const {Storage} = require('@google-cloud/storage');
const {Datastore} = require('@google-cloud/datastore');
const imagemagick = require("imagemagick-stream");
const crypto = require('crypto');

const storage = new Storage();
const pubsub = new PubSub();
const datastore = new Datastore();

const topicName = 'tasks';
const bucketName = 'imagestorage-iq';
const temporalName = 'pending-md5';

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
  const taskId = crypto.randomUUID();
  const messageObject = {
    data: {
      path: originalImagePath,
      taskId: taskId
    },
  };
  const messageBuffer = Buffer.from(JSON.stringify(messageObject), 'utf8');

  // Publish message to be process
  topic.publishMessage({data: messageBuffer}).then(
    function(result) {
      const taskKey = datastore.key(['Task', taskId]);
      res.status(200).send({taskId: taskKey.name, status: 'SUBMITTED'});
      saveTask(taskKey, originalImagePath);
    }, function(error) {
      res.status(500).send('Cannot process your request. Retry in a moment.');
    }
  );  
});

async function saveTask(taskKey, path) {
  const task = {
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
exports.resize = (message, context) => {
  const messageBody = message.data
   ? Buffer.from(message.data, 'base64').toString():'';
   const messageObj = JSON.parse(messageBody);
   const path = messageObj.data.path;
   const taskId = messageObj.data.taskId;
  console.log('Path:' + path);
  console.log('TaskId:' + taskId);
  updateStatus(taskId, 'SETTLING').then(function(result) {
    Promise.all([resizeImage(path, 1024, taskId), resizeImage(path, 800, taskId)])
      .then(values => {
        console.log(values);
        if(values[0]=='OK' && values[1]=='OK'){
          updateStatus(taskId, 'SETTLED');
        } else {
          updateStatus(taskId, 'FAILED');
        }
      });
  });
};

async function resizeImage(path, size, taskId) {
  const bucket = storage.bucket(bucketName);
  const file = bucket.file(path);
  console.log('File Name:' + file.name);
  
  let fileName = file.name;
  const splitFileName = fileName.split(".");
  let fileNoExt = splitFileName[0];
  let fileExt = splitFileName[1];
  let srcStream = file.createReadStream();
  
  let resize = imagemagick().resize(size + 'x' + size);
  let newFilename = 'output/' + fileNoExt + '/' + size + '/' + temporalName + '.' +fileExt;
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
        // updateStatus(taskId, 'SIZE_' + size + '_READY');
        resolve('OK');
      });
  });
}

async function updateStatus(taskId, status) {
  const transaction = datastore.transaction();
  const taskKey = datastore.key(['Task', taskId]);
  try {
    await transaction.run();
    const [task] = await transaction.get(taskKey);
    // const [task] = await datastore.get(taskKey);
    switch(task.status){
      case 'SUBMITTED':
        task.status = status;
        break;
      case 'SETTLING':
        task.status = status;
        break;
      case 'SIZE_1024_READY':
        if(status == 'SIZE_800_READY') {task.status = 'SETTLED';}
        if(status == 'FAILED') {task.status = status;}
        break;
      case 'SIZE_800_READY':
        if(status == 'SIZE_1024_READY') {task.status = 'SETTLED';}
        if(status == 'FAILED') {task.status = status;}
        break;
      case 'FAILED':
        break;
    }
    task.lastModifiedAt = new Date();
    // await datastore.upsert({
    //   key: taskKey,
    //   data: task,
    // });
    transaction.save({
      key: taskKey,
      data: task,
    });
    await transaction.commit();
    console.log(`Task ${taskId} updated successfully to ${status}.`);
  } catch (err) {
    await transaction.rollback();
    throw err;
  }
}

async function renameFile(srcFileName) {
  // renames the file
  const file = storage.bucket(bucketName).file(srcFileName);
  const [metadata] = await file.getMetadata();
  const md5Value = metadata.md5Hash;
  const destFileName = srcFileName.replace(temporalName, md5Value)
  await file.rename(destFileName);

  console.log(
    `gs://${bucketName}/${srcFileName} renamed to gs://${bucketName}/${destFileName}.`
  );
}