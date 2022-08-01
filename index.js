const functions = require('@google-cloud/functions-framework');
const {PubSub} = require('@google-cloud/pubsub');
const {Storage} = require('@google-cloud/storage');
const {Datastore} = require('@google-cloud/datastore');
const imagemagick = require("imagemagick-stream");
const crypto = require('crypto');

const storage = new Storage();
const datastore = new Datastore();

const bucketName = 'imagestorage-iq'; // use here your own bucket name
const temporalName = 'pending-md5';

/**
* Background Cloud Function to be triggered by Pub/Sub.
* It is executed when the trigger topic receives a message.
* Create two thumbnails from the original image.
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

/**
 * Function that generate a new scaled image as output.
 * 
 * @param {string} path Path from original image
 * @param {int} size Size to scale
 * @param {string} taskId Id of related task
 * @returns 
 */
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
        renameFile(taskId, path, newFilename);
        resolve('OK');
      });
  });
}

/**
 * Function to update the status of a task
 * 
 * @param {string} taskId Id of related task 
 * @param {string} status New status to be set
 */
async function updateStatus(taskId, status) {
  const transaction = datastore.transaction();
  const taskKey = datastore.key(['Task', taskId]);
  try {
    await transaction.run();
    const [task] = await transaction.get(taskKey);
    switch(task.status){
      case 'SUBMITTED':
        task.status = status;
        break;
      case 'SETTLING':
        task.status = status;
        break;
      case 'FAILED':
        break;
    }
    task.lastModifiedAt = new Date();
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

/**
 * Utility function to rename image with its md5hash
 * 
 * @param {string} taskId Id of related task
 * @param {string} originalPath Original path of the image
 * @param {string} srcFileName  Path of rescaled image
 */
async function renameFile(taskId, originalPath, srcFileName) {
  const file = storage.bucket(bucketName).file(srcFileName);
  const [metadata] = await file.getMetadata();
  const md5Value = metadata.md5Hash;
  const destFileName = srcFileName.replace(temporalName, md5Value)
  await file.rename(destFileName);

  saveImage(taskId, metadata.size, md5Value, destFileName, originalPath);
  console.log(
    `gs://${bucketName}/${srcFileName} renamed to gs://${bucketName}/${destFileName}.`
  );
}

/**
 * Function to save image details.
 * 
 * @param {string} taskId Id of related task
 * @param {string} resolution Resolution of output image
 * @param {string} md5hash MD5 of the image
 * @param {string} path Path of the image
 * @param {string} originalPath Path of the original image
 */
async function saveImage(taskId, resolution, md5hash, path, originalPath) {
  const image = {
    taskId: taskId,
    resolution: resolution,
    md5hash: md5hash,
    path: path,
    originalPath: originalPath,
    createdAt: new Date()
  };

  const entity = {
    key: datastore.key('Image'),
    data: image,
  };

  await datastore.upsert(entity);
  // Task inserted successfully.
}