const functions = require('@google-cloud/functions-framework');
const {PubSub} = require('@google-cloud/pubsub');
const {Storage} = require('@google-cloud/storage');

const storage = new Storage();
const pubsub = new PubSub();
const topicName = 'tasks';
const bucketName = 'images-resize764';
const imagemagick = require("imagemagick-stream");

/**
 * Create a task. It receives the original image path.
 *
 * @param {Object} req Function request.
 * @param {Object} res Function response.
 */
functions.http('task', (req, res) => {
  // Retrieve original image path from request
  var originalImagePath = req.body.path;
  console.log('Original Path:' + originalImagePath);
  console.log('Topic:' + topicName);
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
      res.status(200).send('Message published.');
    }, function(reason) {
      res.status(500).send('Cannot publish message');
    }
  );
  
});

/**
* Background Cloud Function to be triggered by Pub/Sub.
* This function is exported by index.js, and executed when
* the trigger topic receives a message.
*
* @param {object} message The Pub/Sub message.
* @param {object} context The event metadata.
*/
exports.helloPubSub = (message, context) => {
  const name = message.data
   ? Buffer.from(message.data, 'base64').toString()
   : 'World';

  resizeImage();
  console.log(`Hello, ${name}!`);
};

async function resizeImage() {
  const size = "64x64";
  const bucket = storage.bucket(bucketName);
  const file = bucket.file('pruebita123.jpg');
  console.log('File Name:' + file.name);
  let fileName = file.name;
  let srcStream = file.createReadStream();
  let newFilename = 'otro' + '_thumbnail.' + 'jpg';
  let gcsNewObject = bucket.file(newFilename);
  let dstStream = gcsNewObject.createWriteStream();
  let resize = imagemagick().resize(size).quality(90);
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
      });
  });
}