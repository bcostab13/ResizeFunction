const functions = require('@google-cloud/functions-framework');
const {PubSub} = require('@google-cloud/pubsub');

const pubsub = new PubSub();
const topicName = 'tasks';

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