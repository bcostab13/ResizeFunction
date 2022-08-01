const {PubSub} = require('@google-cloud/pubsub');
const {Datastore} = require('@google-cloud/datastore');
const express = require('express')
const crypto = require('crypto');
const bodyParser = require('body-parser')

const projectId = 'innoqatest';
const topicName = 'tasks';


const pubsub = new PubSub({
    projectId: projectId,
  });
const datastore = new Datastore({
    projectId: projectId,
  });

const app = express()
app.use(bodyParser.json())
const port = 3000

/**
 * Create a task. It receives the original image path.
 *
 * @param {Object} req Function request.
 * Example:
 * {"path":"example.jpg"}
 * @param {Object} res Function response.
 * 
 * gcloud functions deploy task --runtime nodejs14 --trigger-http --allow-unauthenticated
 */
app.get('/task/:taskId', (req, res) => {
    const taskId = req.params.taskId;
    console.log(taskId);
    const taskKey = datastore.key(['Task', taskId]);
    datastore.get(taskKey).then(
      function (task) {
        res.status(200).send(task[0]);
      }, function (error) { 
        console.error(error);
        res.status(500).send(error);}
    );
});

/**
 * Get task detail. It receives the taskId.
 *
 * @param {Object} req Function request.
 * Example:
 * {"path":"example.jpg"}
 * @param {Object} res Function response.
 * 
 * gcloud functions deploy task --runtime nodejs14 --trigger-http --allow-unauthenticated
 */
app.post('/task', (req, res) => {
    // Retrieve original image path from request
  console.log(req);
  var originalImagePath = req.body.imagePath;
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

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
})