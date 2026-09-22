import {kafkaClient} from './kafka-client.js'

const LOCATION_TOPIC='location-updates'

async function setup(){
  // Create the topic used by the location producer and consumer.
    const admin=kafkaClient.admin()
    console.log("kafka admin connect")
    await admin.connect();
    console.log("Adming Connection Success...");

    console.log("Creating Topic location-updates");
    await admin.createTopics({
        topics: [
      {
        topic: LOCATION_TOPIC,
        // Two partitions allow up to two consumers in the same group to work in parallel.
        numPartitions: 2,
      },
    ]
    });
    console.log(`Topic Created Success [${LOCATION_TOPIC}]`);

  console.log("Disconnecting Admin..");
  await admin.disconnect();
}

setup()