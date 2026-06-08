import { createHighVolumeConsumer } from "../consumers/highVolumeConsumer.js";

// Reads from campaign-high-volume, populated by the EventBridge rule that matches
// events where detail.audienceSize > 10000 on the campaign-bus.
const consumer = createHighVolumeConsumer(
  "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/campaign-high-volume",
);

process.on("SIGINT", () => { console.log("\nShutting down…"); consumer.stop(); });

await consumer.start();
