import { createNotificationConsumer } from "../consumers/notificationConsumer.js";

const consumer = createNotificationConsumer(
  "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/campaign-notifier",
);

process.on("SIGINT", () => { console.log("\nShutting down…"); consumer.stop(); });

await consumer.start();
