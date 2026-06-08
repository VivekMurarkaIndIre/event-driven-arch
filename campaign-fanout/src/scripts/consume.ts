import { createAnalyticsConsumer } from "../consumers/analyticsConsumer.js";

const QUEUE_URL =
  "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/campaign-analytics";

const consumer = createAnalyticsConsumer(QUEUE_URL);

process.on("SIGINT", () => {
  console.log("\nShutting down…");
  consumer.stop();
});

await consumer.start();
