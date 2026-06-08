import { createEmailConsumer } from "../consumers/emailConsumer.js";

const consumer = createEmailConsumer(
  "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/campaign-processor",
);

process.on("SIGINT", () => { console.log("\nShutting down…"); consumer.stop(); });

await consumer.start();
