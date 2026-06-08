import { createEmailConsumer } from "../consumers/emailConsumer.js";

// Reads from the FIFO queue — ordered per campaignId, with DynamoDB-backed
// idempotency preventing duplicate email sends on re-delivery.
const consumer = createEmailConsumer(
  "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/campaign-processor.fifo",
);

process.on("SIGINT", () => { console.log("\nShutting down…"); consumer.stop(); });

await consumer.start();
