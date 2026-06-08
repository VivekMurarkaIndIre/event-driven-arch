import { createSurveyConsumer } from "../consumers/surveyConsumer.js";

// Reads from campaign-survey, populated by the EventBridge rule that matches
// events where detail.campaignType === "survey" on the campaign-bus.
const consumer = createSurveyConsumer(
  "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/campaign-survey",
);

process.on("SIGINT", () => { console.log("\nShutting down…"); consumer.stop(); });

await consumer.start();
