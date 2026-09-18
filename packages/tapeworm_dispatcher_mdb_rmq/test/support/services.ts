import { MongoClient } from "mongodb";
import { connect } from "amqplib";
import { randomUUID } from "node:crypto";

export const mongoUri = process.env.TEST_MONGODB_URI ?? "mongodb://127.0.0.1:27187/?directConnection=true&replicaSet=rs0";
export const rabbitUri = process.env.TEST_RABBITMQ_URI ?? "amqp://127.0.0.1:56787";
export const unique = () => `gqxm_${randomUUID().replaceAll("-", "")}`;

export async function mongo(uri = mongoUri) {
  const client = new MongoClient(uri, { serverSelectionTimeoutMS: 5000 });
  await client.connect();
  const db = client.db(unique());
  await db.createCollection("commits");
  await db.collection("commits").createIndex({ token: 1 });
  return { client, db, config: { db, collection: "commits", batchSize: 2, retryDelayMs: 10, maxRetries: 4 },
    close: async () => { await db.dropDatabase(); await client.close(); } };
}

export async function rabbit() {
  const connection = await connect(rabbitUri, { timeout: 5000 });
  const channel = await connection.createChannel();
  const exchange = unique();
  const queue = unique();
  await channel.assertExchange(exchange, "headers", { durable: true });
  await channel.assertQueue(queue, { durable: true });
  await channel.bindQueue(queue, exchange, "", { "x-match": "all", collection: "commits" });
  return { connection, channel, exchange, queue, close: async () => {
    await channel.deleteQueue(queue); await channel.deleteExchange(exchange); await connection.close();
  } };
}

export async function eventually(check: () => Promise<boolean>, timeout = 10000): Promise<void> {
  const end = Date.now() + timeout;
  while (!(await check())) {
    if (Date.now() > end) throw new Error("Condition timed out");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}
