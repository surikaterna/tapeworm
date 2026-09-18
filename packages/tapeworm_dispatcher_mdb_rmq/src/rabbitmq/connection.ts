import { connect, type ChannelModel, type ConfirmChannel } from "amqplib";
import type { RabbitConfig } from "../types";

export interface RabbitConnection {
  connection: ChannelModel;
  channel: ConfirmChannel;
  dispose(): Promise<void>;
}

/** Own only our listeners; leave amqplib's internal listeners untouched. */
export async function openRabbit(config: RabbitConfig, disconnected: () => void): Promise<RabbitConnection> {
  const quietError = () => {};
  const connection = await connect(config.uri, { heartbeat: 30, timeout: config.confirmTimeoutMs ?? 30000 });
  connection.on("error", quietError);
  connection.on("close", disconnected);
  let channel: ConfirmChannel | undefined;
  const dispose = async () => {
    // amqplib emits channel close while its connection is still transitioning.
    // Let that synchronous transition finish before asking it to close again.
    await Promise.resolve();
    // Closing a channel already in broker-initiated close can strand close-ok.
    // Close its owning connection instead; all channel attempts are already rejected.
    await connection.close().catch(quietError);
    connection.removeListener("error", quietError);
    connection.removeListener("close", disconnected);
    channel?.removeListener("error", quietError);
    channel?.removeListener("close", disconnected);
  };
  try {
    channel = await connection.createConfirmChannel();
    channel.on("error", quietError);
    channel.on("close", disconnected);
    await channel.assertExchange(config.exchange, "headers", { durable: true });
    return { connection, channel, dispose };
  } catch (error: unknown) { await dispose(); throw error; }
}
