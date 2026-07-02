import { Kafka, logLevel } from "kafkajs";
import type { Consumer, KafkaMessage, Producer } from "kafkajs";
import type { NormalizedKafkaConfig } from "./config";
import { generateId } from "./keys";
import type { AfterUploadCallback, FileMetadata, Logger, UploadEvent } from "./types";

// entire kafka implementation is under abstraction of publish() and startWorker() methods.
export class KafkaService {
  private kafka: Kafka;
  private topic: string;
  private groupId: string;
  private deadLetterTopic?: string;
  private logger: Logger;
  private producer?: Producer;
  private consumer?: Consumer;
  private producerConnected = false;
  // keep a connect promise, for request deduplication
  private connecting?: Promise<void>;

  // the arg (kafka) is for testing purposes only with fake client 
  constructor(config: NormalizedKafkaConfig, logger: Logger, kafka?: Kafka) {
    this.topic = config.topic;
    this.groupId = config.groupId;
    this.deadLetterTopic = config.deadLetterTopic;
    this.logger = logger;

    if (kafka) {
      this.kafka = kafka;
    } else {
      this.kafka = new Kafka({
        clientId: config.clientId,
        brokers: config.brokers,
        ssl: config.ssl,
        sasl: config.sasl,
        logLevel: logLevel.NOTHING,
      });
    }
  }

  // connect to producer only if not connected
  async connectProducer(): Promise<void> {
    if (this.producerConnected) {
      return;
    }

    if (!this.connecting) {
      const producer = this.kafka.producer();
      this.connecting = producer
        .connect()
        .then(() => {
          this.producer = producer;
          this.producerConnected = true;
          this.logger.info("kafka producer connected");
        })
        .catch((err) => {
          // on connection failure, forget the promise for retry later
          this.connecting = undefined;
          throw err;
        });
    }

    await this.connecting;
  }

 // publish message to kafka queue
  async publish(file: FileMetadata): Promise<void> {
    await this.connectProducer();
    if (!this.producer) {
      throw new Error("Kafka producer is not connected");
    }

    const event: UploadEvent = {
      eventId: generateId(),
      occurredAt: new Date().toISOString(),
      file,
    };

    await this.producer.send({
      topic: this.topic,
      messages: [{ key: file.id, value: JSON.stringify(event) }],
    });
    this.logger.info("published upload event", { id: file.id, topic: this.topic });
  }

  // starts consumer -> reads messages -> runs the developer callback for each one
  async startWorker(handler: AfterUploadCallback): Promise<void> {
    if (this.consumer) {
      this.logger.warn("kafka worker already started");
      return;
    }

    const consumer = this.kafka.consumer({ groupId: this.groupId });
    await consumer.connect();
    await consumer.subscribe({ topic: this.topic, fromBeginning: false });
    this.consumer = consumer;

    await consumer.run({
      eachMessage: async ({ message }) => {
        // change value in bytes -> text
        const raw = message.value ? message.value.toString() : "";
        if (!raw) {
          return;
        }

        // turn the JSON text to an object.
        let parsed: any;
        try {
          parsed = JSON.parse(raw);
        } catch (err) {
          // for invalid JSON, retrying would case infinite failure loop, so push to DLT instead 
          this.logger.error("failed to parse kafka message", err);
          await this.sendToDeadLetter(message, err);
          return;
        }

        let file: FileMetadata;
        if (parsed && parsed.file) {
          file = parsed.file as FileMetadata;
        } else {
          file = parsed as FileMetadata;
        }

        try {
          await handler(file);
        } 
        catch (err) {
          this.logger.error("afterUpload callback failed", err);
          if (this.deadLetterTopic) {
            await this.sendToDeadLetter(message, err);
            return;
          }
          // rethrow for retry in next poll 
          throw err;
        }
      },
    });

    this.logger.info("kafka worker started", { topic: this.topic, groupId: this.groupId });
  }

  // can cause problems if DLT not configured i.e failures may be neither retried nor stored anywhere causing misinterpreted success
  private async sendToDeadLetter(message: KafkaMessage, err: unknown): Promise<void> {
    if (!this.deadLetterTopic) {
      return;
    }
    const reason = err instanceof Error ? err.message : String(err);
    try {
      await this.connectProducer();
      if (!this.producer) {
        throw new Error("Kafka producer is not connected");
      }
      await this.producer.send({
        topic: this.deadLetterTopic,
        messages: [
          {
            key: message.key,
            value: message.value,
            headers: { error: reason, originalTopic: this.topic },
          },
        ],
      });
      this.logger.warn("sent message to dead-letter topic", { topic: this.deadLetterTopic });
    } catch (sendErr) {
      this.logger.error("failed to send message to the dead-letter topic", sendErr);
      throw err;
    }
  }

  // connection closure
  async close(): Promise<void> {
    if (this.consumer) {
      await this.consumer.disconnect();
      this.consumer = undefined;
    }
    if (this.producer) {
      await this.producer.disconnect();
      this.producer = undefined;
      this.producerConnected = false;
    }
    this.connecting = undefined;
  }
}
