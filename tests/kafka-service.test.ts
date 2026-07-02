import { describe, it, expect, vi } from "vitest";
import type { Kafka } from "kafkajs";
import { KafkaService } from "../src/kafka-service";
import type { NormalizedKafkaConfig } from "../src/config";
import type { FileMetadata, Logger } from "../src/types";

const logger: Logger = { info: () => {}, warn: () => {}, error: () => {} };

const config: NormalizedKafkaConfig = {
  brokers: ["localhost:9092"],
  topic: "files",
  clientId: "lb-upload-infra",
  groupId: "lb-upload-infra-workers",
};

const dlqConfig: NormalizedKafkaConfig = { ...config, deadLetterTopic: "files-dlq" };

const file: FileMetadata = {
  id: "id1",
  key: "uploads/id1/a.png",
  bucket: "test-bucket",
  originalName: "a.png",
  mimeType: "image/png",
  size: 10,
  etag: "e",
  uploadedAt: new Date().toISOString(),
};

function makeFakeKafka() {
  const producer = {
    connect: vi.fn(async () => {}),
    send: vi.fn(async () => {}),
    disconnect: vi.fn(async () => {}),
  };
  let handler: ((p: { message: { key: Buffer | null; value: Buffer | null } }) => Promise<void>) | undefined;
  const consumer = {
    connect: vi.fn(async () => {}),
    subscribe: vi.fn(async () => {}),
    run: vi.fn(async (opts: { eachMessage: (p: any) => Promise<void> }) => {
      handler = opts.eachMessage;
    }),
    disconnect: vi.fn(async () => {}),
  };
  const kafka = { producer: () => producer, consumer: () => consumer };
  return {
    kafka: kafka as unknown as Kafka,
    producer,
    consumer,
    deliver: async (value: string | null) => {
      if (!handler) throw new Error("worker not started");
      await handler({ message: { key: null, value: value === null ? null : Buffer.from(value) } });
    },
  };
}

describe("KafkaService", () => {
  it("connects the producer once and publishes a wrapped event", async () => {
    const { kafka, producer } = makeFakeKafka();
    const svc = new KafkaService(config, logger, kafka);
    await svc.publish(file);
    await svc.publish(file);
    expect(producer.connect).toHaveBeenCalledTimes(1);
    expect(producer.send).toHaveBeenCalledTimes(2);

    // The message value is an envelope: { eventId, occurredAt, file }.
    const lastCall = (producer.send as any).mock.calls[1][0];
    expect(lastCall.topic).toBe("files");
    expect(lastCall.messages[0].key).toBe("id1");
    const payload = JSON.parse(lastCall.messages[0].value);
    expect(payload.file).toEqual(file);
    expect(typeof payload.eventId).toBe("string");
    expect(typeof payload.occurredAt).toBe("string");
  });

  it("runs the handler for each consumed message", async () => {
    const { kafka, consumer, deliver } = makeFakeKafka();
    const svc = new KafkaService(config, logger, kafka);
    const handler = vi.fn(async () => {});
    await svc.startWorker(handler);
    expect(consumer.subscribe).toHaveBeenCalledWith({ topic: "files", fromBeginning: false });
    await deliver(JSON.stringify({ eventId: "e1", occurredAt: "t1", file }));
    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith(expect.objectContaining({ id: "id1" }));
  });

  it("rethrows handler errors when there is no dead-letter topic, so Kafka can retry", async () => {
    const { kafka, producer, deliver } = makeFakeKafka();
    const svc = new KafkaService(config, logger, kafka);
    const handler = vi.fn(async () => {
      throw new Error("boom");
    });
    await svc.startWorker(handler);

    await expect(
      deliver(JSON.stringify({ eventId: "e1", occurredAt: "t1", file }))
    ).rejects.toThrow("boom");
    expect(producer.send).not.toHaveBeenCalled();
  });

  it("sends failed messages to the dead-letter topic when one is configured", async () => {
    const { kafka, producer, deliver } = makeFakeKafka();
    const svc = new KafkaService(dlqConfig, logger, kafka);
    const handler = vi.fn(async () => {
      throw new Error("boom");
    });
    await svc.startWorker(handler);

    await expect(deliver(JSON.stringify({ eventId: "e1", occurredAt: "t1", file }))).resolves.toBeUndefined();
    const dlqCall = (producer.send as any).mock.calls.at(-1)[0];
    expect(dlqCall.topic).toBe("files-dlq");
    expect(typeof dlqCall.messages[0].headers.error).toBe("string");
  });

  it("disconnects producer and consumer on close", async () => {
    const { kafka, producer, consumer } = makeFakeKafka();
    const svc = new KafkaService(config, logger, kafka);
    await svc.publish(file);
    await svc.startWorker(vi.fn(async () => {}));
    await svc.close();
    expect(producer.disconnect).toHaveBeenCalledTimes(1);
    expect(consumer.disconnect).toHaveBeenCalledTimes(1);
  });
});
