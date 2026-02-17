export const createConsumerImpl = (kafka, opts) => kafka.consumer(opts);

export const connectConsumerImpl = (consumer) => consumer.connect();

export const subscribeImpl = (consumer, opts) => consumer.subscribe(opts);

// Convert KafkaJS Buffer values to strings
const bufToStr = (val) => {
  if (val == null) return val;
  if (Buffer.isBuffer(val)) return val.toString("utf-8");
  if (typeof val === "string") return val;
  return String(val);
};

// Transform user options to match KafkaJS expectations
export const runImpl = (consumer, opts, transformKey, promiseToAff, affToPromise) => {
  const wrappedOpts = { ...opts };

  if (opts.eachBatch) {
    const userHandler = opts.eachBatch;

    wrappedOpts.eachBatch = async (payload) => {
      for (const msg of payload.batch.messages) {
        msg.key = transformKey(bufToStr(msg.key));
        msg.value = bufToStr(msg.value);
      }

      payload.heartbeat = promiseToAff(payload.heartbeat);

      const flatPayload = {
        topic: payload.batch.topic,
        partition: payload.batch.partition,
        highWatermark: payload.batch.highWatermark,
        messages: payload.batch.messages,
        resolveOffset: (offset) => payload.resolveOffset(offset),
        heartbeat: payload.heartbeat,
        commitOffsetsIfNecessary: () => payload.commitOffsetsIfNecessary(),
        uncommittedOffsets: () => payload.uncommittedOffsets(),
        isRunning: () => payload.isRunning(),
        isStale: () => payload.isStale(),
        pause: () => payload.pause()
      };

      const affResult = userHandler(flatPayload);
      return affToPromise(affResult)();
    };
  }

  if (opts.eachMessage) {
    const userHandler = opts.eachMessage;

    wrappedOpts.eachMessage = async (payload) => {
      payload.message.value = bufToStr(payload.message.value);
      payload.message.key = bufToStr(payload.message.key);

      const affResult = userHandler(payload);
      return affToPromise(affResult)();
    };
  }

  return consumer.run(wrappedOpts);
};

export const disconnectConsumerImpl = (consumer) => consumer.disconnect();
