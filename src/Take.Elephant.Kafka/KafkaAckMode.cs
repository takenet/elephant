namespace Take.Elephant.Kafka
{
    /// <summary>
    /// Controls when the Kafka consumer offset is committed.
    /// </summary>
    public enum KafkaAckMode
    {
        /// <summary>
        /// Legacy default. Auto-commit is managed by the Confluent client
        /// (<c>enable.auto.commit=true</c>). Offset is committed periodically
        /// regardless of processing outcome. If the offset is auto-committed
        /// before processing completes, a crash before completion may cause
        /// message loss (message committed but not processed).
        /// </summary>
        Eager = 0,

        /// <summary>
        /// Offset is committed to the broker only after the caller explicitly invokes
        /// <see cref="KafkaAckableMessage{T}.AcknowledgeAsync"/> following successful
        /// processing. If the process restarts before ack, the message will be
        /// redelivered (at-least-once delivery guarantee).
        /// </summary>
        OnSuccess = 1,

        /// <summary>
        /// Same as <see cref="OnSuccess"/> but the caller has full, explicit control
        /// over when to acknowledge. Useful when acknowledgement depends on downstream
        /// conditions (e.g., waiting for a secondary confirmation).
        /// </summary>
        Manual = 2,
    }
}
