namespace Take.Elephant.Kafka
{
    /// <summary>
    /// Controls when the Kafka consumer offset is committed.
    /// </summary>
    public enum KafkaAckMode
    {
        /// <summary>
        /// Legacy default. Offset committed by Confluent auto-commit before processing completes.
        /// A pod crash after commit but before processing finishes causes silent message loss.
        /// </summary>
        Eager = 0,

        /// <summary>
        /// Offset committed only after <see cref="KafkaAckableMessage{T}.AcknowledgeAsync"/> is called.
        /// At-least-once delivery: message is redelivered if the process restarts before ack.
        /// </summary>
        OnSuccess = 1,

        /// <summary>
        /// Same semantics as <see cref="OnSuccess"/>; the name signals that acknowledgement
        /// timing is fully controlled by the application.
        /// </summary>
        Manual = 2,
    }
}
