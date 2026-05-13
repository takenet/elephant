using Confluent.Kafka;

namespace Take.Elephant.Kafka
{
    /// <summary>
    /// Provides optimized default configurations for Kafka clients targeting high-throughput scenarios.
    /// These values are tuned for Azure Event Hubs (Standard tier) and Confluent Cloud.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Producer Optimization Strategy:</b>
    /// <list type="bullet">
    ///   <item>Aggressive batching with LingerMs=100ms allows dense message batches</item>
    ///   <item>Zstd compression provides best compression ratio for JSON payloads</item>
    ///   <item>BatchSize of 900KB respects Event Hubs Standard 1MB limit</item>
    ///   <item>Idempotent producer prevents duplicates on retries</item>
    /// </list>
    /// </para>
    /// <para>
    /// <b>Consumer Optimization Strategy:</b>
    /// <list type="bullet">
    ///   <item>Large fetch buffers reduce network round trips</item>
    ///   <item>Extended session timeout prevents spurious rebalances</item>
    ///   <item>Auto-commit with 5s interval balances throughput and durability</item>
    /// </list>
    /// </para>
    /// </remarks>
    public static class KafkaClientDefaults
    {
        #region Producer Defaults

        /// <summary>
        /// Compression algorithm. Zstd offers the best compression ratio for typical payloads.
        /// </summary>
        public const CompressionType DefaultCompressionType = CompressionType.Zstd;

        /// <summary>
        /// Time to wait for messages to accumulate before sending a batch.
        /// 100ms allows dense batches while keeping latency acceptable.
        /// </summary>
        public const int DefaultLingerMs = 100;

        /// <summary>
        /// Maximum batch size in bytes. 900KB is safe for Event Hubs Standard (1MB limit).
        /// </summary>
        public const int DefaultBatchSize = 900 * 1024; // 900KB

        /// <summary>
        /// Maximum number of messages in a batch. Higher value for small messages.
        /// </summary>
        public const int DefaultBatchNumMessages = 5000;

        /// <summary>
        /// Local buffer memory for the producer. 128MB provides headroom for network fluctuations.
        /// </summary>
        public const int DefaultBufferMemoryBytes = 128 * 1024 * 1024; // 128MB

        /// <summary>
        /// Acknowledgment mode. Acks.All is required when EnableIdempotence=true.
        /// Ensures all in-sync replicas acknowledge the message, providing strongest durability.
        /// </summary>
        public const Acks DefaultAcks = Acks.All;

        /// <summary>
        /// Maximum delivery retries before giving up.
        /// </summary>
        public const int DefaultMessageSendMaxRetries = 20;

        /// <summary>
        /// Initial backoff between retries.
        /// </summary>
        public const int DefaultRetryBackoffMs = 1000;

        /// <summary>
        /// Maximum time a message can wait in the queue before timeout.
        /// 60 seconds provides resilience for transient failures.
        /// </summary>
        public const int DefaultMessageTimeoutMs = 60000;

        /// <summary>
        /// Maximum in-flight requests. 5 is the safe maximum for idempotent producers.
        /// </summary>
        public const int DefaultMaxInFlight = 5;

        /// <summary>
        /// Network request timeout.
        /// </summary>
        public const int DefaultRequestTimeoutMs = 30000;

        /// <summary>
        /// Enable idempotent producer to prevent duplicates on retries.
        /// </summary>
        public const bool DefaultEnableIdempotence = true;

        #endregion

        #region Consumer Defaults

        /// <summary>
        /// Minimum bytes to fetch per request. 1MB reduces network calls.
        /// </summary>
        public const int DefaultFetchMinBytes = 1024 * 1024; // 1MB

        /// <summary>
        /// Maximum wait time if FetchMinBytes not reached.
        /// </summary>
        public const int DefaultFetchWaitMaxMs = 100;

        /// <summary>
        /// Maximum bytes to fetch per partition per request.
        /// </summary>
        public const int DefaultFetchMaxBytes = 50 * 1024 * 1024; // 50MB

        /// <summary>
        /// Minimum messages to keep in the local queue.
        /// </summary>
        public const int DefaultQueuedMinMessages = 10000;

        /// <summary>
        /// Maximum KB to keep in the local queue. 128MB provides large buffer.
        /// </summary>
        public const int DefaultQueuedMaxMessagesKbytes = 128000; // 128MB

        /// <summary>
        /// Enable auto-commit for simplified offset management.
        /// </summary>
        public const bool DefaultEnableAutoCommit = true;

        /// <summary>
        /// Auto-commit interval. 5 seconds balances throughput and durability.
        /// </summary>
        public const int DefaultAutoCommitIntervalMs = 5000;

        /// <summary>
        /// Session timeout before consumer is considered dead.
        /// Higher value prevents spurious rebalances during GC pauses.
        /// </summary>
        public const int DefaultSessionTimeoutMs = 45000;

        /// <summary>
        /// Heartbeat interval. Should be ~1/3 of SessionTimeout.
        /// </summary>
        public const int DefaultHeartbeatIntervalMs = 15000;

        #endregion

        #region Connection Defaults

        /// <summary>
        /// Enable TCP keepalive to prevent Azure gateway from closing idle connections.
        /// </summary>
        public const bool DefaultSocketKeepaliveEnable = true;

        /// <summary>
        /// Maximum idle time before closing connection. 3 minutes is safe for Azure.
        /// </summary>
        public const int DefaultConnectionsMaxIdleMs = 180000; // 3 minutes

        /// <summary>
        /// Initial backoff for reconnection attempts.
        /// </summary>
        public const int DefaultReconnectBackoffMs = 1000;

        /// <summary>
        /// Maximum backoff for reconnection attempts.
        /// </summary>
        public const int DefaultReconnectBackoffMaxMs = 10000;

        /// <summary>
        /// Metadata refresh interval. 5 minutes reduces control plane traffic.
        /// </summary>
        public const int DefaultMetadataRefreshIntervalMs = 300000; // 5 minutes

        #endregion

        /// <summary>
        /// Applies optimized default values to a <see cref="ProducerConfig"/>.
        /// Only sets values that are not already configured (null or default).
        /// </summary>
        /// <param name="config">The producer configuration to enhance.</param>
        /// <returns>The enhanced configuration (same instance).</returns>
        public static ProducerConfig ApplyOptimizedDefaults(this ProducerConfig config)
        {
            config.CompressionType ??= DefaultCompressionType;
            config.LingerMs ??= DefaultLingerMs;
            config.BatchSize ??= DefaultBatchSize;
            config.BatchNumMessages ??= DefaultBatchNumMessages;
            config.MessageSendMaxRetries ??= DefaultMessageSendMaxRetries;
            config.RetryBackoffMs ??= DefaultRetryBackoffMs;
            config.MessageTimeoutMs ??= DefaultMessageTimeoutMs;
            config.MaxInFlight ??= DefaultMaxInFlight;
            config.RequestTimeoutMs ??= DefaultRequestTimeoutMs;
            config.EnableIdempotence ??= DefaultEnableIdempotence;
            config.Acks ??= DefaultAcks;

            // Connection stability
            config.SocketKeepaliveEnable ??= DefaultSocketKeepaliveEnable;
            config.ConnectionsMaxIdleMs ??= DefaultConnectionsMaxIdleMs;
            config.ReconnectBackoffMs ??= DefaultReconnectBackoffMs;
            config.ReconnectBackoffMaxMs ??= DefaultReconnectBackoffMaxMs;
            config.MetadataMaxAgeMs ??= DefaultMetadataRefreshIntervalMs;

            return config;
        }

        /// <summary>
        /// Applies optimized default values to a <see cref="ConsumerConfig"/>.
        /// Only sets values that are not already configured (null or default).
        /// </summary>
        /// <param name="config">The consumer configuration to enhance.</param>
        /// <returns>The enhanced configuration (same instance).</returns>
        public static ConsumerConfig ApplyOptimizedDefaults(this ConsumerConfig config)
        {
            config.FetchMinBytes ??= DefaultFetchMinBytes;
            config.FetchWaitMaxMs ??= DefaultFetchWaitMaxMs;
            config.FetchMaxBytes ??= DefaultFetchMaxBytes;
            config.QueuedMinMessages ??= DefaultQueuedMinMessages;
            config.QueuedMaxMessagesKbytes ??= DefaultQueuedMaxMessagesKbytes;
            config.EnableAutoCommit ??= DefaultEnableAutoCommit;
            config.AutoCommitIntervalMs ??= DefaultAutoCommitIntervalMs;
            config.SessionTimeoutMs ??= DefaultSessionTimeoutMs;
            config.HeartbeatIntervalMs ??= DefaultHeartbeatIntervalMs;

            // Connection stability
            config.SocketKeepaliveEnable ??= DefaultSocketKeepaliveEnable;
            config.ConnectionsMaxIdleMs ??= DefaultConnectionsMaxIdleMs;
            config.ReconnectBackoffMs ??= DefaultReconnectBackoffMs;
            config.ReconnectBackoffMaxMs ??= DefaultReconnectBackoffMaxMs;
            config.MetadataMaxAgeMs ??= DefaultMetadataRefreshIntervalMs;

            return config;
        }

        /// <summary>
        /// Creates a new <see cref="ProducerConfig"/> with optimized defaults pre-applied.
        /// </summary>
        /// <param name="bootstrapServers">The Kafka bootstrap servers.</param>
        /// <param name="clientId">Optional client identifier for service identification in Confluent Cloud dashboard.</param>
        /// <returns>A new optimized producer configuration.</returns>
        public static ProducerConfig CreateOptimizedProducerConfig(string bootstrapServers = null, string clientId = null)
        {
            var config = new ProducerConfig();
            if (!string.IsNullOrWhiteSpace(bootstrapServers))
            {
                config.BootstrapServers = bootstrapServers;
            }
            if (!string.IsNullOrWhiteSpace(clientId))
            {
                config.ClientId = clientId;
            }
            return config.ApplyOptimizedDefaults();
        }

        /// <summary>
        /// Creates a new <see cref="ConsumerConfig"/> with optimized defaults pre-applied.
        /// </summary>
        /// <param name="bootstrapServers">The Kafka bootstrap servers.</param>
        /// <param name="groupId">The consumer group ID.</param>
        /// <param name="clientId">Optional client identifier for service identification in Confluent Cloud dashboard.</param>
        /// <returns>A new optimized consumer configuration.</returns>
        public static ConsumerConfig CreateOptimizedConsumerConfig(string bootstrapServers = null, string groupId = null, string clientId = null)
        {
            var config = new ConsumerConfig();
            if (!string.IsNullOrWhiteSpace(bootstrapServers))
            {
                config.BootstrapServers = bootstrapServers;
            }
            if (!string.IsNullOrWhiteSpace(groupId))
            {
                config.GroupId = groupId;
            }
            if (!string.IsNullOrWhiteSpace(clientId))
            {
                config.ClientId = clientId;
            }
            return config.ApplyOptimizedDefaults();
        }

        /// <summary>
        /// Sets the ClientId on a <see cref="ClientConfig"/> for service identification.
        /// The ClientId is shown in Confluent Cloud dashboard and broker logs.
        /// </summary>
        /// <typeparam name="T">The config type (ProducerConfig, ConsumerConfig, etc.)</typeparam>
        /// <param name="config">The configuration to modify.</param>
        /// <param name="clientId">The client identifier (typically the service/application name).</param>
        /// <returns>The same configuration instance for method chaining.</returns>
        public static T WithClientId<T>(this T config, string clientId) where T : ClientConfig
        {
            if (!string.IsNullOrWhiteSpace(clientId))
            {
                config.ClientId = clientId;
            }
            return config;
        }
    }
}
