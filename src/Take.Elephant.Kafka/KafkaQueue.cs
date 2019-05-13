using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Dawn;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Kafka
{
    public class KafkaQueue<T> : IBlockingQueue<T> where T : class
    {
        private readonly IProducer<Null, string> _producer;
        private readonly IConsumer<Ignore, string> _consumer;
        private readonly ISerializer<T> _serializer;
        private readonly IAdminClient _adminClient;
        private readonly TimeSpan _dequeueTimeout;
        private readonly SemaphoreSlim _queueCreationSemaphore;
        private readonly SemaphoreSlim _consumerSubscriptionSemaphore;
        private readonly string _queueName;
        private bool _queueExists;
        private readonly int _numberOfPartitions;
        private readonly short _replicationFactor;
        private readonly TimeSpan _commandsTimeout;

        public KafkaQueue(
            string queueName,
            ISerializer<T> serializer,
            AdminClientConfig adminClientConfig,
            ProducerConfig producerConfig,
            ConsumerConfig consumerConfig,
            TimeSpan dequeueTimeout = default,
            TimeSpan commandsTimeout = default)
        {
            Guard.Argument(queueName).NotNull().NotEmpty();
            Guard.Argument(serializer).NotNull();
            Guard.Argument(adminClientConfig).NotNull();
            Guard.Argument(producerConfig).NotNull();
            Guard.Argument(consumerConfig).NotNull();

            if (dequeueTimeout == default)
            {
                dequeueTimeout = TimeSpan.FromSeconds(5);
            }
            if (commandsTimeout == default)
            {
                commandsTimeout = TimeSpan.FromSeconds(5);
            }
            _queueCreationSemaphore = new SemaphoreSlim(1, 1);
            _consumerSubscriptionSemaphore = new SemaphoreSlim(1, 1);
            _queueName = queueName;
            _serializer = serializer;
            _dequeueTimeout = dequeueTimeout;
            _commandsTimeout = commandsTimeout;
            _adminClient = new AdminClientBuilder(adminClientConfig)
                .Build();
            _producer = new ProducerBuilder<Null, string>(producerConfig)
                .Build();
            _consumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
                .Build();
        }

        public async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            await CreateTopicIfNotExistsAsync(cancellationToken);
            await SubscribeIfNotAsync(cancellationToken);
            var consumeResult = await Task.Run(() => _consumer.Consume(cancellationToken));
            return _serializer.Deserialize(consumeResult.Value);
        }

        public async Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            await CreateTopicIfNotExistsAsync(cancellationToken);
            await SubscribeIfNotAsync(cancellationToken);

            using (var cts = new CancellationTokenSource(_dequeueTimeout))
            using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken))
            {
                try
                {
                    var consumeResult = await Task.Run(() => _consumer.Consume(linkedCts.Token));
                    return _serializer.Deserialize(consumeResult.Value);
                }
                catch (OperationCanceledException) when (cts.IsCancellationRequested)
                {
                    return null;
                }
            }
        }

        public async Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            await CreateTopicIfNotExistsAsync(cancellationToken);
            await SubscribeIfNotAsync(cancellationToken);

            await _producer.ProduceAsync(_queueName, new Message<Null, string>
            {
                Value = _serializer.Serialize(item)
            });
        }

        public async Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            await CreateTopicIfNotExistsAsync(cancellationToken);
            var metadata = _adminClient.GetMetadata(_commandsTimeout);
            var topicMetada = metadata.Topics.First(t => t.Topic == _queueName);
            long length = 0;
            foreach (var partition in topicMetada.Partitions)
            {
                var topicPartition = new TopicPartition(_queueName, new Partition(partition.PartitionId));
                var position = _consumer.Position(topicPartition);
                var offSet = _consumer.QueryWatermarkOffsets(topicPartition, _commandsTimeout);
                length += offSet.High - position;
            }

            return length;
        }

        private async Task SubscribeIfNotAsync(CancellationToken cancellationToken)
        {
            if (_consumer.Subscription.Any(s => s == _queueName))
            {
                return;
            }

            await _consumerSubscriptionSemaphore.WaitAsync(cancellationToken);
            try
            {
                if (_consumer.Subscription.Any(s => s == _queueName))
                {
                    return;
                }

                _consumer.Subscribe(_queueName);
            }
            finally
            {
                _consumerSubscriptionSemaphore.Release();
            }
        }

        private async Task CreateTopicIfNotExistsAsync(CancellationToken cancellationToken)
        {
            if (_queueExists)
            {
                return;
            }

            await _queueCreationSemaphore.WaitAsync(cancellationToken);
            try
            {
                if (_queueExists)
                {
                    return;
                }
                var kafkaMetadata = _adminClient.GetMetadata(TimeSpan.FromSeconds(10));
                if (kafkaMetadata.Topics.All(t => t.Topic != _queueName))
                {
                    var topics = new TopicSpecification[]
                    {
                        new TopicSpecification
                        {
                            Name = _queueName,
                            NumPartitions = _numberOfPartitions,
                            ReplicationFactor = _replicationFactor
                        }
                    };
                    await _adminClient.CreateTopicsAsync(topics);
                }

                _queueExists = true;
            }
            finally
            {
                _queueCreationSemaphore.Release();
            }
        }
    }
}