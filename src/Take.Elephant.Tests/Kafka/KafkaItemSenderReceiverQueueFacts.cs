using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using Take.Elephant.Kafka;
using Take.Elephant.Tests.Azure;
using Xunit;

namespace Take.Elephant.Tests.Kafka
{
    [Trait("Category", nameof(Kafka))]
    public class KafkaItemSenderReceiverQueueFacts : ItemSenderReceiverQueueFacts
    {
        private const string TOPIC_PREFIX = "items";
        private static readonly ClientConfig _clientConfig = GetKafkaConfig();
        private readonly List<string> _createdTopics = [];
        private KafkaSenderQueue<Item> _senderQueue;
        private KafkaReceiverQueue<Item> _receiverQueue;

        private static ClientConfig GetKafkaConfig()
        {
            const string bootstrapServers = "localhost:9092";

            var clientConfig = new ClientConfig
            {
                BootstrapServers = bootstrapServers
            };
            
            return clientConfig;
        }

        public override (ISenderQueue<Item>, IBlockingReceiverQueue<Item>) Create()
        {
            var topic = $"{TOPIC_PREFIX}-{Guid.NewGuid():N}";
            EnsureTopicExists(topic);
            _createdTopics.Add(topic);

            var consumerConfig = new ConsumerConfig(_clientConfig)
            {
                GroupId = $"default-{Guid.NewGuid():N}",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            
            var senderQueue = new KafkaSenderQueue<Item>(
                new ProducerConfig(_clientConfig),
                topic,
                new JsonItemSerializer()
            );
            var receiverQueue = new KafkaReceiverQueue<Item>(
                consumerConfig,
                topic,
                new JsonItemSerializer()
            );
            receiverQueue.OpenAsync(CancellationToken).Wait();

            _senderQueue = senderQueue;
            _receiverQueue = receiverQueue;

            return (senderQueue, receiverQueue);
        }

        protected override void Dispose(bool disposing)
        {
            if (!disposing)
             {
                 base.Dispose(disposing);
                 return;
             }
             
            _senderQueue?.Dispose();
            _receiverQueue?.Dispose();
            DeleteCreatedTopics();
            base.Dispose(disposing);
        }

        private static void EnsureTopicExists(string topic)
        {
            using var adminClient = new AdminClientBuilder(_clientConfig).Build();

            try
            {
                adminClient.CreateTopicsAsync(
                    [
                        new TopicSpecification
                        {
                            Name = topic,
                            NumPartitions = 1,
                            ReplicationFactor = 1,
                        }
                    ]
                ).GetAwaiter().GetResult();
            }
            catch (CreateTopicsException ex)
            {
                if (
                    ex.Results is not { Count: 1 }
                    || ex.Results[0].Error.Code != ErrorCode.TopicAlreadyExists
                )
                {
                    throw;
                }
            }
        }

        private void DeleteCreatedTopics()
        {
            if (_createdTopics.Count == 0)
            {
                return;
            }

            using var adminClient = new AdminClientBuilder(_clientConfig).Build();

            foreach (var topic in _createdTopics)
            {
                try
                {
                    adminClient.DeleteTopicsAsync([topic]).GetAwaiter().GetResult();
                }
                catch (DeleteTopicsException ex)
                {
                    if (!ShouldIgnore(ex))
                    {
                        throw;
                    }
                }
            }
        }

        private static bool ShouldIgnore(DeleteTopicsException ex)
        {
            if (ex.Results == null)
            {
                return true;
            }

            foreach (var result in ex.Results)
            {
                if (result == null)
                {
                    continue;
                }

                if (
                    result.Error.Code != ErrorCode.UnknownTopicOrPart
                    && result.Error.Code != ErrorCode.TopicDeletionDisabled
                )
                {
                    return false;
                }
            }

            return true;
        }
    }
}
