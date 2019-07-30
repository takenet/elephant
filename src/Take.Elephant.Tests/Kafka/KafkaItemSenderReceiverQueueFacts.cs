using Confluent.Kafka;
using System;
using System.IO;
using System.Linq;
using Confluent.Kafka.Admin;
using Take.Elephant.Kafka;
using Xunit;
using Take.Elephant.Tests.Azure;

namespace Take.Elephant.Tests.Kafka
{
    [Trait("Category", nameof(Kafka))]
    public class KafkaItemSenderReceiverQueueFacts : ItemSenderReceiverQueueFacts
    {
        public override (ISenderQueue<Item>, IBlockingReceiverQueue<Item>) Create()
        {
            ClientConfig clientConfig;
            var topic = "items";

            var localKafka = false;

            // Local Kafka
            if (localKafka)
            {
                var bootstrapServers = "localhost:9092";

                clientConfig = new ClientConfig
                {
                    BootstrapServers = bootstrapServers
                };
            }
            //Azure Event Hub
            else
            {
                var fqdn = "hmg-msging.servicebus.windows.net:9093";
                var connectionString = "Endpoint=sb://hmg-msging.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=3AGYtV1+SMDSEKpiXOKTJFVP2h05Cvy+iaAf1uQwREQ=";
                var caCertPath = Path.Combine(Environment.CurrentDirectory, "Kafka", "cacert.pem");
                clientConfig = new ClientConfig
                {
                    BootstrapServers = fqdn,
                    SecurityProtocol = SecurityProtocol.SaslSsl,
                    SaslMechanism = SaslMechanism.Plain,
                    SaslUsername = "$ConnectionString",
                    SaslPassword = connectionString,
                    SslCaLocation = caCertPath,
                };
            }
            var consumerConfig = new ConsumerConfig(clientConfig)
            {
                GroupId = "default"
            };

            
            var adminClient = new AdminClientBuilder(clientConfig).Build();
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
            var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == topic);
            if (topicMetadata != null)
            {
                var consumer = new ConsumerBuilder<Ignore, Item>(consumerConfig)
                    .SetValueDeserializer(new JsonDeserializer<Item>())
                    .Build();

                foreach (var partition in topicMetadata.Partitions)
                {
                    var topicPartition = new TopicPartition(topic, new Partition(partition.PartitionId));
                    var offSet = consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(5));
                    consumer.Commit(new TopicPartitionOffset[] {new TopicPartitionOffset(topicPartition, offSet.High)});
                }
                consumer.Close();
            }
            
            var senderQueue = new KafkaSenderQueue<Item>(new ProducerConfig(clientConfig), topic, new JsonItemSerializer());
            var receiverQueue = new KafkaReceiverQueue<Item>(consumerConfig, topic, new JsonItemSerializer());
            receiverQueue.OpenAsync(CancellationToken).Wait();
            return (senderQueue, receiverQueue);
        }
    }
}