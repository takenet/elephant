using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Reflection;
using Take.Elephant.Kafka;
using Take.Elephant.Tests.Redis;
using Xunit;

namespace Take.Elephant.Tests.Kafka
{
    [Trait("Category", nameof(Kafka))]
    [Collection(nameof(Kafka))]
    public class KafkaStorageBlockingQueueFacts : ItemBlockingQueueFacts
    {
        public override IQueue<Item> Create()
        {
            var topicName = "items";
            var queueGroup = "default";

            var fqdn = "";
            var connectionString = "";
            var caCertPath = Path.Combine(Environment.CurrentDirectory, "Kafka", "cacert.pem");
            var adminConfig = new AdminClientConfig()
            {
                BootstrapServers = fqdn,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = connectionString,
                SslCaLocation = caCertPath
            };
            var producerConfig = new ProducerConfig()
            {
                BootstrapServers = fqdn,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = connectionString,
                SslCaLocation = caCertPath
            };
            var consumerConfig = new ConsumerConfig()
            {
                BootstrapServers = fqdn,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = connectionString,
                SslCaLocation = caCertPath,
                GroupId = queueGroup
            };
            var queue = new KafkaQueue<Item>(topicName, new ItemSerializer(), adminConfig, producerConfig, consumerConfig);
            object value = new Item();
            while (value != null)
            {
                var promise = queue.DequeueOrDefaultAsync();
                promise.Wait();
                value = promise.Result;
            }
            return queue;
        }
    }

    public class JsonItemSerializer : ISerializer<Item>
    {
        public Item Deserialize(string value)
        {
            return JsonConvert.DeserializeObject<Item>(value);
        }

        public string Serialize(Item value)
        {
            return JsonConvert.SerializeObject(value);
        }
    }
}