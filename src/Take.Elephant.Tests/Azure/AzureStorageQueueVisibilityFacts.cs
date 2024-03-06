using System;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using Azure.Storage.Queues;
using Newtonsoft.Json;
using Take.Elephant.Azure;
using Xunit;

namespace Take.Elephant.Tests.Azure
{
    [Trait("Category", nameof(Azure))]
    [Collection(nameof(Azure))]
    public class AzureStorageQueueVisibilityFacts : FactsBase
    {
        //This connectionString points a local Azure Storage. You can run one using https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio#running-azurite-from-the-command-line 
        protected string _connectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";
        protected string _queueName = "items";
        readonly ISerializer<Item> _serializer;

        public AzureStorageQueueVisibilityFacts()
        {
            _serializer = new JsonItemSerializer();
        }
        public IBlockingQueue<Item> Create(TimeSpan visibilityTimeout)
        {
            DeleteQueueAsync(_connectionString, _queueName).Wait();

            return new AzureStorageQueue<Item>(
                _connectionString,
                _queueName,
                new JsonItemSerializer(),
                visibilityTimeout: visibilityTimeout);
        }

        public QueueClient CreateQueueClient()
        {
            var options = new QueueClientOptions();
            options.MessageEncoding = QueueMessageEncoding.Base64;
            return new QueueClient(_connectionString, _queueName, options);
        }

        protected Item CreateItem()
        {
            return Fixture.Create<Item>();
        }

        [Fact(DisplayName = "DeleteInvisibleItem")]
        public async Task DeleteInvisibleItemShouldFail()
        {
            //Arrange
            var visibilityTimeout = TimeSpan.FromMinutes(1);
            var queue = Create(visibilityTimeout);
            var azureClient = CreateQueueClient();
            
            var item = CreateItem();
            await queue.EnqueueAsync(item);

            //Act
            var actual = await azureClient.ReceiveMessageAsync(visibilityTimeout);
            var secondDequeue = await queue.DequeueOrDefaultAsync();

            //Assert
            var firstDequeue = _serializer.Deserialize(actual.Value.MessageText);
            AssertIsTrue(firstDequeue.Equals(item));
            AssertIsDefault(secondDequeue);
            AssertEquals(await queue.GetLengthAsync(), 1);

        }

        [Fact(DisplayName = "DeleteVisibleItem")]
        public async Task DeleteVisibleItemShouldSucceed()
        {
            //Arrange
            int visibilitySeconds = 20;
            var visibilityTimeout = TimeSpan.FromSeconds(visibilitySeconds);
            var queue = Create(visibilityTimeout);
            var item = CreateItem();
            var azureClient = CreateQueueClient();

            await queue.EnqueueAsync(item);

            //Act
            var actual = await azureClient.ReceiveMessageAsync(visibilityTimeout);
            // added extra time to ensure that we waited more than visibilitySeconds
            Thread.Sleep(visibilitySeconds * 1000 + 2000);
            var secondDequeue = await queue.DequeueOrDefaultAsync();

            //Assert
            var firstDequeue = _serializer.Deserialize(actual.Value.MessageText);
            AssertIsTrue(firstDequeue.Equals(item));
            AssertIsTrue(secondDequeue.Equals(item));
            AssertEquals(await queue.GetLengthAsync(), 0);
        }

        private async Task DeleteQueueAsync(string connectionString, string queueName)
        {
            var options = new QueueClientOptions();
            options.MessageEncoding = QueueMessageEncoding.Base64;
            var queue = new QueueClient(connectionString, queueName, options);

            if (await queue.ExistsAsync())
            {
                await queue.ClearMessagesAsync();
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
}
