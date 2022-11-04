using System.Threading.Tasks;
using Azure.Storage.Queues;
using Newtonsoft.Json;
using Take.Elephant.Azure;
using Xunit;

namespace Take.Elephant.Tests.Azure
{
    [Trait("Category", nameof(Azure))]
    [Collection(nameof(Azure))]
    public class AzureStorageQueueBlockingQueueFacts : ItemBlockingQueueFacts
    {
        public override IQueue<Item> Create()
        {
            //This connectionString points a local Azure Storage. You can run one using https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio#running-azurite-from-the-command-line 
            var connectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";
            var queueName = "items";

            DeleteQueueAsync(connectionString, queueName).Wait();

            return new AzureStorageQueue<Item>(
                connectionString,
                queueName,
                new JsonItemSerializer());
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

        protected override Item CreateItem()
        {
            var item = base.CreateItem();
            item.StringProperty = $"<any>{item.StringProperty}</any>";
            return item;
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
