using Microsoft.WindowsAzure.Storage;
using Newtonsoft.Json;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Queue;
using Take.Elephant.Azure;
using Take.Elephant.Tests.Redis;
using Xunit;

namespace Take.Elephant.Tests.Azure
{
    [Trait("Category", nameof(Azure))]
    [Collection(nameof(Azure))]
    public class AzureStorageQueueBlockingQueueFacts : ItemBlockingQueueFacts
    {
        public override IQueue<Item> Create()
        {
            var connectionString = "DefaultEndpointsProtocol=https;AccountName=hmgblipqueues2;AccountKey=0qG1Lc5dz/xu5LYFgJTdXAlzYpBtbHAsMvquI1EM9y4dD3x9VspOsEGMAKRNrM/0yszFSID1pRgDG+zGCV++9A==;EndpointSuffix=core.windows.net";
            var queueName = "items";

            DeleteQueueAsync(connectionString, queueName).Wait();

            return new AzureStorageQueue<Item>(
                connectionString,
                queueName,
                new JsonItemSerializer());
        }

        private async Task DeleteQueueAsync(string connectionString, string queueName)
        {
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var client = storageAccount.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);

            if (await queue.ExistsAsync())
            {
                await queue.ClearAsync();
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
