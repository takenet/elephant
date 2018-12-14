using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Take.Elephant.Azure;
using Xunit;

namespace Take.Elephant.Tests.Azure
{
    [Trait("Category", nameof(Azure))]
    [Collection(nameof(Azure))]
    public class AzureStorageItemTransactionalBlockingQueueFacts : ItemTransactionalBlockingQueueFacts
    {
        public override IBlockingQueue<Item> Create()
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
}