using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Take.Elephant.Azure;
using Xunit;

namespace Take.Elephant.Tests.Azure
{
    [Trait("Category", nameof(Azure))]
    [Collection(nameof(Azure))]
    public class AzureServiceItemTransactionalBatchQueueFacts : ItemTransactionalBatchQueueFacts
    {
        public override IBatchReceiverQueue<Item> Create(params Item[] items)
        {
            var connectionString = "";
            var queueName = "batchitems";

            DeleteQueueAsync(connectionString, queueName).Wait();

            var queue = new AzureStorageQueue<Item>(
                connectionString,
                queueName,
                new JsonItemSerializer());

            using (var cts = new CancellationTokenSource(30000))
            {
                foreach (var item in items)
                {
                    queue.EnqueueAsync(item, cts.Token).Wait();
                }
            }

            return queue;
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
    }
}