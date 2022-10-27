using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using Take.Elephant.Azure;
using Xunit;

namespace Take.Elephant.Tests.Azure
{
    [Trait("Category", nameof(Azure))]
    [Collection(nameof(Azure))]
    public class AzureStorageItemBatchReceiverQueueFacts : ItemBatchReceiverQueueFacts
    {
        public override IBatchReceiverQueue<Item> Create(params Item[] items)
        {
            //This connectionString points a local Azure Storage. You can run one using https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio#running-azurite-from-the-command-line 
            var connectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";
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
            var options = new QueueClientOptions();
            options.MessageEncoding = QueueMessageEncoding.Base64;
            var queue = new QueueClient(connectionString, queueName, options);

            if (await queue.ExistsAsync())
            {
                await queue.ClearMessagesAsync();
            }
        }
    }
}
