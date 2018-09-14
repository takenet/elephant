using Microsoft.WindowsAzure.Storage;
using System.Threading.Tasks;
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
            var connectionString = "DefaultEndpointsProtocol=https;AccountName=hmgblipstorage;AccountKey=8Zg/Bmk1vwU0Daap6T+orTk0btf63tdPwNOOrhLLdya2Oym2q/J979PZWXGcNGrI9KZKikhuk8/t3LOD5w7isw==;EndpointSuffix=core.windows.net";
            var queueName = "items";

            DeleteQueueAsync(connectionString, queueName).Wait();

            return new AzureStorageQueue<Item>(
                connectionString,
                queueName,
                new ItemSerializer());
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
