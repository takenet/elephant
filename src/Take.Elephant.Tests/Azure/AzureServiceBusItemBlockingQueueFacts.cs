using System.Threading.Tasks;
using Azure.Messaging.ServiceBus.Administration;
using Take.Elephant.Azure;
using Take.Elephant.Tests.Redis;
using Xunit;

namespace Take.Elephant.Tests.Azure
{
    [Trait("Category", nameof(Azure))]
    [Collection(nameof(Azure))]
    public class AzureServiceBusItemBlockingQueueFacts : ItemBlockingQueueFacts
    {
        public override IQueue<Item> Create()
        {
            //Service Bus does not have a local testing method. Therefore, use an azure service bus connectionstring to run this tests https://jimmybogard.com/local-development-with-azure-service-bus/ 
            var connectionString = "";
            var path = "items";

            DeleteQueueAsync(connectionString, path).Wait();

            return new AzureServiceBusQueue<Item>(
                connectionString,
                path,
                new ItemSerializer());
        }

        private async Task DeleteQueueAsync(string connectionString, string path)
        {
            var administrationClient = new ServiceBusAdministrationClient(connectionString);

            if (await administrationClient.QueueExistsAsync(path))
            {
                await administrationClient.DeleteQueueAsync(path);
            }
        }
    }
}
