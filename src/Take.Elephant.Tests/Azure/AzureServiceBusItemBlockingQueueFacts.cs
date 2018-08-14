using Microsoft.Azure.ServiceBus.Management;
using System.Threading.Tasks;
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
            var connectionString = "Endpoint=sb://hmg-blip.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=57wz+0+zTW5A78wRk05UlFF+e6tICQOfF26wYQ6TpyU=";
            var path = "items";

            DeleteQueueAsync(connectionString, path).Wait();

            return new AzureServiceBusQueue<Item>(
                connectionString,
                path,
                new ItemSerializer());
        }

        private async Task DeleteQueueAsync(string connectionString, string path)
        {
            var managementClient = new ManagementClient(connectionString);

            if (await managementClient.QueueExistsAsync(path))
            {
                await managementClient.DeleteQueueAsync(path);
            }

            await managementClient.CloseAsync();
        }
    }
}
