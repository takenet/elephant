using Takenet.Elephant.Azure;
using Takenet.Elephant.Tests.Redis;
using Xunit;

namespace Takenet.Elephant.Tests.Azure
{
    [Trait("Category", nameof(Azure))]
    [Collection(nameof(Azure))]
    public class AzureServiceBusItemBlockingQueueFacts : ItemBlockingQueueFacts
    {
        public override IQueue<Item> Create()
        {
            return new AzureServiceBusQueue<Item>(
                "Endpoint=sb://?.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=?",
                "items",
                new ItemSerializer());          
        }
    }
}
