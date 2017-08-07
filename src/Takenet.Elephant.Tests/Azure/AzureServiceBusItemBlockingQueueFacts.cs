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
                "Endpoint=sb://sb-iris.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=aEKWbW6XK20gMFqtQAx3yy5ezqyxwb6IHWPFbMImBys=",
                "items",
                new ItemSerializer());          
        }
    }
}
