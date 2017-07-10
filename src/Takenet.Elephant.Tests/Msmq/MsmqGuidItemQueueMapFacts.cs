using System;

using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Takenet.Elephant.Memory;
using Takenet.Elephant.Msmq;
using Takenet.Elephant.Tests.Redis;
using Xunit;

namespace Takenet.Elephant.Tests.Msmq
{
    [Trait("Category", nameof(Msmq))]
    [Collection(nameof(Msmq))]
    public class MsmqGuidItemQueueMapFacts : GuidItemQueueMapFacts
    {
        public override IMap<Guid, IQueue<Item>> Create()
        {
            var pathTemplate = $@".\private$\items_{MsmqQueueMap<Guid, Item>.PATH_TEMPLATE_KEY_PLACEHOLDER}";
            return new MsmqQueueMap<Guid, Item>(pathTemplate, new ItemSerializer());
        }

        public override IQueue<Item> CreateValue(Guid key)
        {
            var set = new Queue<Item>();
            set.EnqueueAsync(Fixture.Create<Item>()).Wait();
            set.EnqueueAsync(Fixture.Create<Item>()).Wait();
            set.EnqueueAsync(Fixture.Create<Item>()).Wait();
            return set;
        }
    }
}
