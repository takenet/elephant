using System;
using System.Collections.Generic;
using System.Linq;
using System.Messaging;
using System.Text;
using System.Threading.Tasks;
using Take.Elephant.Msmq;
using Take.Elephant.Tests.Redis;
using Xunit;

namespace Take.Elephant.Tests.Msmq
{
    [Trait("Category", nameof(Msmq))]
    [Collection(nameof(Msmq))]
    public class MsmqItemBlockingQueueFacts : ItemBlockingQueueFacts
    {
        public override IQueue<Item> Create()
        {
            var path = @".\private$\items";
            if (MessageQueue.Exists(path))
            {
                MessageQueue.Delete(path);
            }

            return new MsmqQueue<Item>(path, new ItemSerializer());            
        }
    }
}
