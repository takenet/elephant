using System;
using System.Collections.Generic;
using System.Linq;
using System.Messaging;
using System.Text;
using System.Threading.Tasks;
using Takenet.Elephant.Msmq;
using Takenet.Elephant.Tests.Redis;
using Xunit;

namespace Takenet.Elephant.Tests.Msmq
{
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
