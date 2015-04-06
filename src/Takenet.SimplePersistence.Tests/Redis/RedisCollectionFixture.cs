using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Redis
{
    [CollectionDefinition("Redis")]
    public class RedisCollectionFixture : ICollectionFixture<RedisFixture>
    {
    }
}
