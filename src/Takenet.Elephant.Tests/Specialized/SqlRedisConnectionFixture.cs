using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime;
using System.Text;
using System.Threading.Tasks;
using Takenet.Elephant.Tests.Redis;
using Takenet.Elephant.Tests.Sql;

namespace Takenet.Elephant.Tests.Specialized
{
    public class SqlRedisFixture : IDisposable
    {
        public SqlRedisFixture()
        {
            SqlConnectionFixture = new SqlFixture();
            RedisFixture = new RedisFixture();
        }

        public SqlFixture SqlConnectionFixture { get;  }

        public RedisFixture RedisFixture { get;  }

        public void Dispose()
        {
            SqlConnectionFixture.Dispose();
            RedisFixture.Dispose();
        }
    }
}
