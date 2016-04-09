using System;
using Takenet.Elephant.Tests.Redis;
using Takenet.Elephant.Tests.Sql.SqlServer;

namespace Takenet.Elephant.Tests.Specialized
{
    public class SqlRedisFixture : IDisposable
    {
        public SqlRedisFixture()
        {
            SqlConnectionFixture = new SqlServerFixture();
            RedisFixture = new RedisFixture();
        }

        public SqlServerFixture SqlConnectionFixture { get;  }

        public RedisFixture RedisFixture { get;  }

        public void Dispose()
        {
            SqlConnectionFixture.Dispose();
            RedisFixture.Dispose();
        }
    }
}
