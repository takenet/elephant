using System;
using Take.Elephant.Tests.Redis;
using Take.Elephant.Tests.Sql.SqlServer;

namespace Take.Elephant.Tests.Specialized
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
