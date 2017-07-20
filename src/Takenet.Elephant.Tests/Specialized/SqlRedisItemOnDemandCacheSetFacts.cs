using Takenet.Elephant.Redis;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;
using Takenet.Elephant.Tests.Redis;
using Xunit;

namespace Takenet.Elephant.Tests.Specialized
{

    [Collection("SqlRedis")]
    public class SqlRedisItemOnDemandCacheSetFacts : ItemOnDemandCacheSetFacts
    {
        private readonly SqlRedisFixture _fixture;

        public SqlRedisItemOnDemandCacheSetFacts(SqlRedisFixture fixture)
        {
            _fixture = fixture;
        }

        public override ISet<Item> CreateSource()
        {
            var databaseDriver = new SqlDatabaseDriver();
            var table = TableBuilder.WithName("Items").WithKeyColumnsFromTypeProperties<Item>().Build();
            _fixture.SqlConnectionFixture.DropTable(table.Schema, table.Name);
            var mapper = new TypeMapper<Item>(table);
            return new SqlSet<Item>(databaseDriver, _fixture.SqlConnectionFixture.ConnectionString, table, mapper);
        }

        public override ISet<Item> CreateCache()
        {
            var db = 1;
            _fixture.RedisFixture.Server.FlushDatabase(db);
            const string setName = "items";
            return new RedisSet<Item>(setName, _fixture.RedisFixture.Connection.Configuration, new ItemSerializer(), db);
        }
    }
}