using System;
using Takenet.Elephant.Redis;
using Takenet.Elephant.Redis.Converters;
using Takenet.Elephant.Specialized;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;
using Xunit;

namespace Takenet.Elephant.Tests.Specialized
{
    [Collection("SqlRedis")]
    public class SqlRedisGuidItemCacheMapFacts : GuidItemCacheMapFacts
    {
        private readonly SqlRedisFixture _fixture;

        public SqlRedisGuidItemCacheMapFacts(SqlRedisFixture fixture)
        {
            _fixture = fixture;
        }

        public override IMap<Guid, Item> CreateSource()
        {
            var databaseDriver = new SqlDatabaseDriver();
            var table = TableBuilder
                .WithName("GuidItems")
                .WithColumnsFromTypeProperties<Item>()
                .WithKeyColumnFromType<Guid>("Key")
                .Build();
            _fixture.SqlConnectionFixture.DropTable(table.Name);

            var keyMapper = new ValueMapper<Guid>("Key");
            var valueMapper = new TypeMapper<Item>(table);
            return new SqlMap<Guid, Item>(databaseDriver, _fixture.SqlConnectionFixture.ConnectionString, table, keyMapper, valueMapper);
        }

        public override IMap<Guid, Item> CreateCache()
        {
            int db = 0;
            _fixture.RedisFixture.Server.FlushDatabase(db);
            const string mapName = "guid-item-hash";
            return new RedisHashMap<Guid, Item>(mapName, new TypeRedisDictionaryConverter<Item>(), _fixture.RedisFixture.Connection.Configuration, db);
        }
    }
}