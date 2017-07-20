using System;
using System.Data;
using System.Reflection;
using Ploeh.AutoFixture;
using Takenet.Elephant.Memory;
using Takenet.Elephant.Redis;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;
using Takenet.Elephant.Tests.Redis;
using Xunit;

namespace Takenet.Elephant.Tests.Specialized
{
    [Collection("SqlRedis")]
    public class SqlRedisGuidItemOnDemandCacheSetMapFacts : GuidItemOnDemandCacheSetMapFacts
    {
        private readonly SqlRedisFixture _fixture;
        public const string MapName = "guid-items";

        public SqlRedisGuidItemOnDemandCacheSetMapFacts(SqlRedisFixture fixture)
        {
            _fixture = fixture;
        }

        public override IMap<Guid, ISet<Item>> CreateSource()
        {
            var databaseDriver = new SqlDatabaseDriver();
            var columns = typeof(Item)
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .ToSqlColumns();
            columns.Add("Key", new SqlType(DbType.Guid));
            var table = new Table("GuidItems", new[] { "Key", nameof(Item.GuidProperty) }, columns);
            _fixture.SqlConnectionFixture.DropTable(table.Schema, table.Name);
            var keyMapper = new ValueMapper<Guid>("Key");
            var valueMapper = new TypeMapper<Item>(table);
            return new SqlSetMap<Guid, Item>(databaseDriver, _fixture.SqlConnectionFixture.ConnectionString, table, keyMapper, valueMapper);
        }

        public override IMap<Guid, ISet<Item>> CreateCache()
        {
            var db = 1;
            _fixture.RedisFixture.Server.FlushDatabase(db);
            var setMap = new RedisSetMap<Guid, Item>(MapName, _fixture.RedisFixture.Connection.Configuration, new ItemSerializer(), db);
            return setMap;
        }

        public override ISet<Item> CreateValue(Guid key, bool populate)
        {
            var set = new Set<Item>();
            if (populate)
            {
                set.AddAsync(Fixture.Create<Item>()).Wait();
                set.AddAsync(Fixture.Create<Item>()).Wait();
                set.AddAsync(Fixture.Create<Item>()).Wait();
            }
            return set;
        }
    }
}