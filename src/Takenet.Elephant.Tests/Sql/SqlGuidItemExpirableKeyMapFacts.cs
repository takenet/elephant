using System;
using System.Data;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Tests.Sql
{
    public abstract class SqlGuidItemExpirableKeyMapFacts : GuidItemExpirableKeyMapFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlGuidItemExpirableKeyMapFacts(ISqlFixture serverFixture)
        {
            _serverFixture = serverFixture;
        }

        public override TimeSpan CreateTtl() => TimeSpan.FromSeconds(1);

        public override IExpirableKeyMap<Guid, Item> Create()
        {
            var expirationColumnName = "Expiration";

            var table = TableBuilder
                .WithName("ExpirableGuidItems")
                .WithColumnsFromTypeProperties<Item>(p => !p.Name.Equals(nameof(Item.StringProperty)))
                .WithColumn(nameof(Item.StringProperty), new SqlType(DbType.String, int.MaxValue))
                .WithColumn(expirationColumnName, new SqlType(DbType.DateTimeOffset))
                .WithKeyColumnFromType<Guid>("Key")
                .Build();
            _serverFixture.DropTable(table.Schema, table.Name);

            var keyMapper = new ValueMapper<Guid>("Key");
            var valueMapper = new TypeMapper<Item>(table);
            return new ExpirableKeySqlMap<Guid, Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, keyMapper, valueMapper, expirationColumnName);
        }
    }
}
