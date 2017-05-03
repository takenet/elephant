using System;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Tests.Sql
{
    public abstract class SqlGuidNumberMapFacts : GuidNumberMapFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlGuidNumberMapFacts(ISqlFixture serverFixture)
        {
            _serverFixture = serverFixture;
        }

        public override INumberMap<Guid> Create()
        {
            var table = TableBuilder
                .WithName("GuidNumbers")
                .WithKeyColumnFromType<Guid>("Key")
                .WithColumnFromType<long>("Counter")
                .Build();
            _serverFixture.DropTable(table.Name);
            var keyMapper = new ValueMapper<Guid>("Key");
            return new SqlNumberMap<Guid>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, keyMapper, "Counter");
        }
    }
}
