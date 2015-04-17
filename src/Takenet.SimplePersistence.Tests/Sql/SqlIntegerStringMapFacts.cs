using System.Collections.Generic;
using System.Data;
using Takenet.SimplePersistence.Sql;
using Takenet.SimplePersistence.Sql.Mapping;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Sql
{
    [Collection("Sql")]
    public class SqlIntegerStringMapFacts : IntegerStringMapFacts
    {
        private readonly SqlConnectionFixture _fixture;

        public SqlIntegerStringMapFacts(SqlConnectionFixture fixture)
        {
            _fixture = fixture;
        }

        public override IMap<int, string> Create()
        {
            var table = new IntegerStringTable();
            _fixture.DropTable(table.Name);
            return new IntegerStringSqlMap(table, _fixture.ConnectionString);
        }

        private class IntegerStringSqlMap : SqlMap<int, string>
        {
            public IntegerStringSqlMap(ITable table, string connectionString) 
                : base(table, connectionString)
                
            {
                KeyMapper = new ValueMapper<int>("Key");
                Mapper = new ValueMapper<string>("Value");                
            }

            protected override IMapper<string> Mapper { get; }
            protected override IDatabaseDriver DatabaseDriver => new SqlDatabaseDriver();
            protected override IMapper<int> KeyMapper { get; }
        }

        private class IntegerStringTable : ITable
        {
            public string Name => "IntegerStrings";

            public string[] KeyColumns { get; } = { "Key" };

            public IDictionary<string, SqlType> Columns { get; } = new Dictionary<string, SqlType>
            {
                {"Key", new SqlType(DbType.Int32)},
                {"Value", new SqlType(DbType.String)}
            };
        }

    }
}
