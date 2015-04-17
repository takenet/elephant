using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Takenet.SimplePersistence.Sql;
using Takenet.SimplePersistence.Sql.Mapping;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Sql
{
    [Collection("Sql")]
    public class SqlIntegerStringSetMapFacts : IntegerStringSetMapFacts
    {
        private readonly SqlConnectionFixture _fixture;

        public SqlIntegerStringSetMapFacts(SqlConnectionFixture fixture)
        {
            _fixture = fixture;
        }

        public override IMap<int, ISet<string>> Create()
        {
            var table = new IntegerStringTable();
            _fixture.DropTable(table.Name);
            return new SqlIntegerStringSetMap(table, _fixture.ConnectionString);
        }

        public override ISet<string> CreateValue(int key)
        {
            var set = new SimplePersistence.Memory.Set<string>();
            set.AddAsync(Fixture.Create<string>()).Wait();
            set.AddAsync(Fixture.Create<string>()).Wait();
            set.AddAsync(Fixture.Create<string>()).Wait();
            return set;
        }

        private class SqlIntegerStringSetMap : SqlSetMap<int, string>
        {
            public SqlIntegerStringSetMap(ITable table, string connectionString) 
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

            public string[] KeyColumns { get; } = { "Key", "Value" };

            public IDictionary<string, SqlType> Columns
            { get; }
            = new Dictionary<string, SqlType>
            {
                {"Key", new SqlType(DbType.Int32)},
                {"Value", new SqlType(DbType.String)}
            };
        }
    }
}
