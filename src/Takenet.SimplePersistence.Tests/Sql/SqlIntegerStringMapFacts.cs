using System.Collections.Generic;
using System.Data;
using Takenet.SimplePersistence.Sql;
using Takenet.SimplePersistence.Sql.Mapping;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Sql
{
    [Collection("Sql")]
    public class SqlIntegerStringMapFacts : IntegerStringMapFacts, IClassFixture<SqlConnectionFixture>
    {
        private readonly SqlConnectionFixture _fixture;

        public SqlIntegerStringMapFacts(SqlConnectionFixture fixture)
        {
            _fixture = fixture;
        }

        public override IMap<int, string> Create()
        {
            var table = new IntegerStringTable();

            using (var command = _fixture.Connection.CreateCommand())
            {
                command.CommandText = $"IF EXISTS(SELECT * FROM sys.tables WHERE Name = '{table.TableName}') DROP TABLE {table.TableName}";
                command.ExecuteNonQuery();
            }

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
            protected override IMapper<int> KeyMapper { get; }
        }

        private class IntegerStringTable : ITable
        {
            public string TableName => "IntegerStrings";

            public string[] KeyColumns { get; } = { "Key" };

            public IDictionary<string, SqlType> Columns { get; } = new Dictionary<string, SqlType>
            {
                {"Key", new SqlType(DbType.Int32)},
                {"Value", new SqlType(DbType.String)}
            };
        }

    }
}
