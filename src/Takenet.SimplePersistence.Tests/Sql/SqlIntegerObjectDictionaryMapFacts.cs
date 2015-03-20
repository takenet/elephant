using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Sql.Mapping;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Sql
{
    public class SqlIntegerStringDictionaryMapFacts : IntegerStringDictionaryMapFacts, IClassFixture<SqlConnectionFixture>
    {
        private readonly SqlConnectionFixture _fixture;

        public SqlIntegerStringDictionaryMapFacts(SqlConnectionFixture fixture)
        {
            _fixture = fixture;
        }

        public override IMap<int, string> Create()
        {
            var mapper = new IntegerStringTableMapper();

            using (var command = _fixture.Connection.CreateCommand())
            {
                command.CommandText = $"IF EXISTS(SELECT * FROM sys.tables WHERE Name = '{mapper.TableName}') DROP TABLE {mapper.TableName}";
                command.ExecuteNonQuery();
            }
            
            return new SqlMap<int, string>(mapper, _fixture.ConnectionString);
        }
    }
}
