using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Sql;
using Takenet.SimplePersistence.Sql.Mapping;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Sql
{
    //[Collection("Sql")]
    //public class SqlGuidItemMapFacts : GuidItemMapFacts, IClassFixture<SqlConnectionFixture>
    //{
    //    private readonly SqlConnectionFixture _fixture;

    //    public SqlGuidItemMapFacts(SqlConnectionFixture fixture)
    //    {
    //        _fixture = fixture;
    //    }

    //    public override IMap<Guid, Item> Create()
    //    {
    //        var mapper = new GuidItemTableMapper();

    //        using (var command = _fixture.Connection.CreateCommand())
    //        {
    //            command.CommandText = $"IF EXISTS(SELECT * FROM sys.tables WHERE Name = '{mapper.TableName}') DROP TABLE {mapper.TableName}";
    //            command.ExecuteNonQuery();
    //        }
            
    //        return new SqlMap<int, string>(mapper, _fixture.ConnectionString);
    //    }

    //    private class GuidItemTableMapper : ITable
    //    {
    //        public string TableName => "GuidItems";

    //        public IEnumerable<string> KeyColumns
    //        {
    //            get { throw new NotImplementedException(); }
    //        }

    //        public IDictionary<string, SqlType> Columns
    //        {
    //            get { throw new NotImplementedException(); }
    //        }

    //        public IDictionary<string, object> GetColumnValues(Item value, string[] columns = null, bool returnNullValues = false)
    //        {
    //            throw new NotImplementedException();
    //        }

    //        public Item Create(IDataRecord record, string[] columns)
    //        {
    //            throw new NotImplementedException();
    //        }

    //        public IEnumerable<string> ExtensionColumns
    //        {
    //            get { throw new NotImplementedException(); }
    //        }

    //        public IDictionary<string, object> GetExtensionColumnValues(Guid extension, string[] columns = null, bool returnNullValues = false)
    //        {
    //            throw new NotImplementedException();
    //        }

    //        public Guid CreateExtension(IDataRecord record, string[] columns)
    //        {
    //            throw new NotImplementedException();
    //        }
    //    }
    //}
}
