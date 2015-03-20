using System.Collections.Generic;
using System.Data;
using Takenet.SimplePersistence.Sql.Mapping;

namespace Takenet.SimplePersistence.Tests.Sql
{
    public class IntegerStringTableMapper : IExtendedTableMapper<int, string>
    {
        public string TableName { get; } = "IntegerStrings";

        public IEnumerable<string> KeyColumns { get; } = new[]
        {
            "Key"
        };

        public IDictionary<string, SqlType> Columns { get; } = new Dictionary<string, SqlType>()
        {
            {"Key", new SqlType(DbType.Int32)},
            {"Value", new SqlType(DbType.String)}
        };        

        public IDictionary<string, object> GetColumnValues(string value, string[] columns = null, bool returnNullValues = false)
        {
            return new Dictionary<string, object>()
            {
                {"Value", value.ToString()}
            };
        }

        public string Create(IDataRecord record, string[] columns)
        {
            return (string)record["Value"];
        }

        public IEnumerable<string> ExtensionColumns { get; } = new[]
        {
            "Key"
        };

        public IDictionary<string, object> GetExtensionColumnValues(int extension, string[] columns = null, bool returnNullValues = false)
        {
            return new Dictionary<string, object>()
            {
                {"Key", extension.ToString()}
            };
        }

        public int CreateExtension(IDataRecord record, string[] columns)
        {
            return (int)record["Key"];
        }
    }
}