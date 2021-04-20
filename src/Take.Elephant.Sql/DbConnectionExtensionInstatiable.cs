using System;
using System.Collections.Generic;
using System.Data.Common;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    public class DbConnectionExtensionInstatiable : IDbConnectionExtensionInstatiable
    {
        public DbCommand CreateSelectTop1Command(
            DbConnection connection, 
            IDatabaseDriver databaseDriver, 
            ITable table, 
            string[] selectColumns, 
            IDictionary<string, object> filterValues)
        {
            var command = connection.CreateSelectTop1Command(databaseDriver,table,selectColumns,filterValues);
            var parameter = command.CreateParameter();
            parameter.ParameterName = "@ExpirableKeySqlMap_ExpirationDate";
            parameter.Value = DateTimeOffset.UtcNow;
            command.Parameters.Add(parameter);

            return command;
        }
    }

    public interface IDbConnectionExtensionInstatiable
    {
        public DbCommand CreateSelectTop1Command(
            DbConnection connection,
            IDatabaseDriver databaseDriver,
            ITable table,
            string[] selectColumns,
            IDictionary<string,object> filterValues);
    }
}
