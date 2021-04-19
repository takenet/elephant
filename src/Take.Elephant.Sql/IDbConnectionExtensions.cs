using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    public interface IDbConnectionExtensions
    {
        DbCommand CreateContainsCommand(DbConnection connection, IDatabaseDriver databaseDriver, ITable table, IDictionary<string, object> filterValues);
        DbCommand CreateDeleteCommand(DbConnection connection, IDatabaseDriver databaseDriver, ITable table, IDictionary<string, object> filterValues);
        DbCommand CreateInsertCommand(DbConnection connection, IDatabaseDriver databaseDriver, ITable table, IDictionary<string, object> columnValues);
        DbCommand CreateInsertOutputCommand(DbConnection connection, IDatabaseDriver databaseDriver, ITable table, IDictionary<string, object> columnValues, string[] outputColumnNames);
        DbCommand CreateInsertWhereNotExistsCommand(DbConnection connection, IDatabaseDriver databaseDriver, ITable table, IDictionary<string, object> filterValues, IDictionary<string, object> columnValues);
        DbCommand CreateMergeCommand(DbConnection connection, IDatabaseDriver databaseDriver, ITable table, IDictionary<string, object> keyValues, IDictionary<string, object> columnValues, IDictionary<string, object> identityKeyValues = null);
        DbCommand CreateMergeIncrementCommand(DbConnection connection, IDatabaseDriver databaseDriver, ITable table, string incrementColumnName, IDictionary<string, object> keyValues, IDictionary<string, object> columnValues);
        DbCommand CreateSelectCommand(DbConnection connection, IDatabaseDriver databaseDriver, ITable table, IDictionary<string, object> filterValues, string[] selectColumns, bool distinct = false);
        DbCommand CreateSelectCountCommand(DbConnection connection, IDatabaseDriver databaseDriver, ITable table, string filter = null, IDictionary<string, object> filterValues = null, bool distinct = false);
        DbCommand CreateSelectCountCommand(DbConnection connection, IDatabaseDriver databaseDriver, ITable table, IDictionary<string, object> filterValues);
        DbCommand CreateSelectSkipTakeCommand(DbConnection connection, IDatabaseDriver databaseDriver, ITable table, string[] selectColumns, string filter, int skip, int take, string[] orderByColumns, bool orderByAscending = true, IDictionary<string, object> filterValues = null, bool distinct = false);
        DbCommand CreateSelectTop1Command(DbConnection connection, IDatabaseDriver databaseDriver, ITable table, string[] selectColumns, IDictionary<string, object> filterValues);
        DbCommand CreateTextCommand(DbConnection connection, string commandTemplate, object format, IEnumerable<DbParameter> sqlParameters = null);
        DbCommand CreateUpdateCommand(DbConnection connection, IDatabaseDriver databaseDriver, ITable table, IDictionary<string, object> filterValues, IDictionary<string, object> columnValues);
        Task<TResult> ExecuteScalarAsync<TResult>(DbConnection connection, string commandText, CancellationToken cancellationToken, SqlParameter[] sqlParameters = null);
    }
}