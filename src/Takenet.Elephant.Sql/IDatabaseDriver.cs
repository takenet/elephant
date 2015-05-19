using System.Data;
using System.Data.Common;

namespace Takenet.Elephant.Sql
{
    public interface IDatabaseDriver
    {
        DbConnection CreateConnection(string connectionString);

        string GetSqlStatementTemplate(SqlStatement sqlStatement);

        string GetSqlTypeName(DbType dbType);
    }

    public enum SqlStatement
    {
        AlterTableAddColumn,
        And,
        ColumnDefinition,
        CreateTable,
        Delete,
        DeleteAndInsertWhereNotExists,
        DeleteFromTableIfExists,
        Equal,
        Exists,
        GetTableColumns,
        IdentityColumnDefinition,
        In,
        Insert,
        InsertWhereNotExists,
        NullableColumnDefinition,
        Or,
        PrimaryKeyConstraintDefinition,
        QueryEquals,
        QueryGreatherThen,
        QueryLessThen,
        Select,
        SelectCount,
        SelectSkipTake,
        SelectTop1,
        TableExists,
        Update
    }
}
