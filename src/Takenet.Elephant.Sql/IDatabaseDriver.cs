using System;
using System.Data;
using System.Data.Common;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Sql
{
    public interface IDatabaseDriver
    {
        TimeSpan Timeout { get; }

        DbConnection CreateConnection(string connectionString);

        string GetSqlStatementTemplate(SqlStatement sqlStatement);

        string GetSqlTypeName(DbType dbType);

        DbParameter CreateParameter(string parameterName, object value);
    }

    public enum SqlStatement
    {
        AlterTableAddColumn,
        And,
        ColumnDefinition,
        CreateTable,
        Delete,
        DeleteAndInsertWhereNotExists,
        Equal,
        Exists,
        GetTableColumns,
        Int16IdentityColumnDefinition,
        Int32IdentityColumnDefinition,
        Int64IdentityColumnDefinition,
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
        Update,
        Merge,
        OneEqualsOne,
        OneEqualsZero,
        DummyEqualsZero,
        ValueAsColumn,
        Not,
        NotEqual,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
        Like,
        MaxLength
    }
}
