using System;
using Take.Elephant.Sql;

namespace Take.Elephant.Tests.Sql
{
    public interface ISqlFixture : IDisposable
    {
        IDatabaseDriver DatabaseDriver { get; }

        string ConnectionString { get; }

        void DropTable(string schemaName, string tableName);
    }
}