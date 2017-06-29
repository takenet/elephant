using System;
using Takenet.Elephant.Sql;

namespace Takenet.Elephant.Tests.Sql
{
    public interface ISqlFixture : IDisposable
    {
        IDatabaseDriver DatabaseDriver { get; }

        string ConnectionString { get; }

        void DropTable(string schemaName, string tableName);
    }
}