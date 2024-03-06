using System;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using Take.Elephant.Sql;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    public class SqlServerFixture : ISqlFixture
    {
        public SqlServerFixture()
        {
            // Note: You should create the Localdb instance if it doesn't exists
            // Go to the command prompt and run: sqllocaldb create "MSSQLLocalDB"
            Connection = new SqlConnection(ConnectionString.Replace(DatabaseName, "master"));
            Connection.Open();
            using (var dropDatabaseCommand = Connection.CreateCommand())
            {
                dropDatabaseCommand.CommandText = $"IF NOT EXISTS(SELECT * FROM sys.databases WHERE Name = '{DatabaseName}') CREATE DATABASE {DatabaseName}";
                dropDatabaseCommand.ExecuteNonQuery();
            }

            using (var useDatabaseCommand = Connection.CreateCommand())
            {
                useDatabaseCommand.CommandText = $"USE {DatabaseName}";
                useDatabaseCommand.ExecuteNonQuery();
            }

            DatabaseDriver = new SqlDatabaseDriver();
        }

        public DbConnection Connection { get; }

        public string DatabaseName { get; } = "Elephant";

        public IDatabaseDriver DatabaseDriver { get; }

        public string ConnectionString { get; } = @"Server=(localdb)\MSSQLLocalDB;Database=Elephant;Integrated Security=true";

        public void DropTable(string schemaName, string tableName)
        {
            schemaName = schemaName ?? DatabaseDriver.DefaultSchema;

            using (var command = Connection.CreateCommand())
            {
                command.CommandText = $"IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schemaName}' AND TABLE_NAME = '{tableName}') DROP TABLE [{schemaName}].[{tableName}]";
                command.ExecuteNonQuery();
            }
        }

        public void Dispose()
        {
            Connection.Close();
            Connection.Dispose();
        }
    }
}