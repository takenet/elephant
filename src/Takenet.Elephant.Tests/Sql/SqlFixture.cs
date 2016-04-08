using System;
using System.Data.Common;
using System.Data.SqlClient;
using Takenet.Elephant.Sql;

namespace Takenet.Elephant.Tests.Sql
{
    public class SqlFixture : IDisposable
    {
        public SqlFixture()
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

        public void DropTable(string tableName)
        {
            using (var command = Connection.CreateCommand())
            {
                command.CommandText = $"IF EXISTS (SELECT * FROM sys.tables WHERE Name = '{tableName}') DROP TABLE {tableName}";
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