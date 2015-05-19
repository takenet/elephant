using System;
using System.Data.SqlClient;

namespace Takenet.Elephant.Tests.Sql
{
    public class SqlConnectionFixture : IDisposable
    {
        public SqlConnectionFixture()
        {
            // Note: You should create the Localdb instance if it doesn't exists
            // Go to the command prompt and run: sqllocaldb create "v12.0"
            Connection = new SqlConnection(@"Server=(localdb)\v12.0;Database=master;Integrated Security=true");

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
        }

        public SqlConnection Connection { get; }

        public string DatabaseName { get; } = "Elephant";

        public string ConnectionString { get; } = @"Server=(localdb)\v12.0;Database=Elephant;Integrated Security=true";

        public void DropTable(string tableName)
        {
            using (var command = Connection.CreateCommand())
            {
                command.CommandText = $"IF EXISTS(SELECT * FROM sys.tables WHERE Name = '{tableName}') DROP TABLE {tableName}";
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