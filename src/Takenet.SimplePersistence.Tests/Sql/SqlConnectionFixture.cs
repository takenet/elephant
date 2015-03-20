using System;
using System.Data.SqlClient;

namespace Takenet.SimplePersistence.Tests.Sql
{
    public class SqlConnectionFixture : IDisposable
    {
        public SqlConnectionFixture()
        {
            // Note: You should create the Localdb instance if it doesn't exists
            // Go to the command prompt and run: sqllocaldb create "v12.0"
            Connection = new SqlConnection(@"Server=(localdb)\v12.0;Database=master;Integrated Security=true");

            Connection.Open();
            using (var command = Connection.CreateCommand())
            {
                command.CommandText = $"IF EXISTS(SELECT * FROM sys.databases WHERE Name = '{DatabaseName}') DROP DATABASE {DatabaseName}; CREATE DATABASE {DatabaseName}; USE {DatabaseName}";
                command.ExecuteNonQuery();
            }            
        }

        public SqlConnection Connection { get; }

        public string DatabaseName { get; } = "SimplePersistence";

        public string ConnectionString { get; } = @"Server=(localdb)\v12.0;Database=SimplePersistence;Integrated Security=true";

        public void Dispose()
        {
            Connection.Close();
            Connection.Dispose();
        }
    }
}