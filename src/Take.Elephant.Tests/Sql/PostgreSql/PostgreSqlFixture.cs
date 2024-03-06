﻿using Take.Elephant.Sql;
using Take.Elephant.Sql.PostgreSql;

namespace Take.Elephant.Tests.Sql.PostgreSql
{
    public class PostgreSqlFixture : ISqlFixture
    {
        public PostgreSqlFixture()
        {
            DatabaseDriver = new PostgreSqlDatabaseDriver();
        }

        public IDatabaseDriver DatabaseDriver { get; }

        public string ConnectionString { get; } =
            @"Server=localhost;Port=5432;Database=elephant;User Id=elephant;Password=elephant;";

        public void DropTable(string schemaName, string tableName)
        {
            using (var connection = DatabaseDriver.CreateConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"DROP TABLE IF EXISTS {DatabaseDriver.ParseIdentifier(schemaName ?? DatabaseDriver.DefaultSchema)}.{DatabaseDriver.ParseIdentifier(tableName)}";
                    command.ExecuteNonQuery();
                }
                connection.Close();
            }
        }

        public void Dispose()
        {
            
        }


    }
}