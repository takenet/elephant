using System.Data.Common;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.PostgreSql;

namespace Takenet.Elephant.Tests.Sql.PostgreSql
{
    public class PostgreSqlFixture : ISqlFixture
    {
        public PostgreSqlFixture()
        {
            DatabaseDriver = new PostgreSqlDatabaseDriver();

            // Note: You should create the Localdb instance if it doesn't exists
            // Go to the command prompt and run: sqllocaldb create "MSSQLLocalDB"
            Connection = DatabaseDriver.CreateConnection(ConnectionString);
            Connection.Open();            
            
        }

        public DbConnection Connection { get; }

        public IDatabaseDriver DatabaseDriver { get; }

        public string ConnectionString { get; } = @"Server=127.0.0.1;Port=5432;Database=Elephant;User Id=elephant;Password=elephant;";

        public void DropTable(string tableName)
        {
            using (var command = Connection.CreateCommand())
            {
                command.CommandText = $"DROP TABLE IF EXISTS public.{tableName.AsSqlIdentifier()}";
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