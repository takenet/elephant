using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using NSubstitute;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;
using Xunit;

namespace Take.Elephant.Tests.Sql
{
    public class TableFacts
    {
        public TableFacts()
        {
            Schema = "my";
            Name = "Items";
            KeyColumnsNames = new[] {"Id"};
            Columns = new Dictionary<string, SqlType>()
            {
                {"Id", new SqlType(DbType.Guid)},
                {"Name", new SqlType(DbType.String, 250)},
            };
            SynchronizationStrategy = SchemaSynchronizationStrategy.UntilSuccess;
            ConnectionString = "db=fake;user=a;pass=b";
            DatabaseDriver = Substitute.For<IDatabaseDriver>();
            DbConnection = Substitute.For<DbConnection>();
            DbCommand = Substitute.For<DbCommand>();
            DbDataReader = Substitute.For<DbDataReader>();
            DatabaseDriver.CreateConnection(ConnectionString).Returns(DbConnection);
            DatabaseDriver.GetSqlStatementTemplate(Arg.Any<SqlStatement>()).Returns("");
            DbConnection.CreateCommand().Returns(DbCommand);
            DbCommand.ExecuteScalarAsync(Arg.Any<CancellationToken>()).Returns(true);
            DbCommand.ExecuteReaderAsync(Arg.Any<CancellationToken>()).Returns(DbDataReader);
        }

        public SchemaSynchronizationStrategy SynchronizationStrategy { get; set; }

        public string Schema { get; set; }

        public IDictionary<string, SqlType> Columns { get; set; }

        public string[] KeyColumnsNames { get; set; }

        public string Name { get; set; }

        public string ConnectionString { get; set; }

        public IDatabaseDriver DatabaseDriver { get; set; }

        public DbConnection DbConnection { get; set; }
        
        public DbCommand DbCommand { get; set; }
        
        public DbDataReader DbDataReader { get; set; }

        public CancellationToken CancellationToken { get; set; }
        
        private Table GetTarget()
        {
            return new Table(Name, KeyColumnsNames, Columns, Schema, SynchronizationStrategy);
        }

        [Fact]
        public async Task WithUntilSuccessSynchronizationStrategyExecuteOneIfSucceed()
        {
            // Arrange
            SynchronizationStrategy = SchemaSynchronizationStrategy.UntilSuccess;
            var target = GetTarget();
            
            // Act
            await target.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, CancellationToken);
            await target.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, CancellationToken);
            
            // Assert
            DatabaseDriver.Received(1).CreateConnection(ConnectionString);
        }
        
        [Fact]
        public async Task WithTryOnceSynchronizationStrategyExecuteOneIfSucceed()
        {
            // Arrange
            SynchronizationStrategy = SchemaSynchronizationStrategy.TryOnce;
            var target = GetTarget();
            
            // Act
            await target.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, CancellationToken);
            await target.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, CancellationToken);
            
            // Assert
            DatabaseDriver.Received(1).CreateConnection(ConnectionString);
        }
        
        [Fact]
        public async Task WithIgnoreSynchronizationStrategyNeverExecutes()
        {
            // Arrange
            SynchronizationStrategy = SchemaSynchronizationStrategy.Ignore;
            var target = GetTarget();
            
            // Act
            await target.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, CancellationToken);
            await target.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, CancellationToken);
            
            // Assert
            DatabaseDriver.DidNotReceive().CreateConnection(ConnectionString);
        }
        
        [Fact]
        public async Task WithUntilSuccessSynchronizationStrategyExecuteManyTimesIfFails()
        {
            // Arrange
            SynchronizationStrategy = SchemaSynchronizationStrategy.UntilSuccess;
            DatabaseDriver.CreateConnection(ConnectionString)
                .Returns(
                    x => throw new ApplicationException(), 
                    x => throw new ApplicationException(),
                    x => DbConnection);
            
            var target = GetTarget();
            
            // Act
            try
            {
                await target.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, CancellationToken);
            }
            catch (ApplicationException) { }
            try
            {
                await target.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, CancellationToken);
            }
            catch (ApplicationException) { }
            await target.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, CancellationToken);
            await target.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, CancellationToken);
            
            // Assert
            DatabaseDriver.Received(3).CreateConnection(ConnectionString);
        }
        
        [Fact]
        public async Task WithTryOnceSynchronizationStrategyExecuteOnceIfFails()
        {
            // Arrange
            SynchronizationStrategy = SchemaSynchronizationStrategy.TryOnce;
            DatabaseDriver.CreateConnection(ConnectionString)
                .Returns(
                    x => throw new ApplicationException(), 
                    x => throw new ApplicationException(),
                    x => DbConnection);
            
            var target = GetTarget();
            
            // Act
            try
            {
                await target.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, CancellationToken);
            }
            catch (ApplicationException) { }
            await target.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, CancellationToken);
            await target.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, CancellationToken);
            
            // Assert
            DatabaseDriver.Received(1).CreateConnection(ConnectionString);
        }

        [Fact]
        public async Task WithDefaultSynchronizationSchemeStrategyAndDebuggerAttachedShouldBeTryOnce()
        {
            // Arrange
            var target = new Table("newTableThatDoesNotExist", KeyColumnsNames, Columns, IsDebugging: true, Schema);

            // Act
            await target.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, CancellationToken);
            await target.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, CancellationToken);

            // Assert
            DatabaseDriver.Received(1).CreateConnection(ConnectionString);
        }

        [Fact]
        public async Task WithDefaultSynchronizationSchemeStrategyAndDebuggerNotAttachedShouldBeIgnore()
        {
            // Arrange
            var target = new Table("newTableThatDoesNotExist", KeyColumnsNames, Columns, IsDebugging: false, Schema);

            // Act
            await target.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, CancellationToken);
            await target.SynchronizeSchemaAsync(ConnectionString, DatabaseDriver, CancellationToken);

            // Assert
            DatabaseDriver.DidNotReceive().CreateConnection(ConnectionString);
        }
    }
}