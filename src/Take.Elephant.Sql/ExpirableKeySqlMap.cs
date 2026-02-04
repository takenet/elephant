using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Take.Elephant.Sql.Mapping;

[assembly: InternalsVisibleTo("Take.Elephant.Tests")]

namespace Take.Elephant.Sql
{
    /// <summary>
    /// Defines a <see cref="SqlMap{TKey,TValue}"/> that ignores expired rows.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public partial class ExpirableKeySqlMap<TKey, TValue> : SqlMap<TKey, TValue>, IExpirableKeyMap<TKey, TValue>
    {
        private readonly string _expirationColumnName;

        public ExpirableKeySqlMap(IDatabaseDriver databaseDriver, string connectionString, ITable table, IMapper<TKey> keyMapper, IMapper<TValue> valueMapper, string expirationColumnName, SqlRetryLogicOption retryOptions = null)
            : base(new ExpirationDatabaseDriverDecorator(databaseDriver, expirationColumnName), connectionString, table, keyMapper, valueMapper, retryOptions)
        {
            _expirationColumnName = expirationColumnName ?? throw new ArgumentNullException(nameof(expirationColumnName));
            if (!Table.Columns.TryGetValue(expirationColumnName, out var expirationColumnType))
            {
                throw new ArgumentException($"The table doesn't contains an '{expirationColumnName}' column", nameof(expirationColumnName));
            }

            if (expirationColumnType.Type != DbType.Date
                && expirationColumnType.Type != DbType.DateTime
                && expirationColumnType.Type != DbType.DateTime2
                && expirationColumnType.Type != DbType.DateTimeOffset)
            {
                throw new ArgumentException($"The expiration column '{expirationColumnName}' must have a date type", nameof(expirationColumnName));
            }
        }

        public Task<bool> TryAddWithRelativeExpirationAsync(TKey key, TValue value,
            TimeSpan expiration = default,
            bool overwrite = false, CancellationToken cancellationToken = default) =>
            ExpirableKeyMapCommon.TryAddWithRelativeExpirationAsync(this, key, value,
                expiration, overwrite, cancellationToken);

        public Task<bool> TryAddWithAbsoluteExpirationAsync(TKey key, TValue value,
            DateTimeOffset expiration = default,
            bool overwrite = false, CancellationToken cancellationToken = default)
            => ExpirableKeyMapCommon.TryAddWithAbsoluteExpirationAsync(this, key, value,
                expiration, overwrite, cancellationToken);

        public virtual Task<bool> SetRelativeKeyExpirationAsync(TKey key, TimeSpan ttl) =>
            SetAbsoluteKeyExpirationAsync(key, DateTimeOffset.UtcNow.Add(ttl));

        public virtual async Task<bool> SetAbsoluteKeyExpirationAsync(TKey key, DateTimeOffset expiration)
        {
            using var cancellationTokenSource = CreateCancellationTokenSource();
            await using var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false);
            var keyColumnValues = KeyMapper.GetColumnValues(key);
            var columnValues = new Dictionary<string, object>
                {
                    {_expirationColumnName, expiration}
                };

            await using var command = connection.CreateTextCommand(
                DatabaseDriver.GetSqlStatementTemplate(SqlStatement.Update),
                new
                {
                    schemaName = DatabaseDriver.ParseIdentifier(Table.Schema ?? DatabaseDriver.DefaultSchema),
                    tableName = DatabaseDriver.ParseIdentifier(Table.Name),
                    columnValues = SqlHelper.GetCommaEqualsStatement(DatabaseDriver, columnValues.Keys.ToArray()),
                    filter = SqlHelper.GetAndEqualsStatement(DatabaseDriver, keyColumnValues.Keys.ToArray())
                },
                keyColumnValues.Concat(columnValues).Select(c => c.ToDbParameter(DatabaseDriver, Table.Columns)));

            if (await command.ExecuteNonQueryAsync(cancellationTokenSource.Token).ConfigureAwait(false) == 0)
            {
                return false;
            }

            return true;
        }

        public virtual async Task<bool> RemoveExpirationAsync(TKey key)
        {
            using var cancellationTokenSource = CreateCancellationTokenSource();
            await using var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false);
            var keyColumnValues = KeyMapper.GetColumnValues(key);
            var columnValues = new Dictionary<string, object>
                {
                    {_expirationColumnName, DBNull.Value}
                };

            await using var command = connection.CreateTextCommand(
                DatabaseDriver.GetSqlStatementTemplate(SqlStatement.Update),
                new
                {
                    schemaName = DatabaseDriver.ParseIdentifier(Table.Schema ?? DatabaseDriver.DefaultSchema),
                    tableName = DatabaseDriver.ParseIdentifier(Table.Name),
                    columnValues = SqlHelper.GetCommaEqualsStatement(DatabaseDriver, columnValues.Keys.ToArray()),
                    filter = SqlHelper.GetCombinedAndStatement(
                        DatabaseDriver,
                        SqlHelper.GetAndEqualsStatement(DatabaseDriver, keyColumnValues.Keys.ToArray()),
                        SqlHelper.GetIsNotNullStatement(DatabaseDriver, columnValues.Keys.ToArray()))
                },
                keyColumnValues.Concat(columnValues).Select(c => c.ToDbParameter(DatabaseDriver, Table.Columns)));

            if (await command.ExecuteNonQueryAsync(cancellationTokenSource.Token).ConfigureAwait(false) == 0)
            {
                return false;
            }

            return true;
        }
    }
}