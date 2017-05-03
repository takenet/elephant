using System;
using System.Threading.Tasks;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Sql
{
    public class SqlNumberMap<TKey> : SqlMap<TKey, long>, INumberMap<TKey>
    {
        private readonly string _numberColumnName;

        public SqlNumberMap(
            string connectionString,
            ITable table,
            IMapper<TKey> keyMapper,
            string numberColumnName) : base(connectionString, table, keyMapper, new ValueMapper<long>(numberColumnName))
        {
            _numberColumnName = numberColumnName;
        }

        public SqlNumberMap(
            IDatabaseDriver databaseDriver,
            string connectionString,
            ITable table,
            IMapper<TKey> keyMapper,
            string numberColumnName) : base(databaseDriver, connectionString, table, keyMapper, new ValueMapper<long>(numberColumnName))
        {
            _numberColumnName = numberColumnName;
        }

        public Task<long> DecrementAsync(TKey key) => DecrementAsync(key, 1);

        public Task<long> DecrementAsync(TKey key, long value) => IncrementAsync(key, value * -1);

        public Task<long> IncrementAsync(TKey key) => IncrementAsync(key, 1);

        public async Task<long> IncrementAsync(TKey key, long value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            using (var cancellationTokenSource = CreateCancellationTokenSource())
            {
                using (var connection = await GetConnectionAsync(cancellationTokenSource.Token).ConfigureAwait(false))
                {
                    var keyColumnValues = KeyMapper.GetColumnValues(key);
                    var columnValues = GetColumnValues(value);
                    using (var command = connection.CreateMergeIncrementCommand(DatabaseDriver, Table.Name, _numberColumnName, keyColumnValues, columnValues))
                    {
                        return (long)await command.ExecuteScalarAsync(cancellationTokenSource.Token).ConfigureAwait(false);
                    }
                }
            }
        }
    }
}
