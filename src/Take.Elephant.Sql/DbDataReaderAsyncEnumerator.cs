using System;
using System.Collections.Generic;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    public class DbDataReaderAsyncEnumerator<T> : IAsyncEnumerator<T>
    {
        private readonly DbConnection _connection;
        private readonly DbCommand _dbCommand;
        private readonly DbDataReader _sqlDataReader;
        private readonly IMapper<T> _mapper;
        private readonly string[] _selectColumns;

        public DbDataReaderAsyncEnumerator(DbConnection connection, DbCommand dbCommand, DbDataReader sqlDataReader, IMapper<T> mapper, string[] selectColumns)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _dbCommand = dbCommand ?? throw new ArgumentNullException(nameof(dbCommand));
            _sqlDataReader = sqlDataReader ?? throw new ArgumentNullException(nameof(sqlDataReader));
            _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            _selectColumns = selectColumns;
        }

        public T Current => _mapper.Create(_sqlDataReader, _selectColumns);

        public ValueTask<bool> MoveNextAsync()
        {
            return new ValueTask<bool>(_sqlDataReader.ReadAsync(CancellationToken.None));
        }

        public async ValueTask DisposeAsync()
        {
            await _sqlDataReader.DisposeAsync().ConfigureAwait(false);
            await _dbCommand.DisposeAsync().ConfigureAwait(false);
            await _connection.DisposeAsync().ConfigureAwait(false);
        }
    }
}
