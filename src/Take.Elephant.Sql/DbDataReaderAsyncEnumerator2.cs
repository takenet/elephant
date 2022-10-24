using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    public class DbDataReaderAsyncEnumerator2<T> : IAsyncEnumerator<T>
    {
        private readonly Func<CancellationToken, Task<DbConnection>> _dbConnectionFactory;
        private readonly Func<DbConnection, DbCommand> _dbCommandFactory;
        private readonly IMapper<T> _mapper;
        private readonly string[] _selectColumns;
        private readonly CancellationToken _cancellationToken;

        private DbCommand _dbCommand;
        private DbDataReader _sqlDataReader;

        public DbDataReaderAsyncEnumerator2(
            Func<CancellationToken, Task<DbConnection>> dbConnectionFactory,
            Func<DbConnection, DbCommand> dbCommandFactory,
            IMapper<T> mapper,
            string[] selectColumns, 
            CancellationToken cancellationToken)
        {
            _dbConnectionFactory = dbConnectionFactory;
            _dbCommandFactory = dbCommandFactory;
            _mapper = mapper;
            _selectColumns = selectColumns;
            _cancellationToken = cancellationToken;
        }

        public T Current => _mapper.Create(_sqlDataReader, _selectColumns);

        public async ValueTask<bool> MoveNextAsync()
        {
            if (_sqlDataReader == null)
            {
                var dbConnection = await _dbConnectionFactory(_cancellationToken).ConfigureAwait(false);
                if (dbConnection.State == ConnectionState.Closed)
                    await dbConnection.OpenAsync(_cancellationToken).ConfigureAwait(false);
                _dbCommand = _dbCommandFactory(dbConnection);
                _sqlDataReader = await _dbCommand.ExecuteReaderAsync(_cancellationToken).ConfigureAwait(false);
            }

            return await _sqlDataReader.ReadAsync(_cancellationToken);
        }

        public async ValueTask DisposeAsync()
        {
            if (_sqlDataReader != null)
            {
                await _sqlDataReader.DisposeAsync().ConfigureAwait(false);
            }

            if (_dbCommand != null)
            {
                await _dbCommand.DisposeAsync().ConfigureAwait(false);
            }

        }
    }
}
