using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql
{
    public class DbDataReaderAsyncEnumerable<T> : IAsyncEnumerable<T>
    {
        private readonly Func<CancellationToken, Task<DbConnection>> _dbConnectionFactory;
        private readonly Func<DbConnection, DbCommand> _dbCommandFactory;
        private readonly IMapper<T> _mapper;
        private readonly string[] _selectColumns;

        public DbDataReaderAsyncEnumerable(
            Func<CancellationToken, Task<DbConnection>> dbConnectionFactory, 
            Func<DbConnection, DbCommand> dbCommandFactory, 
            IMapper<T> mapper, 
            string[] selectColumns)
        {
            _dbConnectionFactory = dbConnectionFactory;
            _dbCommandFactory = dbCommandFactory;
            _mapper = mapper;
            _selectColumns = selectColumns;
        }

        public virtual async Task<IAsyncEnumerator<T>> GetEnumeratorAsync(CancellationToken cancellationToken)
        {
            var dbConnection = await _dbConnectionFactory(cancellationToken).ConfigureAwait(false);
            if (dbConnection.State == ConnectionState.Closed) await dbConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            var dbCommand = _dbCommandFactory(dbConnection);
            var dbReader = await dbCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            return new DbDataReaderAsyncEnumerator<T>(dbConnection, dbCommand, dbReader, _mapper, _selectColumns);
        }

        public virtual IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            // TODO: This is a blocking operation in a async context. How may we fix this?
            return GetEnumeratorAsync(cancellationToken).GetAwaiter().GetResult();
        }
    }
}
