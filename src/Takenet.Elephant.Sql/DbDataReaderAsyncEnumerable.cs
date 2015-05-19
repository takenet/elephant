using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Sql
{
    internal sealed class DbDataReaderAsyncEnumerable<T> : IAsyncEnumerable<T>, IDisposable
    {
        private readonly DbCommand _sqlCommand;
        private readonly IMapper<T> _mapper;
        private readonly string[] _selectColumns;

        public DbDataReaderAsyncEnumerable(DbCommand sqlCommand, IMapper<T> mapper, string[] selectColumns)
        {
            _sqlCommand = sqlCommand;
            _mapper = mapper;
            _selectColumns = selectColumns;
        }

        public async Task<IAsyncEnumerator<T>> GetEnumeratorAsync(CancellationToken cancellationToken)
        {
            var reader = await _sqlCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            return new DbDataReaderAsyncEnumerator<T>(reader, _mapper, _selectColumns);
        }

        public IEnumerator<T> GetEnumerator()
        {
            return GetEnumeratorAsync(CancellationToken.None).Result;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Dispose()
        {            
            _sqlCommand.Dispose();
            _sqlCommand.Connection.Dispose();            
        }
    }
}
