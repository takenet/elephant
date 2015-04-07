using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Sql.Mapping;

namespace Takenet.SimplePersistence.Sql
{
    internal sealed class SqlDataReaderAsyncEnumerable<T> : IAsyncEnumerable<T>, IDisposable
    {
        private readonly SqlCommand _sqlCommand;
        private readonly IMapper<T> _mapper;
        private readonly string[] _selectColumns;

        public SqlDataReaderAsyncEnumerable(SqlCommand sqlCommand, IMapper<T> mapper, string[] selectColumns)
        {
            _sqlCommand = sqlCommand;
            _mapper = mapper;
            _selectColumns = selectColumns;
        }

        public async Task<IAsyncEnumerator<T>> GetEnumeratorAsync(CancellationToken cancellationToken)
        {
            var reader = await _sqlCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            return new SqlDataReaderAsyncEnumerator<T>(reader, _mapper, _selectColumns);
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
