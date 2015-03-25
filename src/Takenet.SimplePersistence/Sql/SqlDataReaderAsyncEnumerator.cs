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
    internal sealed class SqlDataReaderAsyncEnumerator<T> : IAsyncEnumerator<T>
    {
        private readonly SqlDataReader _sqlDataReader;
        private readonly IMapper<T> _mapper;
        private readonly string[] _selectColumns;

        public SqlDataReaderAsyncEnumerator(SqlDataReader sqlDataReader, IMapper<T> mapper, string[] selectColumns)
        {
            if (sqlDataReader == null) throw new ArgumentNullException(nameof(sqlDataReader));
            if (mapper == null) throw new ArgumentNullException(nameof(mapper));
            _sqlDataReader = sqlDataReader;
            _mapper = mapper;
            _selectColumns = selectColumns;
        }

        public bool MoveNext()
        {
            return _sqlDataReader.Read();
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        object IEnumerator.Current => Current;

        public T Current => _mapper.Create(_sqlDataReader, _selectColumns);

        public Task<bool> MoveNextAsync(CancellationToken cancellationToken)
        {
            return _sqlDataReader.ReadAsync(cancellationToken);
        }

        public void Dispose()
        {
            _sqlDataReader.Dispose();
        }
    }
}
