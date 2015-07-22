using System;
using System.Collections;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Sql
{
    internal sealed class DbDataReaderAsyncEnumerator<T> : IAsyncEnumerator<T>
    {
        private readonly DbCommand _dbCommand;
        private readonly DbDataReader _sqlDataReader;
        private readonly IMapper<T> _mapper;
        private readonly string[] _selectColumns;

        public DbDataReaderAsyncEnumerator(DbCommand dbCommand, DbDataReader sqlDataReader, IMapper<T> mapper, string[] selectColumns)
        {
            if (sqlDataReader == null) throw new ArgumentNullException(nameof(sqlDataReader));
            if (mapper == null) throw new ArgumentNullException(nameof(mapper));
            _dbCommand = dbCommand;
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
            _dbCommand.Dispose();
            _dbCommand.Connection.Dispose();
        }
    }
}
