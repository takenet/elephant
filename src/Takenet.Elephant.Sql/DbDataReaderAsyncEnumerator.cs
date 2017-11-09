using System;
using System.Collections;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Sql
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

        public virtual bool MoveNext()
        {
            return _sqlDataReader.Read();
        }

        public virtual void Reset()
        {
            throw new NotSupportedException();
        }

        object IEnumerator.Current => Current;

        public T Current => _mapper.Create(_sqlDataReader, _selectColumns);

        public virtual Task<bool> MoveNextAsync(CancellationToken cancellationToken)
        {
            return _sqlDataReader.ReadAsync(cancellationToken);
        }

        public void Dispose()
        {
            _sqlDataReader.Dispose();
            _dbCommand.Dispose();
            _connection.Dispose();
        }
    }
}
