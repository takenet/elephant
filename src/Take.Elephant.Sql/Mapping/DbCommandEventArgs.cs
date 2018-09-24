using System;
using System.Data.Common;

namespace Take.Elephant.Sql.Mapping
{
    public class DbCommandEventArgs : EventArgs
    {
        public DbCommandEventArgs(DbCommand dbCommand)
        {
            DbCommand = dbCommand;
        }

        public DbCommand DbCommand { get; }
    }
}
