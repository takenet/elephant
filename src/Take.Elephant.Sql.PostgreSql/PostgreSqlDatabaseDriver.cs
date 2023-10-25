﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Npgsql;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Sql.PostgreSql
{
    /// <summary>
    /// Implementation of the PostgreSQL database driver.
    /// <see cref="http://www.sqlines.com/sql-server-to-postgresql"/>
    /// <see cref="http://www.postgresql.org/docs/9.4/static/index.html"/>
    /// </summary>
    public class PostgreSqlDatabaseDriver : IDatabaseDriver
    {
        private static HashSet<string> ReserverdKeywords;

        static PostgreSqlDatabaseDriver()
        {
            ReserverdKeywords = new HashSet<string>(
                PostgreSqlTemplates
                    .ReservedKeywords
                    .Split('\n')
                    .Select(k => k.TrimEnd('\r', '\n').ToLowerInvariant()));
        }

        public TimeSpan Timeout => TimeSpan.FromSeconds(180);

        public string DefaultSchema => "public";

        public DbConnection CreateConnection(string connectionString)
        {
            return new NpgsqlConnection(connectionString);
        }

        public DbParameter CreateParameter(string parameterName, object value)
        {
            return new NpgsqlParameter()
            {
                ParameterName = parameterName,
                Value = value
            };
        }

        public DbParameter CreateParameter(string parameterName, object value, SqlType sqlType)
        {
            return new NpgsqlParameter(parameterName, sqlType.Type)
            {
                Value = value,
                IsNullable = sqlType.IsNullable ?? value.IsNullable(),
                Size = sqlType.Length != null ? sqlType.Length.Value : 0
            };
        }

        public string ParseParameterName(string parameterName) => $"@{parameterName}";

        public string ParseIdentifier(string identifier)
        {
            return $"\"{identifier}\"";
        }

        public string GetSqlStatementTemplate(SqlStatement sqlStatement)
        {
            return PostgreSqlTemplates.ResourceManager.GetString(sqlStatement.ToString());
        }

        public string GetSqlTypeName(DbType dbType)
        {
            return PostgreSqlTemplates.ResourceManager.GetString($"DbType{dbType}");
        }
    }
}