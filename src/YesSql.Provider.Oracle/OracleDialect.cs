using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Data.Common;
using System.Linq;
using Dapper;
using Dapper.Oracle;
using Oracle.ManagedDataAccess.Client;
using YesSql.Indexes;
using YesSql.Sql;
using YesSql.Sql.Schema;

namespace YesSql.Provider.Oracle
{
    public class OracleDialect : BaseDialect
    {
        private const string defaultValueInsertStringForReplace = "defaultValueInsertStringForReplace";
        private static readonly ConcurrentDictionary<string, IEnumerable<TableColumnInfo>> tableColumnInfoCache = new ConcurrentDictionary<string, IEnumerable<TableColumnInfo>>();
        private static readonly ConcurrentDictionary<string, int> defaultCountCache = new ConcurrentDictionary<string, int>();
        private static Dictionary<DbType, string> ColumnTypes = new Dictionary<DbType, string>
        {
            {DbType.Guid, "RAW(16)"},
            {DbType.Binary, "RAW"},
            {DbType.Time, "DATE"},
            {DbType.Date, "DATE"},
            {DbType.DateTime, "TIMESTAMP" },
            {DbType.DateTime2, "TIMESTAMP" },
            {DbType.DateTimeOffset, "NUMBER(25)" },
            {DbType.Boolean, "NUMBER(1,0)"},
            {DbType.Byte, "NUMBER(3,0)"},
            {DbType.Currency, "NUMBER(19,4)"},
            {DbType.Decimal, "NUMBER(25)"},
            {DbType.Double, "NUMBER(25)"},
            {DbType.Int16, "NUMBER(5)"},
            {DbType.UInt16, "NUMBER(5)"},
            {DbType.Int32, "NUMBER(10)"},
            {DbType.UInt32, "NUMBER(10)"},
            {DbType.Int64, "NUMBER(20)"},
            {DbType.UInt64, "NUMBER(20)"},
            {DbType.Single, "FLOAT(49)"},
            {DbType.AnsiStringFixedLength, "VARCHAR2(255)"},
            {DbType.AnsiString, "VARCHAR(255)"},
            {DbType.StringFixedLength, "VARCHAR2(255)"},
            {DbType.String, "VARCHAR2(255)"},
        };

        private static readonly string[] UnsafeParameters =
        {
            "Order",
            "Date",
            "Version"
        };
        private static readonly string safeParameterSuffix = "Safe";

        public OracleDialect()
        {
            Methods.Add("second", new TemplateFunction("extract(second from to_timestamp(to_char({0}, 'DD-MON-YY HH:MI:SS')))"));
            Methods.Add("minute", new TemplateFunction("extract(minute from to_timestamp(to_char({0}, 'DD-MON-YY HH:MI:SS')))"));
            Methods.Add("hour", new TemplateFunction("extract(hour from to_timestamp(to_char({0}, 'DD-MON-YY HH:MI:SS')))"));

            Methods.Add("day", new TemplateFunction("extract(day from {0})"));
            Methods.Add("month", new TemplateFunction("extract(month from {0})"));
            Methods.Add("year", new TemplateFunction("extract(year from {0})"));
        }

        public override string Name => "Oracle";
        public override bool IsSpecialDistinctRequired => true;

        public override bool SupportsIdentityColumns => false;
        public override string IdentitySelectString => "";
        public override string ParameterNamePrefix => ":";
        public override string StatementEnd => "";

        public override string GetTypeName(DbType dbType, int? length, byte precision, byte scale)
        {
            if (length.HasValue)
            {
                if (length.Value > 4000)
                {
                    if (dbType == DbType.String)
                    {
                        return "CLOB";
                    }

                    if (dbType == DbType.AnsiString)
                    {
                        return "CLOB";
                    }

                    if (dbType == DbType.Binary)
                    {
                        return "BLOB";
                    }
                }
                else
                {
                    if (dbType == DbType.String)
                    {
                        return "VARCHAR2(" + length + ")";
                    }

                    if (dbType == DbType.AnsiString)
                    {
                        return "VARCHAR2(" + length + ")";
                    }

                    if (dbType == DbType.Binary)
                    {
                        return "BLOB";
                    }
                }
            }

            if (ColumnTypes.TryGetValue(dbType, out string value))
            {
                return value;
            }

            throw new Exception("DbType not found for: " + dbType);
        }

        public override string DefaultValuesInsert => defaultValueInsertStringForReplace;

        public override void Page(ISqlBuilder sqlBuilder, string offset, string limit)
        {
            if (!string.IsNullOrWhiteSpace(offset))
            {
                sqlBuilder.Trail(" OFFSET ");
                sqlBuilder.Trail(offset);
                sqlBuilder.Trail(" ROWS");

            }
            if (!string.IsNullOrWhiteSpace(limit))
            {
                sqlBuilder.Trail(" FETCH NEXT ");
                sqlBuilder.Trail(limit);
                sqlBuilder.Trail(" ROWS ONLY");
            }
        }

        public override string GetDropIndexString(string indexName, string tableName)
        {
            return "drop index " + QuoteForColumnName(indexName);
        }

        public override string QuoteForColumnName(string columnName)
        {
            return QuoteString + columnName + QuoteString;
        }

        public override string QuoteForTableName(string tableName)
        {
            return QuoteString + tableName + QuoteString;
        }

        public override void Concat(StringBuilder builder, params Action<StringBuilder>[] generators)
        {
            builder.Append("(");

            for (var i = 0; i < generators.Length; i++)
            {
                if (i > 0)
                {
                    builder.Append(" || ");
                }

                generators[i](builder);
            }

            builder.Append(")");
        }

        private static string InsertDefaultValues(DbConnection connection, string insertSql, DbTransaction transaction)
        {
            var firstIndex = "INSERT INTO \"".Count();
            var lenghtTableName = insertSql.IndexOf("\"", firstIndex + 1) - firstIndex;
            var tableName = insertSql.Substring(firstIndex, lenghtTableName);
            if (!defaultCountCache.TryGetValue(tableName, out var count))
            {
                var sqlForDataTypes = $"SELECT COUNT(column_name) as \"ColumnName\"" +
                        $" FROM all_tab_columns where table_name = \'{tableName}\'";
                count = connection.ExecuteScalar<int>(sqlForDataTypes, transaction: transaction);
                defaultCountCache.TryAdd(tableName, count);
            }
            var defaultValues = $"VALUES({String.Join(",", Enumerable.Repeat("DEFAULT", count))})";
            insertSql = insertSql.Replace(defaultValueInsertStringForReplace, defaultValues);
            return insertSql;
        }

        public override object GetDynamicParameters(DbConnection connection, object parameters, string tableName)
        {
            return GetOracleDynamicParameters(connection, parameters, tableName);
        }

        public override object GetSafeIndexParameters(IIndex index)
        {
            return GetSafeParameters(index);
        }

        private OracleDynamicParameters GetOracleDynamicParameters(DbConnection connection, object parameters, string tableName)
        {
            if (!tableColumnInfoCache.TryGetValue(tableName, out var dataTypes))
            {
                var sqlForDataTypes = $"SELECT column_name as \"ColumnName\", data_type as \"DataType\" " +
                             $" FROM all_tab_columns where table_name = \'{tableName}\'";
                dataTypes = connection.Query<TableColumnInfo>(sqlForDataTypes);
                tableColumnInfoCache.TryAdd(tableName, dataTypes);
            }
            var type = parameters.GetType();
            var result = new OracleDynamicParameters();
            foreach (var property in type.GetProperties())
            {
                var value = property.GetValue(parameters);
                var tableColumnInfo = dataTypes.FirstOrDefault(dt => dt.ColumnName == property.Name);
                if (tableColumnInfo != null)
                {
                    var propertyName = property.Name;
                    if (UnsafeParameters.Contains(propertyName, StringComparer.InvariantCultureIgnoreCase))
                    {
                        propertyName += safeParameterSuffix;
                    }
                    result.Add(propertyName, value, tableColumnInfo.OracleMappingType);
                }
            }

            return result;
        }

        private bool IsContainSafeParameters(string sql)
        {
            return UnsafeParameters.Any(parameter => sql.Contains(ParameterNamePrefix + parameter + safeParameterSuffix));
        }

        private DynamicParameters GetSafeParameters(IIndex index)
        {
            var parameters = new DynamicParameters();
            var type = index.GetType();
            foreach (var property in type.GetProperties())
            {
                var value = property.GetValue(index);
                var propertyName = property.Name;
                if (UnsafeParameters.Contains(propertyName, StringComparer.InvariantCultureIgnoreCase))
                {
                    propertyName += safeParameterSuffix;
                }
                var dbType = SchemaUtils.ToDbType(property.PropertyType);
                if (property.PropertyType == typeof(bool))
                {
                    dbType = DbType.Int32;
                }

                parameters.Add(propertyName, value, dbType);
            }
            return parameters;
        }
    }
}
