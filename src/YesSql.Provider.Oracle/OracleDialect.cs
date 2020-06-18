using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using YesSql.Sql;

namespace YesSql.Provider.Oracle
{
    public class OracleDialect : BaseDialect
    {
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

        public override bool SupportsIdentityColumns => false;
        public override string IdentitySelectString => "";

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
            return "\"" + columnName + "\"";
        }

        public override string QuoteForTableName(string tableName)
        {
            return "\"" + tableName + "\"";
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
    }
}
