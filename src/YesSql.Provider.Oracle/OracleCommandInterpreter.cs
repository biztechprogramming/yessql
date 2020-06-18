using System.Text;
using YesSql.Sql;
using YesSql.Sql.Schema;

namespace YesSql.Provider.Oracle
{
    public class OracleCommandInterpreter : BaseCommandInterpreter
    {
        public OracleCommandInterpreter(ISqlDialect dialect) : base(dialect)
        {
        }
    }
}
