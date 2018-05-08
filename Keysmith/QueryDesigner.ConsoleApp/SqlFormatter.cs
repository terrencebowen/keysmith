namespace QueryDesigner.ConsoleApp
{
    public interface ISqlFormatter
    {
        string QuoteEncapsulate(string sql);
    }

    public class SqlFormatter : ISqlFormatter
    {
        private readonly ISqlParserFacade _sqlParserFacade;

        public SqlFormatter(ISqlParserFacade sqlParserFacade)
        {
            _sqlParserFacade = sqlParserFacade;
        }

        public string QuoteEncapsulate(string sql)
        {
            if (sql.Contains(" "))
            {
                return $"[{sql}]";
            }

            var isKeyword = _sqlParserFacade.IsKeyword(sql);
            var quoteEncapsulation = isKeyword
                ? $"[{sql}]"
                : sql;

            return quoteEncapsulation;
        }
    }
}