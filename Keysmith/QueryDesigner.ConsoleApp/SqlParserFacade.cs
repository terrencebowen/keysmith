using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace QueryDesigner.ConsoleApp
{
    public interface ISqlParserFacade
    {
        TSqlFragment Parse(string transactSqlText, out IList<ParseError> parseErrors);
        bool IsKeyword(string tsqlText);
    }

    public class SqlParserFacade : ISqlParserFacade
    {
        private readonly TSql140Parser _tsql140Parser;

        public SqlParserFacade(TSql140Parser tsql140Parser)
        {
            _tsql140Parser = tsql140Parser;
        }

        public TSqlFragment Parse(string transactSqlText, out IList<ParseError> parseErrors)
        {
            TSqlFragment tsqlFragment;

            using (var stringReader = new StringReader(transactSqlText))
            {
                tsqlFragment = _tsql140Parser.Parse(stringReader, out parseErrors);

                if (parseErrors.Any())
                {
                    tsqlFragment = null;
                }
            }

            return tsqlFragment;
        }

        public bool IsKeyword(string tsqlText)
        {
            if (string.IsNullOrWhiteSpace(tsqlText))
            {
                return false;
            }

            TSqlFragment tsqlFragment;

            using (var stringReader = new StringReader(tsqlText))
            {
                tsqlFragment = _tsql140Parser.Parse(stringReader, out IList<ParseError> parseErrors);

                if (!parseErrors.Any())
                {
                    return false;
                }
            }

            var scriptToken = tsqlFragment.ScriptTokenStream.First();

            if (scriptToken == null)
            {
                return false;
            }

            var isKeyword = scriptToken.IsKeyword();

            return isKeyword;
        }
    }
}