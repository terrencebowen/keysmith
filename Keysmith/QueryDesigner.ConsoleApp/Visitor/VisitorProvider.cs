using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;

namespace QueryDesigner.ConsoleApp.Visitor
{
    public interface IVisitorProvider
    {
        IEnumerable<BooleanComparisonExpression> GetBooleanComparisonExpressions(TSqlFragment tsqlFragment);
        IEnumerable<ColumnReferenceExpression> GetColumnReferenceExpressions(TSqlFragment tsqlFragment);
        IEnumerable<FromClause> GetFromClauses(TSqlFragment tsqlFragment);
        IEnumerable<Literal> GetLiteral(TSqlFragment tsqlFragment);
        IEnumerable<TableReference> GetTableReferences(TSqlFragment tsqlFragment);
    }

    public class VisitorProvider : IVisitorProvider
    {
        public IEnumerable<BooleanComparisonExpression> GetBooleanComparisonExpressions(TSqlFragment tsqlFragment)
        {
            var booleanComparisonExpressionVisitor = new BooleanComparisonExpressionVisitor();

            tsqlFragment.AcceptChildren(booleanComparisonExpressionVisitor);

            foreach (var booleanComparisonExpression in booleanComparisonExpressionVisitor.BooleanComparisonExpressions)
            {
                var columnReferenceExpressionVisitor = new ColumnReferenceExpressionVisitor();

                booleanComparisonExpression.AcceptChildren(columnReferenceExpressionVisitor);

                yield return booleanComparisonExpression;
            }
        }

        public IEnumerable<ColumnReferenceExpression> GetColumnReferenceExpressions(TSqlFragment tsqlFragment)
        {
            var columnReferenceExpressionVisitor = new ColumnReferenceExpressionVisitor();

            tsqlFragment.AcceptChildren(columnReferenceExpressionVisitor);

            foreach (var columnReferenceExpression in columnReferenceExpressionVisitor.ColumnReferenceExpressions)
            {
                yield return columnReferenceExpression;
            }
        }

        public IEnumerable<FromClause> GetFromClauses(TSqlFragment tsqlFragment)
        {
            var fromClauseVisitor = new FromClauseVisitor();

            tsqlFragment.AcceptChildren(fromClauseVisitor);

            foreach (var fromClause in fromClauseVisitor.FromClauses)
            {
                yield return fromClause;
            }
        }

        public IEnumerable<Literal> GetLiteral(TSqlFragment tsqlFragment)
        {
            var literalVisitor = new LiteralVisitor();

            tsqlFragment.AcceptChildren(literalVisitor);

            foreach (var literal in literalVisitor.Literals)
            {
                yield return literal;
            }
        }

        public IEnumerable<TableReference> GetTableReferences(TSqlFragment tsqlFragment)
        {
            var tableReferenceVisitor = new TableReferenceVisitor();

            tsqlFragment.AcceptChildren(tableReferenceVisitor);

            foreach (var tableReference in tableReferenceVisitor.TableReferences)
            {
                yield return tableReference;
            }
        }
    }
}