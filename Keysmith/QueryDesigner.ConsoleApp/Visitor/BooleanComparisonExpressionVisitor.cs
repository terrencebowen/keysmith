using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;

namespace QueryDesigner.ConsoleApp.Visitor
{
    public interface IBooleanComparisonExpressionVisitor
    {
        IList<BooleanComparisonExpression> BooleanComparisonExpressions { get; }
    }

    public class BooleanComparisonExpressionVisitor : TSqlFragmentVisitor, IBooleanComparisonExpressionVisitor
    {
        public IList<BooleanComparisonExpression> BooleanComparisonExpressions { get; }

        public BooleanComparisonExpressionVisitor()
        {
            BooleanComparisonExpressions = new List<BooleanComparisonExpression>();
        }

        public override void Visit(BooleanComparisonExpression booleanComparisonExpression)
        {
            base.Visit(booleanComparisonExpression);

            BooleanComparisonExpressions.Add(booleanComparisonExpression);
        }
    }
}