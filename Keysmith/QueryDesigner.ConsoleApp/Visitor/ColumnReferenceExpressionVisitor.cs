using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;

namespace QueryDesigner.ConsoleApp.Visitor
{
    public interface IColumnReferenceExpressionVisitor
    {
        IList<ColumnReferenceExpression> ColumnReferenceExpressions { get; }
    }

    public class ColumnReferenceExpressionVisitor : TSqlFragmentVisitor, IColumnReferenceExpressionVisitor
    {
        public IList<ColumnReferenceExpression> ColumnReferenceExpressions { get; }

        public ColumnReferenceExpressionVisitor()
        {
            ColumnReferenceExpressions = new List<ColumnReferenceExpression>();
        }

        public override void Visit(ColumnReferenceExpression columnReferenceExpression)
        {
            base.Visit(columnReferenceExpression);

            ColumnReferenceExpressions.Add(columnReferenceExpression);
        }
    }
}