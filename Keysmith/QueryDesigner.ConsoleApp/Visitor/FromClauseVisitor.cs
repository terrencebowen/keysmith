using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;

namespace QueryDesigner.ConsoleApp.Visitor
{
    public interface IFromClauseVisitor
    {
        IList<FromClause> FromClauses { get; }
    }

    public class FromClauseVisitor : TSqlFragmentVisitor, IFromClauseVisitor
    {
        public IList<FromClause> FromClauses { get; }

        public FromClauseVisitor()
        {
            FromClauses = new List<FromClause>();
        }

        public override void Visit(FromClause fromClause)
        {
            base.Visit(fromClause);

            FromClauses.Add(fromClause);
        }
    }
}