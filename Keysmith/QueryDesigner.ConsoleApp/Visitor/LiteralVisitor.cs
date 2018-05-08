using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;

namespace QueryDesigner.ConsoleApp.Visitor
{
    public interface ILiteralVisitor
    {
        IList<Literal> Literals { get; }
    }

    public class LiteralVisitor : TSqlFragmentVisitor, ILiteralVisitor
    {
        public IList<Literal> Literals { get; }

        public LiteralVisitor()
        {
            Literals = new List<Literal>();
        }

        public override void Visit(Literal literal)
        {
            base.Visit(literal);

            Literals.Add(literal);
        }
    }
}