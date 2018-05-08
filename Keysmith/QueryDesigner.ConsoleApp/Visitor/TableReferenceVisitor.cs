using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;

namespace QueryDesigner.ConsoleApp.Visitor
{
    public interface ITableReferenceVisitor
    {
        IList<TableReference> TableReferences { get; }
    }

    public class TableReferenceVisitor : TSqlFragmentVisitor, ITableReferenceVisitor
    {
        public IList<TableReference> TableReferences { get; }

        public TableReferenceVisitor()
        {
            TableReferences = new List<TableReference>();
        }

        public override void Visit(TableReference tableReference)
        {
            base.Visit(tableReference);

            TableReferences.Add(tableReference);
        }
    }
}