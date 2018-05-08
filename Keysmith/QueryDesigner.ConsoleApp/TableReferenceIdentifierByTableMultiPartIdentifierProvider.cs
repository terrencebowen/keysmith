using Dapper;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;

namespace QueryDesigner.ConsoleApp
{
    public enum TableReferenceSpecificationKind
    {
        TableAlias,
        TableSource
    }

    public enum TableReferenceSpecificationScope
    {
        AliasAbbreviationIdentifier = 1,
        AliasTableIdentifier = 2,
        AliasSchemaIdentifier = 3,
        AliasDatabaseIdentifier = 4,
        AliasServerIdentifier = 5,
        TableSourceTableIdentifier = 6,
        TableSourceSchemaIdentifier = 7,
        TableSourceDatabaseIdentifier = 8,
        TableSourceServerIdentifier = 9
    }

    public interface ITableReferenceSpecification
    {
        TableReferenceSpecificationScope TableReferenceSpecificationScope { get; set; }
        TableReferenceSpecificationKind TableReferenceSpecificationKind { get; }
        ITable Table { get; }
        string DatabaseSchemaTableSource { get; set; }
        string SchemaTableSource { get; set; }
        string TableSource { get; set; }
        string TableAlias { get; set; }
    }

    public class TableReferenceSpecification : ITableReferenceSpecification
    {
        public TableReferenceSpecificationScope TableReferenceSpecificationScope { get; set; }
        public TableReferenceSpecificationKind TableReferenceSpecificationKind { get; }
        public ITable Table { get; }
        public string DatabaseSchemaTableSource { get; set; }
        public string SchemaTableSource { get; set; }
        public string TableSource { get; set; }
        public string TableAlias { get; set; }

        public TableReferenceSpecification(ITable table, TableReferenceSpecificationScope tableReferenceSpecificationScope, TableReferenceSpecificationKind tableReferenceSpecificationKind, string tableAlias)
        {
            DatabaseSchemaTableSource = $"{table.DatabaseName}.{table.SchemaName}.{table.TableName}".ToLower();
            SchemaTableSource = $"{table.SchemaName}.{table.TableName}".ToLower();
            Table = table;
            TableAlias = tableAlias;
            TableReferenceSpecificationKind = tableReferenceSpecificationKind;
            TableReferenceSpecificationScope = tableReferenceSpecificationScope;
            TableSource = $"{table.TableName}".ToLower();
        }
    }

    public interface ITableReferenceIdentifier
    {
        string TableAlias { get; }
        string TableSource { get; }
    }

    public class TableReferenceIdentifier : ITableReferenceIdentifier
    {
        public string TableAlias { get; }
        public string TableSource { get; }

        public TableReferenceIdentifier(string tableAlias, string tableSource)
        {
            TableAlias = tableAlias;
            TableSource = tableSource;
        }
    }

    public interface ITableReferenceIdentifierByTableMultiPartIdentifierProvider
    {
        IReadOnlyDictionary<string, ITableReferenceIdentifier> GetTableReferenceIdentifierByTableMultiPartIdentifier(IContextScope contextScope, IReadOnlyList<ITable> selectionTables, params ITableRelationship[] tableRelationships);
    }

    public class TableReferenceIdentifierByTableMultiPartIdentifierProvider : ITableReferenceIdentifierByTableMultiPartIdentifierProvider
    {
        private readonly Regex _predicateRegex;
        private readonly Regex _pascalCaseRegex;
        private readonly TextInfo _textInfo;
        private readonly ISqlFormatter _sqlFormatter;

        public TableReferenceIdentifierByTableMultiPartIdentifierProvider(ISqlFormatter sqlFormatter, Regex predicateRegex, Regex pascalCaseRegex, TextInfo textInfo)
        {
            _predicateRegex = predicateRegex;
            _pascalCaseRegex = pascalCaseRegex;
            _textInfo = textInfo;
            _sqlFormatter = sqlFormatter;
        }

        public IReadOnlyDictionary<string, ITableReferenceIdentifier> GetTableReferenceIdentifierByTableMultiPartIdentifier(IContextScope contextScope, IReadOnlyList<ITable> selectionTables, params ITableRelationship[] tableRelationships)
        {
            // TODO: The reason why you're unioning these here is because you definitely need all of the table relationships found, plus, you need the original tables you selected.  Find a better way to do this.
            var fromTables = tableRelationships.Select(tableRelationship => tableRelationship.FromTable);
            var toTables = tableRelationships.Select(tableRelationship => tableRelationship.ToTable);
            var tables = selectionTables.Union(fromTables).Union(toTables);
            var tableReferenceSpecifications = new List<ITableReferenceSpecification>();

            foreach (var table in tables)
            {
                var serverName = _sqlFormatter.QuoteEncapsulate(table.ServerName);
                var databaseName = _sqlFormatter.QuoteEncapsulate(table.DatabaseName);
                var schemaName = _sqlFormatter.QuoteEncapsulate(table.SchemaName);
                var tableName = _sqlFormatter.QuoteEncapsulate(table.TableName);

                var whitespaceDelimitation = _pascalCaseRegex.Replace(tableName, "$1 $2");
                var titleCasing = _textInfo.ToTitleCase(whitespaceDelimitation);
                var tableAlias = _predicateRegex.Replace(titleCasing, string.Empty);

                tableReferenceSpecifications.AddRange(new List<ITableReferenceSpecification>
                {
                    new TableReferenceSpecification(table, TableReferenceSpecificationScope.AliasAbbreviationIdentifier, TableReferenceSpecificationKind.TableAlias, tableAlias),
                    new TableReferenceSpecification(table, TableReferenceSpecificationScope.AliasTableIdentifier, TableReferenceSpecificationKind.TableAlias, tableName),
                    new TableReferenceSpecification(table, TableReferenceSpecificationScope.AliasSchemaIdentifier, TableReferenceSpecificationKind.TableAlias, $"{schemaName}_{tableName}"),
                    new TableReferenceSpecification(table, TableReferenceSpecificationScope.AliasDatabaseIdentifier, TableReferenceSpecificationKind.TableAlias, $"{databaseName}_{schemaName}_{tableName}"),
                    new TableReferenceSpecification(table, TableReferenceSpecificationScope.AliasServerIdentifier, TableReferenceSpecificationKind.TableAlias, $"{serverName.ToUpper()}_{databaseName}_{schemaName}_{tableName}"),
                    new TableReferenceSpecification(table, TableReferenceSpecificationScope.TableSourceTableIdentifier, TableReferenceSpecificationKind.TableSource,  table.SchemaName.ToLower() != "dbo" ? $"{table.SchemaName}.{table.TableName}" : tableName),
                    new TableReferenceSpecification(table, TableReferenceSpecificationScope.TableSourceSchemaIdentifier, TableReferenceSpecificationKind.TableSource, $"{schemaName}.{tableName}"),
                    new TableReferenceSpecification(table, TableReferenceSpecificationScope.TableSourceDatabaseIdentifier, TableReferenceSpecificationKind.TableSource, $"{databaseName}.{schemaName}.{tableName}"),
                    new TableReferenceSpecification(table, TableReferenceSpecificationScope.TableSourceServerIdentifier, TableReferenceSpecificationKind.TableSource, $"{serverName.ToUpper()}.{databaseName}.{schemaName}.{tableName}")
                });
            }

            RemoveTableReferenceSpecificationCollisions(contextScope, ref tableReferenceSpecifications);

            var tableReferenceIdentifierByTableMultiPartIdentifier = tableReferenceSpecifications.GroupBy(tableReferenceSpecification => tableReferenceSpecification.Table.MultiPartIdentifier)
                                                                                                 .ToDictionary(tableMultiPartIdentifier => tableMultiPartIdentifier.Key, grouping =>
                                                                                                 {
                                                                                                     var orderedEnumerable = grouping.OrderBy(tableReferenceSpecification => tableReferenceSpecification.TableReferenceSpecificationScope);
                                                                                                     var tableAlias = orderedEnumerable.First(tableReferenceSpecification => tableReferenceSpecification.TableReferenceSpecificationKind == TableReferenceSpecificationKind.TableAlias);
                                                                                                     var tableSource = orderedEnumerable.First(tableReferenceSpecification => tableReferenceSpecification.TableReferenceSpecificationKind == TableReferenceSpecificationKind.TableSource);

                                                                                                     return new TableReferenceIdentifier
                                                                                                     (
                                                                                                         tableAlias: tableAlias.TableAlias,
                                                                                                         tableSource: tableSource.TableAlias
                                                                                                     )
                                                                                                     as ITableReferenceIdentifier;
                                                                                                 });

            return tableReferenceIdentifierByTableMultiPartIdentifier;
        }

        public void RemoveTableReferenceSpecificationCollisions(IContextScope contextScope, ref List<ITableReferenceSpecification> tableReferenceSpecifications)
        {
            for (var index = 0; index < tableReferenceSpecifications.Count; index++)
            {
                var tableReferenceSpecification = tableReferenceSpecifications[index];

                var tableReferenceSpecificationCollisions = tableReferenceSpecifications.Where(parameter =>
                {
                    if (tableReferenceSpecification == parameter)
                    {
                        return false;
                    }

                    const TableReferenceSpecificationKind tableAlias = TableReferenceSpecificationKind.TableAlias;

                    var isTableAliasComparison = tableReferenceSpecification.TableReferenceSpecificationKind == tableAlias &&
                                                 parameter.TableReferenceSpecificationKind == tableAlias;

                    var isTableAliasSame = tableReferenceSpecification.TableAlias == parameter.TableAlias;
                    var isMostExplicitUniqueTableAliasScope = parameter.TableReferenceSpecificationScope == TableReferenceSpecificationScope.AliasServerIdentifier;

                    var isTableAliasCollision = isTableAliasComparison &&
                                                isTableAliasSame &&
                                                !isMostExplicitUniqueTableAliasScope;

                    const TableReferenceSpecificationKind tableSource = TableReferenceSpecificationKind.TableSource;

                    var isTableSourceComparison = tableReferenceSpecification.TableReferenceSpecificationKind == tableSource &&
                                                  parameter.TableReferenceSpecificationKind == tableSource;

                    var isTableSourceSame = tableReferenceSpecification.TableSource == parameter.TableSource ||
                                            tableReferenceSpecification.SchemaTableSource == parameter.SchemaTableSource ||
                                            tableReferenceSpecification.DatabaseSchemaTableSource == parameter.DatabaseSchemaTableSource;

                    var isTableSourceServerDifferent = !string.Equals(tableReferenceSpecification.Table.ServerName, contextScope.ServerName, StringComparison.CurrentCultureIgnoreCase);
                    var isMostExplicitUniqueTableSourceScope = parameter.TableReferenceSpecificationScope == TableReferenceSpecificationScope.TableSourceServerIdentifier;

                    var isTableSourceCollision = isTableSourceComparison &&
                                                 isTableSourceSame &&
                                                 isTableSourceServerDifferent &&
                                                 !isMostExplicitUniqueTableSourceScope;

                    var isTableReferenceSpecificationCollision = isTableAliasCollision ||
                                                                 isTableSourceCollision;

                    return isTableReferenceSpecificationCollision;
                })
                .AsList();

                if (!tableReferenceSpecificationCollisions.Any())
                {
                    continue;
                }

                tableReferenceSpecificationCollisions.Add(tableReferenceSpecification);

                foreach (var tableReferenceSpecificationCollision in tableReferenceSpecificationCollisions)
                {
                    tableReferenceSpecifications.Remove(tableReferenceSpecificationCollision);
                }

                index = 0;
            }
        }
    }
}