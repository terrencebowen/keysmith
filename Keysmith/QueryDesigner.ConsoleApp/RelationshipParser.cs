using Dapper;
using LibGit2Sharp;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using QueryDesigner.ConsoleApp.Visitor;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace QueryDesigner.ConsoleApp
{
    public interface IRelationshipParser
    {
        IEnumerable<IColumnRelationship> ParseRelationships(string dataDirectoryPath);
    }

    public class RelationshipParser : IRelationshipParser
    {
        private readonly ISqlParserFacade _sqlParserFacade;
        private readonly IReadOnlyCollection<IServer> _servers;
        private readonly IReadOnlyDictionary<string, IMetadataIdentifier> _metadataIdentifierByMultiPartIdentifier;
        private readonly IVisitorProvider _visitorProvider;

        public RelationshipParser(ISqlParserFacade sqlParserFacade, IVisitorProvider visitorProvider, IReadOnlyCollection<IServer> servers, IReadOnlyDictionary<string, IMetadataIdentifier> metadataIdentifierByMultiPartIdentifier)
        {
            _metadataIdentifierByMultiPartIdentifier = metadataIdentifierByMultiPartIdentifier;
            _servers = servers;
            _sqlParserFacade = sqlParserFacade;
            _visitorProvider = visitorProvider;
        }

        public IEnumerable<IColumnRelationship> ParseRelationships(string dataDirectoryPath)
        {
            foreach (var columnRelationship in Parse(dataDirectoryPath))
            {
                // TODO: Do you want to parse out the actual base table references here into a dictionary so you can query the actual weight for it later??? use relationship origin to determine if it came from a view
                yield return new ColumnRelationship(columnRelationship.FromColumn, columnRelationship.ToColumn, columnRelationship.RelationshipOrigin);
                yield return new ColumnRelationship(columnRelationship.ToColumn, columnRelationship.FromColumn, columnRelationship.RelationshipOrigin);
            }
        }

        public IEnumerable<(IColumn FromColumn, IColumn ToColumn, RelationshipOrigin RelationshipOrigin)> Parse(string dataDirectoryPath)
        {
            var columnRelationships = new List<(IColumn FromColumn, IColumn ToColumn, RelationshipOrigin RelationshipOrigin)>();

            columnRelationships.AddRange(GetColumnRelationshipsWithKeyReferenceDefinition(_servers, RelationshipOrigin.KeyReferenceDefinition));
            columnRelationships.AddRange(GetColumnRelationshipsWithSqlModuleDefinition(_servers, RelationshipOrigin.SqlModuleDefinition));
            columnRelationships.AddRange(GetColumnRelationshipsWithChangeScriptReference(dataDirectoryPath, RelationshipOrigin.ChangeScriptReference));
            columnRelationships.AddRange(GetColumnRelationshipsByDatabaseStructuralCloneReference(_servers, RelationshipOrigin.DatabaseStructuralCloneReference));

            foreach (var columnRelationship in columnRelationships)
            {
                yield return columnRelationship;
            }
        }

        public IEnumerable<(IColumn FromColumn, IColumn ToColumn, RelationshipOrigin RelationshipOrigin)> GetColumnRelationshipsByDatabaseStructuralCloneReference(IEnumerable<IServer> servers, RelationshipOrigin relationshipOrigin)
        {
            yield break;
        }

        public IEnumerable<(IColumn FromColumn, IColumn ToColumn, RelationshipOrigin RelationshipOrigin)> GetColumnRelationshipsWithChangeScriptReference(string dataDirectoryPath, RelationshipOrigin relationshipOrigin)
        {
            var dataDirectoryUri = new Uri(dataDirectoryPath);

            if (!Directory.Exists(dataDirectoryUri.LocalPath))
            {
                yield break;
            }

            var repositoryPath = Repository.Discover(dataDirectoryPath);

            if (string.IsNullOrEmpty(repositoryPath))
            {
                yield break;
            }

            using (var repository = new Repository(repositoryPath))
            {
                var contextualServerName = dataDirectoryUri.Host.ToLower();
                var head = repository.Head;
                var tip = head.Tip;
                var tree = tip.Tree;

                foreach (var databaseNameWithFileContent in GetDatabaseNameWithFileContent(contextualServerName, tree))
                {
                    var transactSqlText = databaseNameWithFileContent.FileContent;
                    var databaseName = databaseNameWithFileContent.DatabaseName;

                    foreach (var columnRelationship in GetColumnRelationshipsByChangeScriptDefinition(transactSqlText, contextualServerName, databaseName, relationshipOrigin))
                    {
                        yield return (columnRelationship.FromColumn, columnRelationship.ToColumn, relationshipOrigin);
                    }
                }
            }
        }

        public IEnumerable<(string DatabaseName, string FileContent)> GetDatabaseNameWithFileContent(string contextualServerName, Tree tree)
        {
            foreach (var treeEntry in tree)
            {
                switch (treeEntry.TargetType)
                {
                    case TreeEntryTargetType.Blob:
                        var filePath = treeEntry.Path;
                        var filePathSegments = filePath.Split('\\');

                        if (filePathSegments.Length <= 1)
                        {
                            continue;
                        }

                        var databaseName = filePathSegments[1].ToLower();
                        var databaseMultiPartIdentifier = $"{contextualServerName}.{databaseName}";
                        var isKnownDatabaseMultiPartIdentifier = _metadataIdentifierByMultiPartIdentifier.ContainsKey(databaseMultiPartIdentifier);

                        if (!isKnownDatabaseMultiPartIdentifier)
                        {
                            continue;
                        }

                        var blob = (Blob)treeEntry.Target;
                        var contentText = blob.GetContentText();

                        yield return (databaseName, contentText);
                        break;

                    case TreeEntryTargetType.Tree:
                        foreach (var databaseNameWithFileContent in GetDatabaseNameWithFileContent(contextualServerName, (Tree)treeEntry.Target))
                        {
                            yield return databaseNameWithFileContent;
                        }
                        break;

                    case TreeEntryTargetType.GitLink:
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }

        public IEnumerable<IColumnRelationship> GetColumnRelationshipsByChangeScriptDefinition(string sql, string contextualServerName, string contextualDatabaseName, RelationshipOrigin relationshipOrigin)
        {
            sql = sql.ToLower();
            contextualServerName = contextualServerName.ToLower();
            contextualDatabaseName = contextualDatabaseName.ToLower();

            var tsqlScript = (TSqlScript)_sqlParserFacade.Parse(sql, out IList<ParseError> parseErrors);

            if (parseErrors.Any())
            {
                yield break;
            }

            var booleanComparisonExpressionIdentifiers = new List<int>();

            foreach (var tsqlBatch in tsqlScript.Batches)
            {
                for (var index = 0; index < tsqlBatch.Statements.Count; index++)
                {
                    var tsqlStatement = tsqlBatch.Statements[index];

                    switch (tsqlStatement)
                    {
                        case PrintStatement printStatement:
                            continue;
                        case ExecuteStatement executeStatement:
                            foreach (var literal in _visitorProvider.GetLiteral(executeStatement))
                            {
                                if (literal.LiteralType != LiteralType.String)
                                {
                                    continue;
                                }

                                var nestedTSqlScript = (TSqlScript)_sqlParserFacade.Parse(literal.Value, out IList<ParseError> nestedParseErrors);

                                if (nestedParseErrors.Any())
                                {
                                    continue;
                                }

                                foreach (var nestedTSqlBatch in nestedTSqlScript.Batches)
                                {
                                    foreach (var nestedTsqlStatements in nestedTSqlBatch.Statements)
                                    {
                                        tsqlBatch.Statements.Insert(index + 1, nestedTsqlStatements);
                                    }
                                }
                            }
                            break;

                        case UseStatement useStatement:
                            var usingDatabaseName = useStatement.DatabaseName.Value.ToLower();

                            if (usingDatabaseName != "master" && usingDatabaseName != contextualDatabaseName)
                            {
                                contextualDatabaseName = usingDatabaseName;
                            }
                            continue;
                    }

                    foreach (var columnRelationship in ParseFromClauseRelationships(tsqlStatement, contextualServerName, contextualDatabaseName, booleanComparisonExpressionIdentifiers, relationshipOrigin))
                    {
                        yield return columnRelationship;
                    }

                    foreach (var columnRelationship in ParseViewTableColumnToBaseTableColumnRelationships(tsqlStatement, contextualServerName, contextualDatabaseName, relationshipOrigin))
                    {
                        yield return columnRelationship;
                    }
                }
            }
        }

        public IEnumerable<IColumnRelationship> ParseFromClauseRelationships(TSqlStatement tsqlStatement, string contextualServerName, string contextualDatabaseName, IList<int> booleanComparisonExpressionIdentifiers, RelationshipOrigin relationshipOrigin)
        {
            foreach (var fromClause in _visitorProvider.GetFromClauses(tsqlStatement))
            {
                if (!IsParsableFromClause(fromClause))
                {
                    continue;
                }

                if (!TryParseTableByAliasIdentifier(fromClause, contextualServerName, contextualDatabaseName, out IDictionary<string, ITable> tableByAliasIdentifier))
                {
                    continue;
                }

                foreach (var booleanComparisonExpression in _visitorProvider.GetBooleanComparisonExpressions(fromClause))
                {
                    foreach (var columnRelationship in ParseBooleanComparisonExpression(booleanComparisonExpression, tableByAliasIdentifier, booleanComparisonExpressionIdentifiers, relationshipOrigin))
                    {
                        if (columnRelationship.FromColumn.Table.MultiPartIdentifier == columnRelationship.ToColumn.Table.MultiPartIdentifier)
                        {
                            continue;
                        }

                        yield return columnRelationship;
                    }
                }
            }
        }

        public IEnumerable<IColumnRelationship> ParseViewTableColumnToBaseTableColumnRelationships(TSqlStatement tsqlStatement, string contextualServerName, string contextualDatabaseName, RelationshipOrigin relationshipOrigin)
        {
            var viewStatementBody = tsqlStatement as ViewStatementBody;

            if (viewStatementBody == null)
            {
                yield break;
            }

            if (!TryParseTable(contextualServerName, contextualDatabaseName, viewStatementBody.SchemaObjectName, out ITable viewTable))
            {
                yield break;
            }

            var selectStatement = viewStatementBody.SelectStatement;
            var queryExpression = selectStatement.QueryExpression;
            var querySpecification = queryExpression as QuerySpecification;

            if (!(queryExpression is QuerySpecification))
            {
                yield break;
            }

            var selectElements = querySpecification.SelectElements;

            if (viewTable.Columns.Count != selectElements.Count)
            {
                yield break;
            }

            var viewStatementFromClause = querySpecification.FromClause;

            if (!TryParseTableByAliasIdentifier(viewStatementFromClause, contextualServerName, contextualDatabaseName, out IDictionary<string, ITable> viewTableByAliasIdentifier))
            {
                yield break;
            }

            for (var index = 0; index < selectElements.Count; index++)
            {
                var selectElement = selectElements[index];
                var selectScalarExpression = (SelectScalarExpression)selectElement;
                var scalarExpression = selectScalarExpression.Expression;
                var columnReferenceExpression = scalarExpression as ColumnReferenceExpression;

                if (!(scalarExpression is ColumnReferenceExpression))
                {
                    continue;
                }

                var viewColumns = viewTable.Columns.AsList();
                var viewTableColumn = viewColumns[index];

                if (!TryParseColumn(columnReferenceExpression, viewTableByAliasIdentifier, out IColumn baseTableColumn))
                {
                    continue;
                }

                var columnRelationship = new ColumnRelationship(viewTableColumn, baseTableColumn, relationshipOrigin);

                yield return columnRelationship;
            }
        }

        public bool TryParseTableByAliasIdentifier(TSqlFragment tsqlFragment, string contextualServerName, string contextualDatabaseName, out IDictionary<string, ITable> tableByAliasIdentifier)
        {
            tableByAliasIdentifier = new Dictionary<string, ITable>();

            foreach (var tableReference in _visitorProvider.GetTableReferences(tsqlFragment))
            {
                foreach (var tableIdentifier in GetTableIdentifiers(tableReference))
                {
                    if (!TryParseTable(contextualServerName, contextualDatabaseName, tableIdentifier.SchemaObjectName, out ITable table))
                    {
                        continue;
                    }

                    var alias = tableIdentifier.Alias;
                    var tableAliasIdentifier = alias?.Value ?? table.TableName.ToLower();
                    var isKeyValuePairAssigned = tableByAliasIdentifier.ContainsKey(tableAliasIdentifier) && tableByAliasIdentifier[tableAliasIdentifier].MultiPartIdentifier == table.MultiPartIdentifier;

                    if (isKeyValuePairAssigned)
                    {
                        continue;
                    }

                    tableByAliasIdentifier.Add(tableAliasIdentifier, table);
                }
            }

            if (!tableByAliasIdentifier.Any())
            {
                return false;
            }

            var columns = tableByAliasIdentifier.Values.SelectMany(table => table.Columns);
            var columnsHavingUniqueColumnNames = columns.GroupBy(column => column.ColumnName).Where(grouping => grouping.Count() == 1).Select(grouping => grouping.Single());

            foreach (var columnsHavingUniqueColumnName in columnsHavingUniqueColumnNames)
            {
                var table = columnsHavingUniqueColumnName.Table;

                foreach (var identifier in new List<string>
                {
                    $"{table.ServerName}.{table.DatabaseName}.{table.SchemaName}.{table.TableName}",
                    $"{table.DatabaseName}.{table.SchemaName}.{table.TableName}",
                    $"{table.SchemaName}.{table.TableName}",
                    $"{table.TableName}",
                    columnsHavingUniqueColumnName.ColumnName
                })
                {
                    var aliasIdentifier = identifier.ToLower();

                    if (tableByAliasIdentifier.ContainsKey(aliasIdentifier))
                    {
                        continue;
                    }

                    tableByAliasIdentifier.Add(aliasIdentifier, table);
                }
            }

            return true;
        }

        public bool IsParsableFromClause(FromClause tsqlFragment)
        {
            var tsqlParserTokens = tsqlFragment.ScriptTokenStream.AsList();
            var firstTokenIndex = tsqlFragment.FirstTokenIndex;
            var lastTokenIndex = tsqlFragment.LastTokenIndex - firstTokenIndex + 1;
            var range = tsqlParserTokens.GetRange(firstTokenIndex, lastTokenIndex);
            var count = range.Count(tsqlParserToken => tsqlParserToken.TokenType == TSqlTokenType.From);
            var isParsableFromClause = count == 1;

            return isParsableFromClause;
        }

        public bool TryParseTable(string contextualServerName, string contextualDatabaseName, SchemaObjectName schemaObjectName, out ITable table)
        {
            table = null;

            if (schemaObjectName == null)
            {
                return false;
            }

            var identifiers = schemaObjectName.Identifiers;
            var values = identifiers.Select(identifier => identifier.Value).AsList();
            var multiPartIdentifier = string.Join(".", values);
            var tableName = values.Last();

            if (tableName.StartsWith("#"))
            {
                return false;
            }

            string tableMultiPartIdentifier;

            switch ((TableIdentifierScope)identifiers.Count)
            {
                case TableIdentifierScope.Server:
                    tableMultiPartIdentifier = multiPartIdentifier;
                    break;

                case TableIdentifierScope.Database:
                    tableMultiPartIdentifier = $"{contextualServerName}.{multiPartIdentifier}";
                    break;

                case TableIdentifierScope.Schema:
                    tableMultiPartIdentifier = $"{contextualServerName}.{contextualDatabaseName}.{multiPartIdentifier}";
                    break;

                case TableIdentifierScope.Table:
                    tableMultiPartIdentifier = $"{contextualServerName}.{contextualDatabaseName}.{schemaObjectName.SchemaIdentifier?.Value ?? "dbo"}.{multiPartIdentifier}";
                    break;

                default:
                    throw new ArgumentOutOfRangeException();
            }

            if (!_metadataIdentifierByMultiPartIdentifier.ContainsKey(tableMultiPartIdentifier))
            {
                return false;
            }

            table = (ITable)_metadataIdentifierByMultiPartIdentifier[tableMultiPartIdentifier];

            return true;
        }

        public IEnumerable<IColumnRelationship> ParseBooleanComparisonExpression(BooleanComparisonExpression booleanComparisonExpression, IDictionary<string, ITable> tableByAliasIdentifier, IList<int> booleanComparisonExpressionIdentifiers, RelationshipOrigin relationshipOrigin)
        {
            if (booleanComparisonExpression.ComparisonType == BooleanComparisonType.NotEqualToBrackets || booleanComparisonExpression.ComparisonType == BooleanComparisonType.NotEqualToExclamation)
            {
                yield break;
            }

            var columnReferenceExpressions = _visitorProvider.GetColumnReferenceExpressions(booleanComparisonExpression).AsList();

            if (columnReferenceExpressions.Count != 2)
            {
                yield break;
            }

            var fromColumnReferenceExpression = columnReferenceExpressions[0];

            if (!TryParseColumn(fromColumnReferenceExpression, tableByAliasIdentifier, out IColumn fromColumn))
            {
                yield break;
            }

            var toColumnReferenceExpression = columnReferenceExpressions[1];

            if (!TryParseColumn(toColumnReferenceExpression, tableByAliasIdentifier, out IColumn toColumn))
            {
                yield break;
            }

            var booleanComparisonExpressionIdentifier = booleanComparisonExpression.FirstTokenIndex + booleanComparisonExpression.LastTokenIndex;

            if (booleanComparisonExpressionIdentifiers.Contains(booleanComparisonExpressionIdentifier))
            {
                yield break;
            }

            booleanComparisonExpressionIdentifiers.Add(booleanComparisonExpressionIdentifier);

            var columnRelationship = new ColumnRelationship(fromColumn, toColumn, relationshipOrigin);

            yield return columnRelationship;
        }

        public bool TryParseColumn(ColumnReferenceExpression columnReferenceExpression, IDictionary<string, ITable> tableByAliasIdentifier, out IColumn column)
        {
            column = null;

            var multiPartIdentifier = columnReferenceExpression.MultiPartIdentifier;
            var identifiers = multiPartIdentifier.Identifiers;
            var columnName = identifiers.Last().Value;

            var tableAliasIdentifier = identifiers.Count > (int)ColumnIdentifierScope.Column
                ? identifiers[identifiers.Count - (int)ColumnIdentifierScope.Table].Value
                : columnName;

            if (!tableByAliasIdentifier.ContainsKey(tableAliasIdentifier))
            {
                return false;
            }

            var table = tableByAliasIdentifier[tableAliasIdentifier];
            var columnMultiPartIdentifier = $"{table.MultiPartIdentifier}.{columnName}";

            if (!_metadataIdentifierByMultiPartIdentifier.ContainsKey(columnMultiPartIdentifier))
            {
                return false;
            }

            column = (IColumn)_metadataIdentifierByMultiPartIdentifier[columnMultiPartIdentifier];

            return true;
        }

        public IEnumerable<(Microsoft.SqlServer.TransactSql.ScriptDom.Identifier Alias, SchemaObjectName SchemaObjectName)> GetTableIdentifiers(TableReference tableReference)
        {
            switch (tableReference)
            {
                case AdHocTableReference adHocTableReference:
                    yield return (adHocTableReference.Alias, adHocTableReference.Object?.SchemaObjectName);
                    break;

                case ChangeTableChangesTableReference changeTableChangesTableReference:
                    yield return (changeTableChangesTableReference.Alias, changeTableChangesTableReference.Target);
                    break;

                case ChangeTableVersionTableReference changeTableVersionTableReference:
                    yield return (changeTableVersionTableReference.Alias, changeTableVersionTableReference.Target);
                    break;

                case FullTextTableReference fullTextTableReference:
                    yield return (fullTextTableReference.Alias, fullTextTableReference.TableName);
                    break;

                case NamedTableReference namedTableReference:
                    yield return (namedTableReference.Alias, namedTableReference.SchemaObject);
                    break;

                case OpenRowsetTableReference openRowsetTableReference:
                    yield return (openRowsetTableReference.Alias, openRowsetTableReference.Object);
                    break;

                case OpenXmlTableReference openXmlTableReference:
                    yield return (openXmlTableReference.Alias, openXmlTableReference.TableName);
                    break;

                case SchemaObjectFunctionTableReference schemaObjectFunctionTableReference:
                    yield return (schemaObjectFunctionTableReference.Alias, schemaObjectFunctionTableReference.SchemaObject);
                    break;

                case SemanticTableReference semanticTableReference:
                    yield return (semanticTableReference.Alias, semanticTableReference.TableName);
                    break;

                case BuiltInFunctionTableReference builtInFunctionTableReference:
                case BulkOpenRowset bulkOpenRowset:
                case DataModificationTableReference dataModificationTableReference:
                case GlobalFunctionTableReference globalFunctionTableReference:
                case InlineDerivedTable inlineDerivedTable:
                case InternalOpenRowset internalOpenRowset:
                case JoinParenthesisTableReference joinParenthesisTableReference:
                case JoinTableReference joinTableReference:
                case OdbcQualifiedJoinTableReference odbcQualifiedJoinTableReference:
                case OpenJsonTableReference openJsonTableReference:
                case OpenQueryTableReference openQueryTableReference:
                case PivotedTableReference pivotedTableReference:
                case QueryDerivedTable queryDerivedTable:
                case UnpivotedTableReference unpivotedTableReference:
                case VariableMethodCallTableReference variableMethodCallTableReference:
                case VariableTableReference variableTableReference:
                    yield break;

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public IEnumerable<string> GetFilePaths(string directoryPath)
        {
            var filePaths = Directory.GetFiles(directoryPath).AsList();

            foreach (var subdirectoryPath in Directory.GetDirectories(directoryPath))
            {
                filePaths.AddRange(GetFilePaths(subdirectoryPath));
            }

            return filePaths;
        }

        public IEnumerable<(IColumn FromColumn, IColumn ToColumn, RelationshipOrigin RelationshipOrigin)> GetColumnRelationshipsWithKeyReferenceDefinition(IEnumerable<IServer> servers, RelationshipOrigin relationshipOrigin)
        {
            foreach (var server in servers)
            {
                foreach (var database in server.Databases)
                {
                    using (var sqlConnection = new SqlConnection($"Data Source={server.ServerName};Initial Catalog={database.DatabaseName};Integrated Security=true"))
                    {
                        IEnumerable<dynamic> keyReferenceDefinitions;

                        try
                        {
                            keyReferenceDefinitions = sqlConnection.Query(@"
                            WITH ObjectExpression
                            AS (
                                SELECT @@SERVERNAME                      AS ServerName
                                     , DB_NAME()                         AS DatabaseName
                                     , ISC.TABLE_SCHEMA                  AS SchemaName
                                     , O.NAME                            AS TableName
                                     , ISC.COLUMN_NAME                   AS ColumnName
                                     , O.OBJECT_ID                       AS TableIdentifier
                                     , C.COLUMN_ID                       AS ColumnIdentifier
                                FROM SYS.OBJECTS O
                                INNER JOIN INFORMATION_SCHEMA.COLUMNS AS ISC ON SCHEMA_NAME(O.SCHEMA_ID) = ISC.TABLE_SCHEMA AND O.NAME = ISC.TABLE_NAME
                                INNER JOIN SYS.COLUMNS AS C                  ON O.OBJECT_ID = C.OBJECT_ID                   AND ISC.COLUMN_NAME = C.NAME
                                ), ColumnRelationshipExpression
                            AS (
                                SELECT ObjectExpressionFrom.ServerName   AS FromServerName
                                     , ObjectExpressionFrom.DatabaseName AS FromDatabaseName
                                     , ObjectExpressionFrom.SchemaName   AS FromSchemaName
                                     , ObjectExpressionFrom.TableName    AS FromTableName
                                     , ObjectExpressionFrom.ColumnName   AS FromColumnName
                                     , ObjectExpressionTo.ServerName     AS ToServerName
                                     , ObjectExpressionTo.DatabaseName   AS ToDatabaseName
                                     , ObjectExpressionTo.SchemaName     AS ToSchemaName
                                     , ObjectExpressionTo.TableName      AS ToTableName
                                     , ObjectExpressionTo.ColumnName     AS ToColumnName
                                FROM SYS.FOREIGN_KEY_COLUMNS AS FKC
                                INNER JOIN ObjectExpression AS ObjectExpressionFrom ON FKC.PARENT_OBJECT_ID     = ObjectExpressionFrom.TableIdentifier AND ObjectExpressionFrom.ColumnIdentifier = FKC.PARENT_COLUMN_ID
                                INNER JOIN ObjectExpression AS ObjectExpressionTo   ON FKC.REFERENCED_OBJECT_ID = ObjectExpressionTo.TableIdentifier   AND ObjectExpressionTo.ColumnIdentifier   = FKC.REFERENCED_COLUMN_ID
                                )
                            SELECT LOWER(CONCAT(FromServerName, '.', FromDatabaseName, '.', FromSchemaName, '.', FromTableName, '.', FromColumnName, '.',
                                                ToServerName,   '.', ToDatabaseName,   '.', ToSchemaName,   '.', ToTableName,   '.', ToColumnName))   AS Identifier
                                 , LOWER(CONCAT(FromServerName, '.', FromDatabaseName, '.', FromSchemaName, '.', FromTableName, '.', FromColumnName)) AS FromColumnMultiPartIdentifier
                                 , LOWER(CONCAT(ToServerName,   '.', ToDatabaseName,   '.', ToSchemaName,   '.', ToTableName,   '.', ToColumnName))   AS ToColumnMultiPartIdentifier
                            FROM ColumnRelationshipExpression
                            ORDER BY FromColumnMultiPartIdentifier
                                   , ToColumnMultiPartIdentifier
                            ");
                        }
                        catch (Exception)
                        {
                            yield break;
                        }

                        foreach (var keyReferenceDefinition in keyReferenceDefinitions)
                        {
                            var fromColumn = _metadataIdentifierByMultiPartIdentifier[keyReferenceDefinition.FromColumnMultiPartIdentifier];
                            var toColumn = _metadataIdentifierByMultiPartIdentifier[keyReferenceDefinition.ToColumnMultiPartIdentifier];

                            yield return (fromColumn, toColumn, relationshipOrigin);
                        }
                    }
                }
            }
        }

        public IEnumerable<(IColumn FromColumn, IColumn ToColumn, RelationshipOrigin RelationshipOrigin)> GetColumnRelationshipsWithSqlModuleDefinition(IEnumerable<IServer> servers, RelationshipOrigin relationshipOrigin)
        {
            foreach (var server in servers)
            {
                foreach (var database in server.Databases)
                {
                    using (var sqlConnection = new SqlConnection($"Data Source={server.ServerName};Initial Catalog={database.DatabaseName};Integrated Security=true"))
                    {
                        IEnumerable<string> sqlModuleDefinitions;

                        try
                        {
                            sqlModuleDefinitions = sqlConnection.Query<string>(@"
                            SELECT SM.DEFINITION AS SqlModuleDefinition
                            FROM SYS.OBJECTS O
                            INNER JOIN SYS.SQL_MODULES SM ON O.OBJECT_ID = SM.OBJECT_ID;
                            ");
                        }
                        catch (Exception)
                        {
                            yield break;
                        }

                        foreach (var sqlModuleDefinition in sqlModuleDefinitions)
                        {
                            foreach (var columnRelationship in GetColumnRelationshipsByChangeScriptDefinition(sqlModuleDefinition, server.ServerName, database.DatabaseName, relationshipOrigin))
                            {
                                yield return (columnRelationship.FromColumn, columnRelationship.ToColumn, relationshipOrigin);
                            }
                        }
                    }
                }
            }
        }
    }
}