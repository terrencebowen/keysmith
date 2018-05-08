using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;

namespace QueryDesigner.ConsoleApp
{
    public interface IQuerySelectionProvider
    {
        IQuerySelection GetQuerySelection(IReadOnlyDictionary<string, int> weightByTableMultiPartIdentifier, IReadOnlyCollection<IMetadataIdentifier> metadataIdentifiers);
    }

    public class QuerySelectionProvider : IQuerySelectionProvider
    {
        private readonly string _localServerName;

        public QuerySelectionProvider(string localServerName)
        {
            _localServerName = localServerName;
        }

        public IQuerySelection GetQuerySelection(IReadOnlyDictionary<string, int> weightByTableMultiPartIdentifier, IReadOnlyCollection<IMetadataIdentifier> metadataIdentifiers)
        {
            if (metadataIdentifiers.Count == 0)
            {
                return null;
            }

            var indistinctMetadataIdentifiers = new List<IMetadataIdentifier>();

            foreach (var metadataIdentifier in metadataIdentifiers)
            {
                switch (metadataIdentifier)
                {
                    case IServer server:
                        indistinctMetadataIdentifiers.Add(server);
                        break;

                    case IDatabase database:
                        indistinctMetadataIdentifiers.Add(database.Server);
                        indistinctMetadataIdentifiers.Add(database);
                        break;

                    case ISchema schema:
                        indistinctMetadataIdentifiers.Add(schema.Database.Server);
                        indistinctMetadataIdentifiers.Add(schema.Database);
                        indistinctMetadataIdentifiers.Add(schema);
                        break;

                    case ITable table:
                        indistinctMetadataIdentifiers.Add(table.Schema.Database.Server);
                        indistinctMetadataIdentifiers.Add(table.Schema.Database);
                        indistinctMetadataIdentifiers.Add(table.Schema);
                        indistinctMetadataIdentifiers.Add(table);
                        break;

                    case IColumn column:
                        indistinctMetadataIdentifiers.Add(column.Table.Schema.Database.Server);
                        indistinctMetadataIdentifiers.Add(column.Table.Schema.Database);
                        indistinctMetadataIdentifiers.Add(column.Table.Schema);
                        indistinctMetadataIdentifiers.Add(column.Table);
                        indistinctMetadataIdentifiers.Add(column);
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            var distinctServers = GetDistinctMetadataIdentifiers<IServer>(indistinctMetadataIdentifiers);
            var distinctDatabases = GetDistinctMetadataIdentifiers<IDatabase>(indistinctMetadataIdentifiers);
            var distinctSchemas = GetDistinctMetadataIdentifiers<ISchema>(indistinctMetadataIdentifiers);

            var distinctTables = GetDistinctMetadataIdentifiers<ITable>(indistinctMetadataIdentifiers).OrderBy(table => weightByTableMultiPartIdentifier[table.MultiPartIdentifier])
                                                                                                      .ThenBy(table => table.ServerName)
                                                                                                      .ThenBy(table => table.DatabaseName)
                                                                                                      .ThenBy(table => table.SchemaName)
                                                                                                      .ThenBy(table => table.TableName)
                                                                                                      .AsList();

            var distinctColumns = GetDistinctMetadataIdentifiers<IColumn>(indistinctMetadataIdentifiers);

            bool isTableSingleSelection;
            bool isColumnSingleSelection;

            if (distinctTables.Count == 1 && distinctColumns.Count == 0)
            {
                isTableSingleSelection = true;
                isColumnSingleSelection = false;
            }
            else if (distinctTables.Count == 1 && distinctColumns.Count == 1)
            {
                isTableSingleSelection = false;
                isColumnSingleSelection = true;
            }
            else
            {
                isTableSingleSelection = false;
                isColumnSingleSelection = false;
            }

            var querySelection = new QuerySelection(distinctServers,
                                                    distinctDatabases,
                                                    distinctSchemas,
                                                    distinctTables,
                                                    distinctColumns,
                                                    isTableSingleSelection,
                                                    isColumnSingleSelection,
                                                    indistinctMetadataIdentifiers,
                                                    _localServerName);

            return querySelection;
        }

        public IReadOnlyList<T> GetDistinctMetadataIdentifiers<T>(IReadOnlyCollection<IMetadataIdentifier> metadataIdentifiers)
        {
            return metadataIdentifiers.Where(metadataIdentifier => metadataIdentifier is T)
                                      .Distinct()
                                      .Cast<T>()
                                      .AsList();
        }
    }

    public interface IQuerySelection
    {
        IReadOnlyList<IServer> DistinctServers { get; }
        IReadOnlyList<IDatabase> DistinctDatabases { get; }
        IReadOnlyList<ISchema> DistinctSchemas { get; }
        IReadOnlyList<ITable> DistinctTables { get; }
        IReadOnlyList<IColumn> DistinctColumns { get; }
        bool IsTableSingleSelection { get; }
        bool IsColumnSingleSelection { get; }
        IReadOnlyList<IServer> AvailableContextServers { get; }
        IReadOnlyList<IDatabase> AvailableContextDatabases { get; }
        IReadOnlyList<IMetadataIdentifier> IndistinctMetadataIdentifiers { get; }
        string LocalServerName { get; }
    }

    public class QuerySelection : IQuerySelection
    {
        public IReadOnlyList<IServer> DistinctServers { get; }
        public IReadOnlyList<IDatabase> DistinctDatabases { get; }
        public IReadOnlyList<ISchema> DistinctSchemas { get; }
        public IReadOnlyList<ITable> DistinctTables { get; }
        public IReadOnlyList<IColumn> DistinctColumns { get; }
        public bool IsTableSingleSelection { get; }
        public bool IsColumnSingleSelection { get; }
        public IReadOnlyList<IServer> AvailableContextServers => DistinctServers;
        public IReadOnlyList<IDatabase> AvailableContextDatabases => DistinctDatabases;
        public IReadOnlyList<IMetadataIdentifier> IndistinctMetadataIdentifiers { get; }
        public string LocalServerName { get; }

        public QuerySelection(IReadOnlyList<IServer> distinctServers,
                              IReadOnlyList<IDatabase> distinctDatabases,
                              IReadOnlyList<ISchema> distinctSchemas,
                              IReadOnlyList<ITable> distinctTables,
                              IReadOnlyList<IColumn> distinctColumns,
                              bool isTableSingleSelection,
                              bool isColumnSingleSelection,
                              IReadOnlyList<IMetadataIdentifier> indistinctMetadataIdentifiers,
                              string localServerName)
        {
            DistinctServers = distinctServers;
            DistinctDatabases = distinctDatabases;
            DistinctSchemas = distinctSchemas;
            DistinctTables = distinctTables;
            DistinctColumns = distinctColumns;
            IsTableSingleSelection = isTableSingleSelection;
            IsColumnSingleSelection = isColumnSingleSelection;
            IndistinctMetadataIdentifiers = indistinctMetadataIdentifiers;
            LocalServerName = localServerName;
        }
    }
}