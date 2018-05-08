using Dapper;
using System.Collections.Generic;
using System.Linq;

namespace QueryDesigner.ConsoleApp
{
    public interface IMetadataIdentifierFamilyProvider
    {
        IMetadataIdentifierFamily GetMetadataIdentifierFamily(IReadOnlyDictionary<string, IDictionary<string, IList<IColumnRelationship>>> columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier);
    }

    public class MetadataIdentifierFamilyProvider : IMetadataIdentifierFamilyProvider
    {
        private readonly IReadOnlyCollection<IServer> _servers;
        private readonly IReadOnlyDictionary<string, IMetadataIdentifier> _metadataIdentifierByMultiPartIdentifier;

        public MetadataIdentifierFamilyProvider(IReadOnlyCollection<IServer> servers, IReadOnlyDictionary<string, IMetadataIdentifier> metadataIdentifierByMultiPartIdentifier)
        {
            _servers = servers;
            _metadataIdentifierByMultiPartIdentifier = metadataIdentifierByMultiPartIdentifier;
        }

        public IMetadataIdentifierFamily GetMetadataIdentifierFamily(IReadOnlyDictionary<string, IDictionary<string, IList<IColumnRelationship>>> columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier)
        {
            var columnsByColumnMultiPartIdentifier = new Dictionary<string, IList<IColumn>>();

            foreach (var server in _servers)
            {
                foreach (var database in server.Databases)
                {
                    foreach (var schema in database.Schemas)
                    {
                        var relatedTablesByTableMultiPartIdentifier = new Dictionary<string, IList<ITable>>();

                        foreach (var table in schema.Tables)
                        {
                            var tableMultiPartIdentifier = table.MultiPartIdentifier;

                            if (columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier.ContainsKey(tableMultiPartIdentifier))
                            {
                                var tableQueue = new Queue<ITable>();

                                tableQueue.Enqueue(table);

                                do
                                {
                                    var dequeueTable = tableQueue.Dequeue();
                                    var columnRelationshipsByFromTableMultiPartIdentifier = columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier[dequeueTable.MultiPartIdentifier];

                                    foreach (var fromMultiPartIdentifier in columnRelationshipsByFromTableMultiPartIdentifier.Keys)
                                    {
                                        var relatedTable = (ITable)_metadataIdentifierByMultiPartIdentifier[fromMultiPartIdentifier];

                                        if (relatedTablesByTableMultiPartIdentifier.ContainsKey(tableMultiPartIdentifier))
                                        {
                                            var tables = relatedTablesByTableMultiPartIdentifier[tableMultiPartIdentifier];

                                            if (tables.Contains(relatedTable))
                                            {
                                                continue;
                                            }

                                            tables.Add(relatedTable);
                                        }
                                        else
                                        {
                                            relatedTablesByTableMultiPartIdentifier.Add(tableMultiPartIdentifier, new List<ITable> { relatedTable });
                                        }

                                        tableQueue.Enqueue(relatedTable);
                                    }
                                }
                                while (tableQueue.Count > 0);
                            }
                            else
                            {
                                relatedTablesByTableMultiPartIdentifier.Add(tableMultiPartIdentifier, new List<ITable> { table });
                            }

                            if (!columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier.ContainsKey(table.MultiPartIdentifier)) // TODO: Find a more efficient way to incorporate this this logic here.
                            {
                                continue;
                            }

                            foreach (var column in table.Columns)
                            {
                                foreach (var relatedTable in relatedTablesByTableMultiPartIdentifier[table.MultiPartIdentifier])
                                {
                                    foreach (var relatedColumn in relatedTable.Columns)
                                    {
                                        if (columnsByColumnMultiPartIdentifier.ContainsKey(column.MultiPartIdentifier))
                                        {
                                            columnsByColumnMultiPartIdentifier[column.MultiPartIdentifier].Add(relatedColumn);
                                        }
                                        else
                                        {
                                            columnsByColumnMultiPartIdentifier.Add(column.MultiPartIdentifier, new List<IColumn> { relatedColumn });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            string ServerMultiPartIdentifierKeySelector<T>(KeyValuePair<string, IList<T>> keyValuePair) => ((IServer)_metadataIdentifierByMultiPartIdentifier[keyValuePair.Key]).MultiPartIdentifier;
            string DatabaseMultiPartIdentifierKeySelector<T>(KeyValuePair<string, IList<T>> keyValuePair) => ((IDatabase)_metadataIdentifierByMultiPartIdentifier[keyValuePair.Key]).MultiPartIdentifier;
            string SchemaMultiPartIdentifierKeySelector<T>(KeyValuePair<string, IList<T>> keyValuePair) => ((ISchema)_metadataIdentifierByMultiPartIdentifier[keyValuePair.Key]).MultiPartIdentifier;
            string TableMultiPartIdentifierKeySelector<T>(KeyValuePair<string, IList<T>> keyValuePair) => ((ITable)_metadataIdentifierByMultiPartIdentifier[keyValuePair.Key]).MultiPartIdentifier;
            string ColumnMultiPartIdentifierKeySelector<T>(KeyValuePair<string, IList<T>> keyValuePair) => _metadataIdentifierByMultiPartIdentifier[keyValuePair.Key].MultiPartIdentifier;
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            string DictionaryKeySelector<T>(IGrouping<string, KeyValuePair<string, IList<T>>> grouping) => grouping.Key;
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            IList<IColumn> ColumnElementSelector(IGrouping<string, KeyValuePair<string, IList<IColumn>>> grouping) => grouping.SelectMany(keyValuePair => keyValuePair.Value.Select(column => column)).Distinct().AsList();
            IList<ITable> TableElementSelector(IGrouping<string, KeyValuePair<string, IList<IColumn>>> grouping) => grouping.SelectMany(keyValuePair => keyValuePair.Value.Select(column => column.Table)).Distinct().AsList();
            IList<ISchema> DatabaseElementSelector(IGrouping<string, KeyValuePair<string, IList<ITable>>> grouping) => grouping.SelectMany(keyValuePair => keyValuePair.Value.Select(table => table.Schema)).Distinct().AsList();
            IList<IDatabase> SchemaElementSelector(IGrouping<string, KeyValuePair<string, IList<ISchema>>> grouping) => grouping.SelectMany(keyValuePair => keyValuePair.Value.Select(schema => schema.Database)).Distinct().AsList();
            IList<IServer> ServerElementSelector(IGrouping<string, KeyValuePair<string, IList<IDatabase>>> grouping) => grouping.SelectMany(keyValuePair => keyValuePair.Value.Select(database => database.Server)).Distinct().AsList();
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var columnsByServerMultiPartIdentifier = columnsByColumnMultiPartIdentifier.GroupBy(ColumnMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, ColumnElementSelector);
            var columnsByDatabaseMultiPartIdentifier = columnsByColumnMultiPartIdentifier.GroupBy(ColumnMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, ColumnElementSelector);
            var columnsBySchemaMultiPartIdentifier = columnsByColumnMultiPartIdentifier.GroupBy(ColumnMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, ColumnElementSelector);
            var columnsByTableMultiPartIdentifier = columnsByColumnMultiPartIdentifier.GroupBy(ColumnMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, ColumnElementSelector);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var tablesByServerMultiPartIdentifier = columnsByServerMultiPartIdentifier.GroupBy(keyValuePair => ((IColumn)_metadataIdentifierByMultiPartIdentifier[keyValuePair.Key]).Table.Schema.Database.Server.MultiPartIdentifier).ToDictionary(DictionaryKeySelector, TableElementSelector);
            var tablesByDatabaseMultiPartIdentifier = columnsByDatabaseMultiPartIdentifier.GroupBy(keyValuePair => ((IColumn)_metadataIdentifierByMultiPartIdentifier[keyValuePair.Key]).Table.Schema.Database.MultiPartIdentifier).ToDictionary(DictionaryKeySelector, TableElementSelector);
            var tablesBySchemaMultiPartIdentifier = columnsBySchemaMultiPartIdentifier.GroupBy(keyValuePair => ((IColumn)_metadataIdentifierByMultiPartIdentifier[keyValuePair.Key]).Table.Schema.MultiPartIdentifier).ToDictionary(DictionaryKeySelector, TableElementSelector);
            var tablesByTableMultiPartIdentifier = columnsByTableMultiPartIdentifier.GroupBy(keyValuePair => ((IColumn)_metadataIdentifierByMultiPartIdentifier[keyValuePair.Key]).Table.MultiPartIdentifier).ToDictionary(DictionaryKeySelector, TableElementSelector);
            var tablesByColumnMultiPartIdentifier = columnsByColumnMultiPartIdentifier.GroupBy(ColumnMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, TableElementSelector);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var schemasByServerMultiPartIdentifier = tablesByServerMultiPartIdentifier.GroupBy(ServerMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, DatabaseElementSelector);
            var schemasByDatabaseMultiPartIdentifier = tablesByDatabaseMultiPartIdentifier.GroupBy(DatabaseMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, DatabaseElementSelector);
            var schemasBySchemaMultiPartIdentifier = tablesBySchemaMultiPartIdentifier.GroupBy(SchemaMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, DatabaseElementSelector);
            var schemasByTableMultiPartIdentifier = tablesByTableMultiPartIdentifier.GroupBy(TableMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, DatabaseElementSelector);
            var schemasByColumnMultiPartIdentifier = tablesByColumnMultiPartIdentifier.GroupBy(ColumnMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, DatabaseElementSelector);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var databasesByServerMultiPartIdentifier = schemasByServerMultiPartIdentifier.GroupBy(ServerMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, SchemaElementSelector);
            var databasesByDatabaseMultiPartIdentifier = schemasByDatabaseMultiPartIdentifier.GroupBy(DatabaseMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, SchemaElementSelector);
            var databasesBySchemaMultiPartIdentifier = schemasBySchemaMultiPartIdentifier.GroupBy(SchemaMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, SchemaElementSelector);
            var databasesByTableMultiPartIdentifier = schemasByTableMultiPartIdentifier.GroupBy(TableMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, SchemaElementSelector);
            var databasesByColumnMultiPartIdentifier = schemasByColumnMultiPartIdentifier.GroupBy(ColumnMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, SchemaElementSelector);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var serversByServerMultiPartIdentifier = databasesByServerMultiPartIdentifier.GroupBy(ServerMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, ServerElementSelector);
            var serversByDatabaseMultiPartIdentifier = databasesByDatabaseMultiPartIdentifier.GroupBy(DatabaseMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, ServerElementSelector);
            var serversBySchemaMultiPartIdentifier = databasesBySchemaMultiPartIdentifier.GroupBy(SchemaMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, ServerElementSelector);
            var serversByTableMultiPartIdentifier = databasesByTableMultiPartIdentifier.GroupBy(TableMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, ServerElementSelector);
            var serversByColumnMultiPartIdentifier = databasesByColumnMultiPartIdentifier.GroupBy(ColumnMultiPartIdentifierKeySelector).ToDictionary(DictionaryKeySelector, ServerElementSelector);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var metadataIdentifierFamily = new MetadataIdentifierFamily(serversByServerMultiPartIdentifier: serversByServerMultiPartIdentifier,
                                                                        serversByDatabaseMultiPartIdentifier: serversByDatabaseMultiPartIdentifier,
                                                                        serversBySchemaMultiPartIdentifier: serversBySchemaMultiPartIdentifier,
                                                                        serversByTableMultiPartIdentifier: serversByTableMultiPartIdentifier,
                                                                        serversByColumnMultiPartIdentifier: serversByColumnMultiPartIdentifier,
                                                                        databasesByServerMultiPartIdentifier: databasesByServerMultiPartIdentifier,
                                                                        databasesByDatabaseMultiPartIdentifier: databasesByDatabaseMultiPartIdentifier,
                                                                        databasesBySchemaMultiPartIdentifier: databasesBySchemaMultiPartIdentifier,
                                                                        databasesByTableMultiPartIdentifier: databasesByTableMultiPartIdentifier,
                                                                        databasesByColumnMultiPartIdentifier: databasesByColumnMultiPartIdentifier,
                                                                        schemasByServerMultiPartIdentifier: schemasByServerMultiPartIdentifier,
                                                                        schemasByDatabaseMultiPartIdentifier: schemasByDatabaseMultiPartIdentifier,
                                                                        schemasBySchemaMultiPartIdentifier: schemasBySchemaMultiPartIdentifier,
                                                                        schemasByTableMultiPartIdentifier: schemasByTableMultiPartIdentifier,
                                                                        schemasByColumnMultiPartIdentifier: schemasByColumnMultiPartIdentifier,
                                                                        tablesByServerMultiPartIdentifier: tablesByServerMultiPartIdentifier,
                                                                        tablesByDatabaseMultiPartIdentifier: tablesByDatabaseMultiPartIdentifier,
                                                                        tablesBySchemaMultiPartIdentifier: tablesBySchemaMultiPartIdentifier,
                                                                        tablesByTableMultiPartIdentifier: tablesByTableMultiPartIdentifier,
                                                                        tablesByColumnMultiPartIdentifier: tablesByColumnMultiPartIdentifier,
                                                                        columnsByServerMultiPartIdentifier: columnsByServerMultiPartIdentifier,
                                                                        columnsByDatabaseMultiPartIdentifier: columnsByDatabaseMultiPartIdentifier,
                                                                        columnsBySchemaMultiPartIdentifier: columnsBySchemaMultiPartIdentifier,
                                                                        columnsByTableMultiPartIdentifier: columnsByTableMultiPartIdentifier,
                                                                        columnsByColumnMultiPartIdentifier: columnsByColumnMultiPartIdentifier);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            return metadataIdentifierFamily;
        }
    }

    public interface IMetadataIdentifierFamily
    {
        IReadOnlyDictionary<string, IList<IServer>> ServersByServerMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<IServer>> ServersByDatabaseMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<IServer>> ServersBySchemaMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<IServer>> ServersByTableMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<IServer>> ServersByColumnMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<IDatabase>> DatabasesByServerMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<IDatabase>> DatabasesByDatabaseMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<IDatabase>> DatabasesBySchemaMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<IDatabase>> DatabasesByTableMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<IDatabase>> DatabasesByColumnMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<ISchema>> SchemasByServerMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<ISchema>> SchemasByDatabaseMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<ISchema>> SchemasBySchemaMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<ISchema>> SchemasByTableMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<ISchema>> SchemasByColumnMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<ITable>> TablesByServerMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<ITable>> TablesByDatabaseMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<ITable>> TablesBySchemaMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<ITable>> TablesByTableMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<ITable>> TablesByColumnMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<IColumn>> ColumnsByServerMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<IColumn>> ColumnsByDatabaseMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<IColumn>> ColumnsBySchemaMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<IColumn>> ColumnsByTableMultiPartIdentifier { get; }
        IReadOnlyDictionary<string, IList<IColumn>> ColumnsByColumnMultiPartIdentifier { get; }
    }

    public class MetadataIdentifierFamily : IMetadataIdentifierFamily
    {
        public IReadOnlyDictionary<string, IList<IServer>> ServersByServerMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<IServer>> ServersByDatabaseMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<IServer>> ServersBySchemaMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<IServer>> ServersByTableMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<IServer>> ServersByColumnMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<IDatabase>> DatabasesByServerMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<IDatabase>> DatabasesByDatabaseMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<IDatabase>> DatabasesBySchemaMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<IDatabase>> DatabasesByTableMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<IDatabase>> DatabasesByColumnMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<ISchema>> SchemasByServerMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<ISchema>> SchemasByDatabaseMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<ISchema>> SchemasBySchemaMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<ISchema>> SchemasByTableMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<ISchema>> SchemasByColumnMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<ITable>> TablesByServerMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<ITable>> TablesByDatabaseMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<ITable>> TablesBySchemaMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<ITable>> TablesByTableMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<ITable>> TablesByColumnMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<IColumn>> ColumnsByServerMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<IColumn>> ColumnsByDatabaseMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<IColumn>> ColumnsBySchemaMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<IColumn>> ColumnsByTableMultiPartIdentifier { get; }
        public IReadOnlyDictionary<string, IList<IColumn>> ColumnsByColumnMultiPartIdentifier { get; }

        public MetadataIdentifierFamily(IReadOnlyDictionary<string, IList<IServer>> serversByServerMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<IServer>> serversByDatabaseMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<IServer>> serversBySchemaMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<IServer>> serversByTableMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<IServer>> serversByColumnMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<IDatabase>> databasesByServerMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<IDatabase>> databasesByDatabaseMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<IDatabase>> databasesBySchemaMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<IDatabase>> databasesByTableMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<IDatabase>> databasesByColumnMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<ISchema>> schemasByServerMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<ISchema>> schemasByDatabaseMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<ISchema>> schemasBySchemaMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<ISchema>> schemasByTableMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<ISchema>> schemasByColumnMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<ITable>> tablesByServerMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<ITable>> tablesByDatabaseMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<ITable>> tablesBySchemaMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<ITable>> tablesByTableMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<ITable>> tablesByColumnMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<IColumn>> columnsByServerMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<IColumn>> columnsByDatabaseMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<IColumn>> columnsBySchemaMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<IColumn>> columnsByTableMultiPartIdentifier,
                                        IReadOnlyDictionary<string, IList<IColumn>> columnsByColumnMultiPartIdentifier)
        {
            ServersByServerMultiPartIdentifier = serversByServerMultiPartIdentifier;
            ServersByDatabaseMultiPartIdentifier = serversByDatabaseMultiPartIdentifier;
            ServersBySchemaMultiPartIdentifier = serversBySchemaMultiPartIdentifier;
            ServersByTableMultiPartIdentifier = serversByTableMultiPartIdentifier;
            ServersByColumnMultiPartIdentifier = serversByColumnMultiPartIdentifier;
            DatabasesByServerMultiPartIdentifier = databasesByServerMultiPartIdentifier;
            DatabasesByDatabaseMultiPartIdentifier = databasesByDatabaseMultiPartIdentifier;
            DatabasesBySchemaMultiPartIdentifier = databasesBySchemaMultiPartIdentifier;
            DatabasesByTableMultiPartIdentifier = databasesByTableMultiPartIdentifier;
            DatabasesByColumnMultiPartIdentifier = databasesByColumnMultiPartIdentifier;
            SchemasByServerMultiPartIdentifier = schemasByServerMultiPartIdentifier;
            SchemasByDatabaseMultiPartIdentifier = schemasByDatabaseMultiPartIdentifier;
            SchemasBySchemaMultiPartIdentifier = schemasBySchemaMultiPartIdentifier;
            SchemasByTableMultiPartIdentifier = schemasByTableMultiPartIdentifier;
            SchemasByColumnMultiPartIdentifier = schemasByColumnMultiPartIdentifier;
            TablesByServerMultiPartIdentifier = tablesByServerMultiPartIdentifier;
            TablesByDatabaseMultiPartIdentifier = tablesByDatabaseMultiPartIdentifier;
            TablesBySchemaMultiPartIdentifier = tablesBySchemaMultiPartIdentifier;
            TablesByTableMultiPartIdentifier = tablesByTableMultiPartIdentifier;
            TablesByColumnMultiPartIdentifier = tablesByColumnMultiPartIdentifier;
            ColumnsByServerMultiPartIdentifier = columnsByServerMultiPartIdentifier;
            ColumnsByDatabaseMultiPartIdentifier = columnsByDatabaseMultiPartIdentifier;
            ColumnsBySchemaMultiPartIdentifier = columnsBySchemaMultiPartIdentifier;
            ColumnsByTableMultiPartIdentifier = columnsByTableMultiPartIdentifier;
            ColumnsByColumnMultiPartIdentifier = columnsByColumnMultiPartIdentifier;
        }
    }
}