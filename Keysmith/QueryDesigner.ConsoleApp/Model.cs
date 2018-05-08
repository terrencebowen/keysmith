using QuickGraph;
using System;
using System.Collections.Generic;

namespace QueryDesigner.ConsoleApp
{
    public interface IMetadataNode
    {
        string ServerName { get; }
        string DatabaseName { get; }
        string SchemaName { get; }
        string TableName { get; }
        string TableTypeDescription { get; }
        DateTime TableModifyDate { get; }
        string ColumnName { get; }
        string ColumnDataType { get; }
        int ColumnOrdinalPosition { get; }
        string ServerMultiPartIdentifier { get; }
        string DatabaseMultiPartIdentifier { get; }
        string SchemaMultiPartIdentifier { get; }
        string TableMultiPartIdentifier { get; }
        string ColumnMultiPartIdentifier { get; }
    }

    public class MetadataNode : IMetadataNode
    {
        public string ServerName { get; }
        public string DatabaseName { get; }
        public string SchemaName { get; }
        public string TableName { get; }
        public string TableTypeDescription { get; }
        public DateTime TableModifyDate { get; }
        public string ColumnName { get; }
        public string ColumnDataType { get; }
        public int ColumnOrdinalPosition { get; }
        public string ServerMultiPartIdentifier { get; }
        public string DatabaseMultiPartIdentifier { get; }
        public string SchemaMultiPartIdentifier { get; }
        public string TableMultiPartIdentifier { get; }
        public string ColumnMultiPartIdentifier { get; }

        public MetadataNode(string serverName, string databaseName, string schemaName, string tableName, string tableTypeDescription, DateTime tableModifyDate, string columnName, string columnDataType, int columnOrdinalPosition)
        {
            ServerName = serverName;
            DatabaseName = databaseName;
            SchemaName = schemaName;
            TableName = tableName;
            TableTypeDescription = tableTypeDescription;// == "USER_TABLE" ? TableTypeDescription.BaseTable : TableTypeDescription.View;
            TableModifyDate = tableModifyDate;
            ColumnName = columnName;
            ColumnDataType = columnDataType;
            ColumnOrdinalPosition = columnOrdinalPosition;
            ServerMultiPartIdentifier = $"{serverName}".ToLower();
            DatabaseMultiPartIdentifier = $"{ServerMultiPartIdentifier}.{databaseName}".ToLower();
            SchemaMultiPartIdentifier = $"{DatabaseMultiPartIdentifier}.{schemaName}".ToLower();
            TableMultiPartIdentifier = $"{SchemaMultiPartIdentifier}.{tableName}".ToLower();
            ColumnMultiPartIdentifier = $"{TableMultiPartIdentifier}.{columnName}".ToLower();
        }
    }

    public interface IMetadataIdentifier
    {
        Identifier Identifier { get; }
        string MultiPartIdentifier { get; }
    }

    public interface IServer : IMetadataIdentifier
    {
        string ServerName { get; }
        IReadOnlyCollection<IDatabase> Databases { get; }
        void AddDatabase(IDatabase database);
    }

    public class Server : IServer
    {
        public string ServerName { get; }
        public IReadOnlyCollection<IDatabase> Databases { get; }
        public Identifier Identifier { get; }
        public string MultiPartIdentifier { get; }

        public Server(string serverName, string multiPartIdentifier, IReadOnlyCollection<IDatabase> databases)
        {
            Identifier = Identifier.Server;
            ServerName = serverName;
            MultiPartIdentifier = multiPartIdentifier;
            Databases = databases;

            foreach (var database in databases)
            {
                database.SetParent(this);
            }
        }

        public void AddDatabase(IDatabase database)
        {
            throw new NotImplementedException();
        }
    }

    public interface IDatabase : IMetadataIdentifier
    {
        IServer Server { get; }
        string ServerName { get; }
        string DatabaseName { get; }
        IReadOnlyCollection<ISchema> Schemas { get; }
        void SetParent(IServer server);
    }

    public class Database : IDatabase
    {
        public IServer Server { get; private set; }
        public string ServerName { get; }
        public string DatabaseName { get; }
        public IReadOnlyCollection<ISchema> Schemas { get; }
        public Identifier Identifier { get; }
        public string MultiPartIdentifier { get; }

        public Database(string serverName, string databaseName, string multiPartIdentifier, IReadOnlyCollection<ISchema> schemas)
        {
            Identifier = Identifier.Database;
            ServerName = serverName;
            DatabaseName = databaseName;
            MultiPartIdentifier = multiPartIdentifier;
            Schemas = schemas;

            foreach (var schema in schemas)
            {
                schema.SetParent(this);
            }
        }

        public void SetParent(IServer server)
        {
            Server = server;
        }
    }

    public interface ISchema : IMetadataIdentifier
    {
        string ServerName { get; }
        IDatabase Database { get; }
        string DatabaseName { get; }
        string SchemaName { get; }
        IReadOnlyCollection<ITable> Tables { get; }
        void SetParent(IDatabase database);
    }

    public class Schema : ISchema
    {
        public string ServerName { get; }
        public IDatabase Database { get; private set; }
        public string DatabaseName { get; }
        public string SchemaName { get; }
        public IReadOnlyCollection<ITable> Tables { get; }
        public Identifier Identifier { get; }
        public string MultiPartIdentifier { get; }

        public Schema(string serverName, string databaseName, string schemaName, string multiPartIdentifier, IReadOnlyCollection<ITable> tables)
        {
            Identifier = Identifier.Schema;
            ServerName = serverName;
            DatabaseName = databaseName;
            SchemaName = schemaName;
            MultiPartIdentifier = multiPartIdentifier;
            Tables = tables;

            foreach (var table in tables)
            {
                table.SetParent(this);
            }
        }

        public void SetParent(IDatabase database)
        {
            Database = database;
        }
    }

    public interface ITable : IMetadataIdentifier
    {
        string ServerName { get; }
        string DatabaseName { get; }
        ISchema Schema { get; }
        string SchemaName { get; }
        string TableName { get; }
        TableTypeDescription TableTypeDescription { get; }
        DateTime ModificationDateTime { get; }
        IReadOnlyCollection<IColumn> Columns { get; }
        void SetParent(ISchema schema);
    }

    public class Table : ITable
    {
        public string ServerName { get; }
        public string DatabaseName { get; }
        public ISchema Schema { get; private set; }
        public string SchemaName { get; }
        public string TableName { get; }
        public TableTypeDescription TableTypeDescription { get; }
        public DateTime ModificationDateTime { get; }
        public IReadOnlyCollection<IColumn> Columns { get; }
        public Identifier Identifier { get; }
        public string MultiPartIdentifier { get; }

        public Table(string serverName, string databaseName, string schemaName, string tableName, TableTypeDescription tableTypeDescription, DateTime modificationDateTime, string multiPartIdentifier, IReadOnlyCollection<IColumn> columns)
        {
            Identifier = Identifier.Table;
            ServerName = serverName;
            DatabaseName = databaseName;
            SchemaName = schemaName;
            TableName = tableName;
            TableTypeDescription = tableTypeDescription;
            ModificationDateTime = modificationDateTime;
            MultiPartIdentifier = multiPartIdentifier;
            Columns = columns;

            foreach (var column in columns)
            {
                column.SetParent(this);
            }
        }

        public void SetParent(ISchema schema)
        {
            Schema = schema;
        }
    }

    public enum TableTypeDescription
    {
        BaseTable,
        View
    }

    public interface IColumn : IMetadataIdentifier
    {
        string ServerName { get; }
        string DatabaseName { get; }
        string SchemaName { get; }
        ITable Table { get; }
        string TableName { get; }
        string ColumnName { get; }
        string ColumnDataType { get; }
        int ColumnOrdinalPosition { get; }
        void SetParent(ITable table);
    }

    public class Column : IColumn
    {
        public string ServerName { get; }
        public string DatabaseName { get; }
        public string SchemaName { get; }
        public ITable Table { get; private set; }
        public string TableName { get; }
        public string ColumnName { get; }
        public string ColumnDataType { get; }
        public int ColumnOrdinalPosition { get; }
        public Identifier Identifier { get; }
        public string MultiPartIdentifier { get; }

        public Column(string serverName, string databaseName, string schemaName, string tableName, string columnName, string columnDataType, int columnOrdinalPosition, string multiPartIdentifier)
        {
            Identifier = Identifier.Column;
            ServerName = serverName;
            DatabaseName = databaseName;
            SchemaName = schemaName;
            TableName = tableName;
            ColumnName = columnName;
            ColumnDataType = columnDataType;
            ColumnOrdinalPosition = columnOrdinalPosition;
            MultiPartIdentifier = multiPartIdentifier;
        }

        public void SetParent(ITable table)
        {
            Table = table;
        }
    }

    public interface IColumnRelationship
    {
        string Identifier { get; }
        IColumn FromColumn { get; }
        IColumn ToColumn { get; }
        RelationshipOrigin RelationshipOrigin { get; }
    }

    public class ColumnRelationship : IColumnRelationship
    {
        public string Identifier { get; }
        public IColumn FromColumn { get; }
        public IColumn ToColumn { get; }
        public RelationshipOrigin RelationshipOrigin { get; }

        public ColumnRelationship(IColumn fromColumn, IColumn toColumn, RelationshipOrigin relationshipOrigin)
        {
            FromColumn = fromColumn;
            Identifier = $"{fromColumn.MultiPartIdentifier}.{toColumn.MultiPartIdentifier}";
            ToColumn = toColumn;
            RelationshipOrigin = relationshipOrigin;
        }
    }

    public interface ITableRelationship : IEdge<string>
    {
        ITable FromTable { get; }
        ITable ToTable { get; }
        RelationshipOrigin RelationshipOrigin { get; }
    }

    public class TableRelationship : ITableRelationship
    {
        public ITable FromTable { get; }
        public ITable ToTable { get; }
        public string Source { get; }
        public string Target { get; }
        public RelationshipOrigin RelationshipOrigin { get; }

        public TableRelationship(IReadOnlyDictionary<string, IMetadataIdentifier> metadataIdentifierByMultiPartIdentifier, string fromTableMultiPartIdentifier, string toTableMultiPartIdentifier, RelationshipOrigin relationshipOrigin)
        {
            FromTable = (ITable)metadataIdentifierByMultiPartIdentifier[fromTableMultiPartIdentifier];
            ToTable = (ITable)metadataIdentifierByMultiPartIdentifier[toTableMultiPartIdentifier];
            Source = fromTableMultiPartIdentifier;
            Target = toTableMultiPartIdentifier;
            RelationshipOrigin = relationshipOrigin;
        }
    }

    public enum RelationshipOrigin
    {
        ChangeScriptReference,
        DatabaseStructuralCloneReference,
        KeyReferenceDefinition,
        ManualDefinition, // TODO: You still need this ability before you attempt to wire up the UI!
        SelfReference,
        SqlModuleDefinition
    }

    public enum TableIdentifierScope
    {
        Server = 4,
        Database = 3,
        Schema = 2,
        Table = 1
    }

    public enum ColumnIdentifierScope
    {
        Server = 5,
        Database = 4,
        Schema = 3,
        Table = 2,
        Column = 1
    }

    public enum Identifier
    {
        Server = 1,
        Database = 2,
        Schema = 3,
        Table = 4,
        Column = 5
    }

    public interface ITableMetadataNode
    {
        string TableName { get; }
        TableTypeDescription TableTypeDescription { get; }
        DateTime TableModificationDateTime { get; }
    }

    public class TableMetadataNode : ITableMetadataNode
    {
        public string TableName { get; }
        public TableTypeDescription TableTypeDescription { get; }
        public DateTime TableModificationDateTime { get; }

        public TableMetadataNode(string tableName, string tableTypeDescription, DateTime tableModificationDateTime)
        {
            TableName = tableName;
            TableTypeDescription = tableTypeDescription == "BASE TABLE" ? TableTypeDescription.BaseTable : TableTypeDescription.View;
            TableModificationDateTime = tableModificationDateTime;
        }
    }

    public interface IColumnMetadataNode
    {
        string ColumnName { get; }
        string ColumnDataType { get; }
        int ColumnOrdinalPosition { get; }
    }

    public class ColumnMetadataNode : IColumnMetadataNode
    {
        public string ColumnName { get; }
        public string ColumnDataType { get; }
        public int ColumnOrdinalPosition { get; }

        public ColumnMetadataNode(string columnName, string columnDataType, int columnOrdinalPosition)
        {
            ColumnName = columnName;
            ColumnDataType = columnDataType;
            ColumnOrdinalPosition = columnOrdinalPosition;
        }
    }

    public interface IContextScope
    {
        string ServerName { get; }
        string DatabaseName { get; }
    }

    public class ContextScope : IContextScope
    {
        public string ServerName { get; }
        public string DatabaseName { get; }

        public ContextScope(string serverName, string databaseName)
        {
            ServerName = serverName;
            DatabaseName = databaseName;
        }
    }
}