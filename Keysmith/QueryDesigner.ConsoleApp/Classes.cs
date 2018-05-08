using Dapper;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using QueryDesigner.ConsoleApp.Visitor;
using QuickGraph;
using QuickGraph.Algorithms.Observers;
using QuickGraph.Algorithms.ShortestPath;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace QueryDesigner.ConsoleApp
{
    public class MetadataProvider
    {
        public void Start()
        {
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            //DatabaseIntegrationExecution.RestoreDatabase("tbowen", 3);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            // Parameters Start
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            const string dataDirectoryPath = @"...";
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var tab = new string(' ', 4);
            string contextServerName = "tbowen";
            string contextDatabaseName = "someDatabaseName";
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var serverNames = new List<string>
            {
                "tbowen",
            };
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var databaseCloneMultiPartIdentifiers = new List<string>
            {
                "tbowen.someDatabaseName",
            };
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            // Parameters End
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var localServerName = Environment.MachineName.ToLower();
            var linkedServerNames = GetLinkedServerNames(localServerName).AsList();
            var linkableServerNames = new List<string>();
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            foreach (var serverName in serverNames)
            {
                var isLocalServer = localServerName == serverName;

                if (isLocalServer)
                {
                    linkableServerNames.Add(serverName);
                    continue;
                }

                var isConnectedServer = IsConnectedServer(serverName, millisecondsTimeout: 3000);

                if (!isConnectedServer)
                {
                    continue;
                }

                var isLinkedServer = linkedServerNames.Contains(serverName.ToLower());

                if (isLinkedServer)
                {
                    linkableServerNames.Add(serverName);
                    continue;
                }

                var isLinkableServer = TryLinkServer(localServerName, serverName);

                if (isLinkableServer)
                {
                    linkableServerNames.Add(serverName);
                }
            }
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var databaseNamesByServerMultiPartIdentifier = new Dictionary<string, IReadOnlyList<string>>();

            foreach (var serverName in serverNames)
            {
                if (IsQueryableDatabaseMetadata(serverName, out IEnumerable<string> databaseNames))
                {
                    databaseNamesByServerMultiPartIdentifier.Add(serverName, databaseNames.AsList());
                }
            }
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var capacity = databaseNamesByServerMultiPartIdentifier.Select(keyValuePair => keyValuePair.Value.Count).Sum();
            var metadataNodesByDatabaseMultiPartIdentifier = new Dictionary<string, dynamic>(capacity);

            foreach (var keyValuePair in databaseNamesByServerMultiPartIdentifier)
            {
                var serverName = keyValuePair.Key;

                using (var sqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog=master;Integrated Security=true"))
                {
                    try
                    {
                        var databaseNames = keyValuePair.Value;
                        var sqlStringBuilder = new StringBuilder(databaseNames.Count);

                        foreach (var databaseName in databaseNames)
                        {
                            sqlStringBuilder.AppendLine($@"
                            USE [{databaseName}];
                            SELECT C.TABLE_SCHEMA           AS SchemaName
                                 , C.TABLE_NAME             AS TableName
                                 , O.TYPE_DESC              AS TableTypeDescription
                                 , O.MODIFY_DATE            AS TableModifyDate
                                 , C.COLUMN_NAME            AS ColumnName
                                 , C.DATA_TYPE              AS ColumnDataType
                                 , C.ORDINAL_POSITION       AS ColumnOrdinalPosition
                            FROM INFORMATION_SCHEMA.COLUMNS AS C
                            INNER JOIN SYS.OBJECTS          AS O ON C.TABLE_SCHEMA = SCHEMA_NAME(O.SCHEMA_ID)
                                                                AND C.TABLE_NAME   = O.NAME
                            ORDER BY C.TABLE_SCHEMA;
                            ");
                        }

                        using (var queryMultiple = sqlConnection.QueryMultiple(sqlStringBuilder.ToString()))
                        {
                            foreach (var databaseName in databaseNames)
                            {
                                try
                                {
                                    var databaseMultiPartIdentifier = $"{serverName}.{databaseName}".ToLower();
                                    var metadataNodes = queryMultiple.Read();

                                    metadataNodesByDatabaseMultiPartIdentifier.Add(databaseMultiPartIdentifier, metadataNodes);
                                }
                                catch (Exception exception)
                                {
                                    continue;
                                }
                            }
                        }
                    }
                    catch (Exception exception)
                    {
                        continue;
                    }
                }
            }
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var metadataIdentifierByMultiPartIdentifier = new Dictionary<string, IMetadataIdentifier>();
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var servers = new List<IServer>();

            foreach (var serverName in linkableServerNames)
            {
                var serverMultiPartIdentifier = $"{serverName}".ToLower();
                var isQueryableDatabaseMetadata = IsQueryableDatabaseMetadata(serverName, out IEnumerable<string> databaseNames);

                if (!isQueryableDatabaseMetadata)
                {
                    continue;
                }

                var databases = new List<IDatabase>();

                foreach (var databaseName in databaseNames)
                {
                    if (!Regex.IsMatch(databaseName, "SomeDatabaseName1|SomeDatabaseName2", RegexOptions.IgnoreCase))
                    {
                        continue;
                    }

                    var databaseMultiPartIdentifier = $"{serverName}.{databaseName}".ToLower();
                    var isQueryableSchemaMetadata = IsQueryableSchemaMetadata(serverName, databaseName, out IEnumerable<string> schemaNames);

                    if (!isQueryableSchemaMetadata)
                    {
                        continue;
                    }

                    var schemas = new List<ISchema>();

                    foreach (var schemaName in schemaNames)
                    {
                        var schemaMultiPartIdentifier = $"{serverName}.{databaseName}.{schemaName}".ToLower();
                        var isQueryableTableMetadata = IsQueryableTableMetadata(serverName, databaseName, schemaName, out IEnumerable<ITableMetadataNode> tableMetadataNodes);

                        if (!isQueryableTableMetadata)
                        {
                            continue;
                        }

                        var tables = new List<ITable>();

                        foreach (var tableMetadataNode in tableMetadataNodes)
                        {
                            var tableName = tableMetadataNode.TableName;
                            var tableMultiPartIdentifier = $"{serverName}.{databaseName}.{schemaName}.{tableName}".ToLower();
                            var tableTypeDescription = tableMetadataNode.TableTypeDescription;
                            var tableModificationDateTime = tableMetadataNode.TableModificationDateTime;

                            if (tableTypeDescription == TableTypeDescription.View)
                            {
                                var isQueryableView = IsQueryableView(serverName, databaseName, tableMultiPartIdentifier);

                                if (!isQueryableView)
                                {
                                    continue;
                                }
                            }

                            var isQueryableColumnMetadata = IsQueryableColumnMetadata(serverName, databaseName, schemaName, tableName, out IEnumerable<IColumnMetadataNode> columnMetadtaNodes);

                            if (!isQueryableColumnMetadata)
                            {
                                continue;
                            }

                            var columns = new List<IColumn>();

                            foreach (var columnMetadataNode in columnMetadtaNodes)
                            {
                                var columnName = columnMetadataNode.ColumnName;
                                var columnDataType = columnMetadataNode.ColumnDataType;
                                var columnOrdinalPosition = columnMetadataNode.ColumnOrdinalPosition;
                                var columnOrphanMultiPartIdentifier = $"{databaseName}.{schemaName}.{tableName}.{columnName}".ToLower();
                                var columnMultiPartIdentifier = $"{serverName}.{columnOrphanMultiPartIdentifier}".ToLower();
                                var column = new Column(serverName, databaseName, schemaName, tableName, columnName, columnDataType, columnOrdinalPosition, columnMultiPartIdentifier);

                                metadataIdentifierByMultiPartIdentifier.Add(column.MultiPartIdentifier, column);
                                columns.Add(column);
                            }

                            var table = new Table(serverName, databaseName, schemaName, tableName, tableTypeDescription, tableModificationDateTime, tableMultiPartIdentifier, columns);

                            metadataIdentifierByMultiPartIdentifier.Add(table.MultiPartIdentifier, table);
                            tables.Add(table);
                        }

                        var schema = new Schema(serverName, databaseName, schemaName, schemaMultiPartIdentifier, tables);

                        metadataIdentifierByMultiPartIdentifier.Add(schema.MultiPartIdentifier, schema);
                        schemas.Add(schema);
                    }

                    var database = new Database(serverName, databaseName, databaseMultiPartIdentifier, schemas);

                    metadataIdentifierByMultiPartIdentifier.Add(database.MultiPartIdentifier, database);
                    databases.Add(database);
                }

                var server = new Server(serverName, serverMultiPartIdentifier, databases);

                metadataIdentifierByMultiPartIdentifier.Add(server.MultiPartIdentifier, server);
                servers.Add(server);
            }
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var tsql140Parser = new TSql140Parser(initialQuotedIdentifiers: false);
            var sqlParserFacade = new SqlParserFacade(tsql140Parser);
            var visitorProvider = new VisitorProvider();
            var relationshipParser = new RelationshipParser(sqlParserFacade, visitorProvider, servers, metadataIdentifierByMultiPartIdentifier);
            var columnRelationships = relationshipParser.ParseRelationships(dataDirectoryPath).AsList(); // TODO: This is only .AsListed for now for testing.
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            //columnRelationships.Add(new ColumnRelationship((IColumn)metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.acquirerprocessor.processorid"], (IColumn)metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.visasreacquirerbinmap.processorid"], RelationshipOrigin.ChangeScriptReference));
            //columnRelationships.Add(new ColumnRelationship((IColumn)metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.visasreacquirerbinmap.processorid"], (IColumn)metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.acquirerprocessor.processorid"], RelationshipOrigin.ChangeScriptReference));
            //columnRelationships.Add(new ColumnRelationship((IColumn)metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.acquirerprocessor.processorid"], (IColumn)metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.visasreacquirerbinmap.processorid"], RelationshipOrigin.ChangeScriptReference));
            //columnRelationships.Add(new ColumnRelationship((IColumn)metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.visasreacquirerbinmap.processorid"], (IColumn)metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.acquirerprocessor.processorid"], RelationshipOrigin.ChangeScriptReference));
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var adjacencyGraph = new AdjacencyGraph<string, ITableRelationship>();
            var weightByTableMultiPartIdentifier = new Dictionary<string, int>();
            var weightByColumnMultiPartIdentifier = new Dictionary<string, int>();
            var columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier = new Dictionary<string, IDictionary<string, IList<IColumnRelationship>>>();
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            foreach (var columnRelationship in columnRelationships)
            {
                //---------------------------------------------------------------------------------------------------------------------------------------------
                var fromTableMultiPartIdentifier = columnRelationship.FromColumn.Table.MultiPartIdentifier;
                var toTableMultiPartIdentifier = columnRelationship.ToColumn.Table.MultiPartIdentifier;
                //---------------------------------------------------------------------------------------------------------------------------------------------
                if (fromTableMultiPartIdentifier != toTableMultiPartIdentifier)
                {
                    if (columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier.ContainsKey(fromTableMultiPartIdentifier))
                    {
                        if (columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier[fromTableMultiPartIdentifier].ContainsKey(toTableMultiPartIdentifier))
                        {
                            if (columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier[fromTableMultiPartIdentifier][toTableMultiPartIdentifier].All(relationship => relationship.Identifier != columnRelationship.Identifier))
                            {
                                columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier[fromTableMultiPartIdentifier][toTableMultiPartIdentifier].Add(columnRelationship);
                            }
                        }
                        else
                        {
                            columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier[fromTableMultiPartIdentifier].Add(toTableMultiPartIdentifier, new List<IColumnRelationship> { columnRelationship });
                        }
                    }
                    else
                    {
                        columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier.Add(fromTableMultiPartIdentifier, new Dictionary<string, IList<IColumnRelationship>> { { toTableMultiPartIdentifier, new List<IColumnRelationship> { columnRelationship } } });
                    }
                    //---------------------------------------------------------------------------------------------------------------------------------------------
                    if (!adjacencyGraph.ContainsEdge(fromTableMultiPartIdentifier, toTableMultiPartIdentifier))
                    {
                        adjacencyGraph.AddVerticesAndEdge(new TableRelationship(metadataIdentifierByMultiPartIdentifier, fromTableMultiPartIdentifier, toTableMultiPartIdentifier, columnRelationship.RelationshipOrigin));
                    }
                    //---------------------------------------------------------------------------------------------------------------------------------------------
                }
                //-------------------------------------------------------------------------------------------------------------------------------------------------
                int weightSeed;

                switch (columnRelationship.RelationshipOrigin)
                {
                    case RelationshipOrigin.ChangeScriptReference:
                    case RelationshipOrigin.SqlModuleDefinition:
                        weightSeed = int.MaxValue / 2;
                        break;

                    case RelationshipOrigin.DatabaseStructuralCloneReference:
                        weightSeed = int.MaxValue;
                        break;

                    case RelationshipOrigin.KeyReferenceDefinition:
                        weightSeed = int.MaxValue / 4;
                        break;

                    case RelationshipOrigin.ManualDefinition:
                        weightSeed = 0;
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }
                //---------------------------------------------------------------------------------------------------------------------------------------------
                if (columnRelationship.RelationshipOrigin != RelationshipOrigin.DatabaseStructuralCloneReference)
                {
                    //-----------------------------------------------------------------------------------------------------------------------------------------
                    if (weightByTableMultiPartIdentifier.ContainsKey(fromTableMultiPartIdentifier))
                    {
                        if (weightByTableMultiPartIdentifier[fromTableMultiPartIdentifier] > 0)
                        {
                            weightByTableMultiPartIdentifier[fromTableMultiPartIdentifier]--;
                        }
                    }
                    else
                    {
                        weightByTableMultiPartIdentifier.Add(fromTableMultiPartIdentifier, weightSeed);
                    }
                    //-----------------------------------------------------------------------------------------------------------------------------------------
                    var fromColumnMultiPartIdentifier = columnRelationship.FromColumn.MultiPartIdentifier;

                    if (weightByColumnMultiPartIdentifier.ContainsKey(fromColumnMultiPartIdentifier))
                    {
                        if (weightByColumnMultiPartIdentifier[fromColumnMultiPartIdentifier] > 0)
                        {
                            weightByColumnMultiPartIdentifier[fromColumnMultiPartIdentifier]--;
                        }
                    }
                    else
                    {
                        weightByColumnMultiPartIdentifier.Add(fromColumnMultiPartIdentifier, weightSeed);
                    }
                    //-----------------------------------------------------------------------------------------------------------------------------------------
                }
                //---------------------------------------------------------------------------------------------------------------------------------------------
            }
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            //var metadataIdentifierFamilyProvider = new MetadataIdentifierFamilyProvider(servers, metadataIdentifierByMultiPartIdentifier);
            //var metadataIdentifierFamily = metadataIdentifierFamilyProvider.GetMetadataIdentifierFamily(columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier);
            //var value1 = metadataIdentifierFamily.TablesByTableMultiPartIdentifier.ContainsKey("cladev02.clearent.dbo.visasreacquirerbinmap");
            //var value2 = metadataIdentifierFamily.TablesByTableMultiPartIdentifier.ContainsKey("tbowen.clearent.dbo.visasreacquirerbinmap");
            //var value3 = columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier.ContainsKey("cladev02.clearent.dbo.visasreacquirerbinmap");
            //var value4 = columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier.ContainsKey("tbowen.clearent.dbo.visasreacquirerbinmap");
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            foreach (var server in servers)
            {
                foreach (var database in server.Databases)
                {
                    foreach (var schema in database.Schemas)
                    {
                        foreach (var table in schema.Tables)
                        {
                            //---------------------------------------------------------------------------------------------------------------------------------
                            if (!weightByTableMultiPartIdentifier.ContainsKey(table.MultiPartIdentifier))
                            {
                                weightByTableMultiPartIdentifier.Add(table.MultiPartIdentifier, int.MaxValue);
                            }

                            //---------------------------------------------------------------------------------------------------------------------------------
                            foreach (var column in table.Columns)
                            {
                                //-----------------------------------------------------------------------------------------------------------------------------
                                var columnMultiPartIdentifier = column.MultiPartIdentifier;

                                if (!weightByColumnMultiPartIdentifier.ContainsKey(columnMultiPartIdentifier))
                                {
                                    weightByColumnMultiPartIdentifier.Add(columnMultiPartIdentifier, int.MaxValue);
                                }
                                //-----------------------------------------------------------------------------------------------------------------------------
                            }
                        }
                    }
                }
            }
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var metadataIdentifiers = new List<IMetadataIdentifier>();
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.adventureworks2014.person.address.addressid"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.adventureworks2014.production.unitmeasure.unitmeasurecode"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.adventureworks2014.sales.salestaxrate.salestaxrateid"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.adventureworks2014.production.transactionhistory.transactionid"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.adventureworks2014.sales.specialofferproduct"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.adventureworks2014.person.password.businessentityid"]);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.merchant.merchantnumber"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.businesstaxinfobusinesses.businesstaxinfoid"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.merchantlifecyclestatus.merchantlifecyclestatusid"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.business.businessdba"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.business.businesscommonname"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.business.tin"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.business.tintypeid"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.emailaddress.emailaddress"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.phone.formattedphone"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.address.line1"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.address.city"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.address.state"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.address.zip"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.address.country"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.merchantprofile.stateincorporated"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.principal.firstname"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.principal.lastname"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.principal.principalid"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.principal.ownershipamount"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.principal.dateofbirth"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.principal.ssn"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.companytypecode.companytypeid"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.phone.phoneid"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.contacttypecoderef.contacttypecode"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.contacttypecoderef.displayorder"]);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            // TODO: Find out why you get a 'VSB.*' table alias when running these identifiers! (this table is clearent.dbo.visasreacquirerbinmap).
            // TODO: If tihs is technically the moment the user clicks the item it may be worth checking to see if this identifier exists in the event the server goes offline just after it was available.
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.acquirer"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.acquirerbusiness"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.acquirerprocessor"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.bankidnumber"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.business.bin"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.businesspricing"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.businesspricingfee"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.businesstaxinfo"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.businesstaxinfobusinesses"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.merchantalias"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.merchantaliastype"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.sponsorbank"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.sponsorbankreplacementtags"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.visasreacquirerbinmap"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.rpt.dim_bankidnumber"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.rpt.dim_business.bin"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.rpt.dim_cardassociation"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.rpt.dim_date"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.rpt.dim_date_month"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.rpt.dim_transactiontype"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.rpt.fact_transactionvolume"]);
            metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.rpt.fact_transactionvolume_monthsettled"]);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.rejects.v2.rejectstatus"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev02.rejects.v2.rejectstatus"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladevsql01.rejects.v2.rejectstatus"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.rejects.v2.rejectstatus"]);
            ////
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev02.edocs.dbo.documents"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev02.provisioning.geo.timezones"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev02.provisioning.geo.timezones.timezoneid"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladevsql01.edocs.dbo.documents"]);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.principal.principalid"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.principal.principalid"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.principal"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.principal"]);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.database2.schema2.table2"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.database2.schema2.table2.column1"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.database2.schema2.table2.column1"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["tbowen.database2.schema2.table2.column3"]);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.merchant"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.business"]);
            //metadataIdentifiers.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.bankidnumber"]);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var metadataIdentifierExclusions = new List<IMetadataIdentifier>();
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            // TODO: Do you need to programatically/automatically remove all metadataIdentifiers from all cloned databases? (otherwise you would have to specify tbowen.clearent.dbo.access for each database you're joining with like beneath).
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            //metadataIdentifierExclusions.Add(metadataIdentifierByMultiPartIdentifier["cladev01.boarding.dbo.access"]);
            //metadataIdentifierExclusions.Add(metadataIdentifierByMultiPartIdentifier["cladev01.boarding.dbo.contact"]);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            //metadataIdentifierExclusions.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.access"]);
            //metadataIdentifierExclusions.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.accessbusiness"]);
            //metadataIdentifierExclusions.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.businessbase"]);
            //metadataIdentifierExclusions.Add(metadataIdentifierByMultiPartIdentifier["cladev01.clearent.dbo.contact"]);
            //metadataIdentifierExclusions.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.access"]);
            //metadataIdentifierExclusions.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.accessbusiness"]);
            //metadataIdentifierExclusions.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.businessbase"]);
            //metadataIdentifierExclusions.Add(metadataIdentifierByMultiPartIdentifier["tbowen.clearent.dbo.contact"]);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            if (!metadataIdentifiers.Any())
            {
                return; // TODO: This is only a simple return for now because this method Start() is void temporarily.
            }
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var predicateRegex = new Regex("[^A-Z0-9]");
            var pascalCaseRegex = new Regex("([a-z])([A-Z])");
            var textInfo = new CultureInfo("en-US").TextInfo;
            var sqlFormatter = new SqlFormatter(sqlParserFacade);
            var tableReferenceIdentifierByTableMultiPartIdentifierProvider = new TableReferenceIdentifierByTableMultiPartIdentifierProvider(sqlFormatter, predicateRegex, pascalCaseRegex, textInfo);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var querySelectionProvider = new QuerySelectionProvider(localServerName);
            var querySelection = querySelectionProvider.GetQuerySelection(weightByTableMultiPartIdentifier, metadataIdentifiers);
            var contextScopeProvider = new ContextScopeProvider();
            var contextScope = contextScopeProvider.GetContextScope(metadataIdentifierByMultiPartIdentifier, querySelection, contextServerName, contextDatabaseName); // TODO: Make sure you provide the option to simply explicitly qualify all database names.
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            metadataIdentifierExclusions.AddRange(servers.Except(querySelection.DistinctServers));
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            // TODO: Execute a fail fast iteration over all of the metadataidentifiers to see if they will actually have columnRelationshps before going any further!!!!!!!!!!!!!!!!
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var visitedGraph = new AdjacencyGraph<string, ITableRelationship>();

            visitedGraph.AddVerticesAndEdgeRange(adjacencyGraph.Edges);

            if (metadataIdentifierExclusions.Any())
            {
                foreach (var metadataIdentifier in metadataIdentifierExclusions)
                {
                    switch (metadataIdentifier)
                    {
                        case IServer server:
                            foreach (var table in from database in server.Databases from schema in database.Schemas from table in schema.Tables select table)
                            {
                                visitedGraph.RemoveEdgeIf(RemoveEdgePredicateIf(table));
                            }
                            break;

                        case IDatabase database:
                            foreach (var table in database.Schemas.SelectMany(schema => schema.Tables))
                            {
                                visitedGraph.RemoveEdgeIf(RemoveEdgePredicateIf(table));
                            }
                            break;

                        case ISchema schema:
                            foreach (var table in schema.Tables)
                            {
                                visitedGraph.RemoveEdgeIf(RemoveEdgePredicateIf(table));
                            }
                            break;

                        case ITable table:
                            visitedGraph.RemoveEdgeIf(RemoveEdgePredicateIf(table));
                            break;

                        case IColumn column:
                            visitedGraph.RemoveEdgeIf(RemoveEdgePredicateIf(column.Table));
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var joinStatementStringBuilder = new StringBuilder();
            var selectionTables = querySelection.DistinctTables;
            //---------------------------------------------------------------------------------------------------------------------------------------------
            if (querySelection.IsTableSingleSelection)
            {
                var tableReferenceIdentifierByTableMultiPartIdentifier = tableReferenceIdentifierByTableMultiPartIdentifierProvider.GetTableReferenceIdentifierByTableMultiPartIdentifier(contextScope, selectionTables);
                var table = selectionTables.Single();
                var tableReferenceIdentifier = tableReferenceIdentifierByTableMultiPartIdentifier[table.MultiPartIdentifier];

                joinStatementStringBuilder.AppendLine($"{tab}{tableReferenceIdentifier.TableAlias}.*");
                joinStatementStringBuilder.Append($"FROM {tableReferenceIdentifier.TableSource}");
            }
            else if (querySelection.IsColumnSingleSelection)
            {
                var tableReferenceIdentifierByTableMultiPartIdentifier = tableReferenceIdentifierByTableMultiPartIdentifierProvider.GetTableReferenceIdentifierByTableMultiPartIdentifier(contextScope, selectionTables);
                var table = selectionTables.Single();
                var tableReferenceIdentifier = tableReferenceIdentifierByTableMultiPartIdentifier[table.MultiPartIdentifier];
                var metadataIdentifier = metadataIdentifiers.Single();
                var column = (IColumn)metadataIdentifier;

                joinStatementStringBuilder.AppendLine($"{tab}{column.ColumnName}");
                joinStatementStringBuilder.Append($"FROM {tableReferenceIdentifier.TableSource}");
            }
            else
            {
                //---------------------------------------------------------------------------------------------------------------------------------------------
                var tableRelationships = new List<ITableRelationship>();
                //---------------------------------------------------------------------------------------------------------------------------------------------
                for (var index = 1; index < selectionTables.Count; index++)
                {
                    var fromTable = selectionTables[0];
                    var toTable = selectionTables[index];

                    if (!TryParseTableRelationships(visitedGraph, weightByTableMultiPartIdentifier, fromTable, toTable, out IEnumerable<ITableRelationship> relationships))
                    {
                        //TODO: Consider what you should do here in the event you don't get any relationships, is this an early warning that the items don't all relate too each other?
                        continue;
                    }

                    tableRelationships = relationships.Union(tableRelationships).AsList();
                }
                //---------------------------------------------------------------------------------------------------------------------------------------------
                if (!tableRelationships.Any())
                {
                    throw new Exception();
                }
                //---------------------------------------------------------------------------------------------------------------------------------------------
                var tableReferenceIdentifierByTableMultiPartIdentifier = tableReferenceIdentifierByTableMultiPartIdentifierProvider.GetTableReferenceIdentifierByTableMultiPartIdentifier(contextScope, selectionTables, tableRelationships.ToArray());
                //---------------------------------------------------------------------------------------------------------------------------------------------
                // TODO: You are outputing your list of select list items before you know which tables you're actually using.
                for (var index = 0; index < metadataIdentifiers.Count; index++)
                {
                    switch (metadataIdentifiers[index])
                    {
                        case IColumn column:
                            var columnAlias = tableReferenceIdentifierByTableMultiPartIdentifier[column.Table.MultiPartIdentifier].TableAlias;
                            var columnName = column.ColumnName;

                            joinStatementStringBuilder.AppendLine(index == 0 ? $"{tab}  {columnAlias}.{columnName}" : $"{tab}, {columnAlias}.{columnName}");
                            break;

                        case ITable table:
                            var tableAlias = tableReferenceIdentifierByTableMultiPartIdentifier[table.MultiPartIdentifier].TableAlias;

                            joinStatementStringBuilder.AppendLine(index == 0 ? $"{tab}  {tableAlias}.*" : $"{tab}, {tableAlias}.*");
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
                //---------------------------------------------------------------------------------------------------------------------------------------------
                var fromTableFirst = tableRelationships[0].FromTable;
                var tableReferenceIdentifierFirst = tableReferenceIdentifierByTableMultiPartIdentifier[fromTableFirst.MultiPartIdentifier];
                var tableSourceFirst = tableReferenceIdentifierFirst.TableSource;
                var tableAliasFirst = tableReferenceIdentifierFirst.TableAlias;
                var joinStatements = new List<string>();

                joinStatementStringBuilder.AppendLine($"FROM {tableSourceFirst} AS {tableAliasFirst}");

                for (var index = 0; index < tableRelationships.Count; index++)
                {
                    var fromTable = index == 0 ? tableRelationships[0].FromTable
                                               : tableRelationships[index].FromTable;

                    var toTable = tableRelationships[index].ToTable;
                    var tableReferenceIdentifier = tableReferenceIdentifierByTableMultiPartIdentifier[toTable.MultiPartIdentifier];
                    var joinString = GetJoinString(fromTable, toTable, tableReferenceIdentifierByTableMultiPartIdentifier, columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier, weightByColumnMultiPartIdentifier);
                    var multiPartIdentifier = tableReferenceIdentifier.TableSource;
                    var alias = tableReferenceIdentifier.TableAlias;
                    var joinStatement = $"INNER JOIN {multiPartIdentifier} AS {alias} ON {joinString}";

                    if (joinStatements.Contains(joinStatement))
                    {
                        continue;
                    }

                    if (index < tableRelationships.Count - 1)
                    {
                        joinStatementStringBuilder.AppendLine(joinStatement);
                    }
                    else
                    {
                        joinStatementStringBuilder.Append(joinStatement);
                    }

                    joinStatements.Add(joinStatement);
                }
                //---------------------------------------------------------------------------------------------------------------------------------------------
            }
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            var sql = string.Join(Environment.NewLine,
                "BEGIN TRANSACTION",
                $"USE {sqlFormatter.QuoteEncapsulate(contextScope.DatabaseName)};",
                "SELECT",
                $"{joinStatementStringBuilder}",
                "ROLLBACK");
            //-------------------------------------------------------------------------------------------------------------------------------------------------
            const string outputFilePath = @"C:\Users\tbowen\Desktop\Query Designer\Notes\Output.sql";
            File.WriteAllText(outputFilePath, sql.Trim());
            Console.WriteLine(sql);
            Debug.WriteLine(sql);
            //-------------------------------------------------------------------------------------------------------------------------------------------------
        }

        public bool IsQueryableView(string serverName, string databaseName, string tableMultiPartIdentifier)
        {
            try
            {
                using (var nestedSqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog={databaseName};Integrated Security=true"))
                {
                    nestedSqlConnection.Query($@"
                    BEGIN TRANSACTION
                        SELECT TOP 0 * FROM {tableMultiPartIdentifier};
                    ROLLBACK
                    ");
                }

                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public void AddColumn(ref Dictionary<string, IList<IColumn>> columnFamilyByColumnMultiPartIdentifier, IColumn fromColumn, IColumn toColumn)
        {
            if (fromColumn.MultiPartIdentifier == toColumn.MultiPartIdentifier)
            {
                return;
            }

            if (columnFamilyByColumnMultiPartIdentifier.ContainsKey(fromColumn.MultiPartIdentifier))
            {
                if (!columnFamilyByColumnMultiPartIdentifier[fromColumn.MultiPartIdentifier].Contains(toColumn))
                {
                    columnFamilyByColumnMultiPartIdentifier[fromColumn.MultiPartIdentifier].Add(toColumn);
                }
            }
            else
            {
                columnFamilyByColumnMultiPartIdentifier.Add(fromColumn.MultiPartIdentifier, new List<IColumn> { toColumn });
            }
        }

        public IEnumerable<string> GetLinkedServerNames(string localServerName)
        {
            using (var sqlConnection = new SqlConnection($"Data Source={localServerName};Initial Catalog=master;Integrated Security=true"))
            {
                IEnumerable<string> serverNames;

                try
                {
                    serverNames = sqlConnection.Query<string>("SP_LINKEDSERVERS", commandType: CommandType.StoredProcedure);
                }
                catch (Exception)
                {
                    yield break;
                }

                foreach (var serverName in serverNames)
                {
                    yield return serverName.ToLower();
                }
            }
        }

        public bool IsConnectedServer(string serverName, int millisecondsTimeout)
        {
            var isConnectedServer = false;

            try
            {
                Task.Factory.StartNew(() =>
                {
                    try
                    {
                        var ipHostEntry = Dns.GetHostEntry(serverName);
                        var ipAddresses = ipHostEntry.AddressList;

                        isConnectedServer = ipAddresses.Any() && ipAddresses.First() != null;
                    }
                    catch (Exception)
                    {
                        isConnectedServer = false;
                    }
                }).Wait(millisecondsTimeout);
            }
            catch (Exception)
            {
                isConnectedServer = false;
            }

            return isConnectedServer;
        }

        public bool TryLinkServer(string localServerName, string serverName)
        {
            bool isLinkableServer;

            using (var sqlConnection = new SqlConnection($"Data Source={localServerName};Initial Catalog=master;Integrated Security=true"))
            {
                try
                {
                    sqlConnection.Execute("SP_ADDLINKEDSERVER", new { server = $"{serverName}" }, commandType: CommandType.StoredProcedure);
                    isLinkableServer = true;
                }
                catch (Exception)
                {
                    isLinkableServer = false;
                }
            }

            return isLinkableServer;
        }

        public string GetJoinString(ITable fromTable, ITable toTable, IReadOnlyDictionary<string, ITableReferenceIdentifier> tableReferenceSpecificationByTableMultiPartIdentifier, IDictionary<string, IDictionary<string, IList<IColumnRelationship>>> columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier, IReadOnlyDictionary<string, int> weightByColumnMultiPartIdentifier)
        {
            var joinColumnTextStringBuilder = new StringBuilder();
            var columnRelationshipsFrom = columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier[fromTable.MultiPartIdentifier][toTable.MultiPartIdentifier];
            var fromColumnRelationships = columnRelationshipsFrom.OrderBy(columnRelationship => weightByColumnMultiPartIdentifier[columnRelationship.FromColumn.MultiPartIdentifier]).AsList();
            var columnRelationshipsTo = columnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier[toTable.MultiPartIdentifier][fromTable.MultiPartIdentifier];
            var toColumnRelationships = columnRelationshipsTo.OrderBy(columnRelationship => weightByColumnMultiPartIdentifier[columnRelationship.FromColumn.MultiPartIdentifier]).AsList();

            // TODO: the count initializer is commented out because you only want to return the first relationships, the remainder will be kept in some container object to be added or removed as the user prefers.
            for (var index = 0; index < /*fromColumnRelationships.Count*/1; index++)
            {
                var fromColumnRelationship = fromColumnRelationships[index];
                var toColumnRelationship = toColumnRelationships[index];
                string andOperatorPrefix;

                if (index == 0)
                {
                    andOperatorPrefix = null;
                }
                else
                {
                    andOperatorPrefix = $"{Environment.NewLine}{new string(' ', 6)} AND ";
                }

                var aliasSource = tableReferenceSpecificationByTableMultiPartIdentifier[fromTable.MultiPartIdentifier].TableAlias;
                var columnNameSource = fromColumnRelationship.FromColumn.ColumnName;
                var aliasTarget = tableReferenceSpecificationByTableMultiPartIdentifier[toTable.MultiPartIdentifier].TableAlias;
                var columnNameTarget = toColumnRelationship.FromColumn.ColumnName;

                joinColumnTextStringBuilder.Append($"{andOperatorPrefix}{aliasSource}.{columnNameSource} = {aliasTarget}.{columnNameTarget}");
            }

            var joinColumnText = joinColumnTextStringBuilder.ToString();

            return joinColumnText;
        }

        public bool TryParseTableRelationships(AdjacencyGraph<string, ITableRelationship> adjacencyGraph, IReadOnlyDictionary<string, int> weightByTableMultiPartIdentifier, ITable fromTable, ITable toTable, out IEnumerable<ITableRelationship> tableRelationships)
        {
            tableRelationships = null;

            double WeightByTableMultiPartIdentifierFunction(ITableRelationship tableRelationship) => weightByTableMultiPartIdentifier[tableRelationship.FromTable.MultiPartIdentifier];
            var dijkstraShortestPathAlgorithm = new DijkstraShortestPathAlgorithm<string, ITableRelationship>(adjacencyGraph, WeightByTableMultiPartIdentifierFunction);
            var vertexPredecessorRecorderObserver = new VertexPredecessorRecorderObserver<string, ITableRelationship>();
            var vertexDistanceRecorderObserver = new VertexDistanceRecorderObserver<string, ITableRelationship>(WeightByTableMultiPartIdentifierFunction);

            vertexPredecessorRecorderObserver.Attach(dijkstraShortestPathAlgorithm);
            vertexDistanceRecorderObserver.Attach(dijkstraShortestPathAlgorithm);

            dijkstraShortestPathAlgorithm.Compute(fromTable.MultiPartIdentifier);

            if (!vertexPredecessorRecorderObserver.TryGetPath(toTable.MultiPartIdentifier, out IEnumerable<ITableRelationship> paths))
            {
                return false;
            }

            tableRelationships = paths;

            return true;
        }

        public EdgePredicate<string, ITableRelationship> RemoveEdgePredicateIf(ITable table)
        {
            return tableRelationship => tableRelationship.Source == table.MultiPartIdentifier || tableRelationship.Target == table.MultiPartIdentifier;
        }

        public bool IsQueryableDatabaseMetadata(string serverName, out IEnumerable<string> databaseNames)
        {
            using (var sqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog=master;Integrated Security=true"))
            {
                databaseNames = null;

                try
                {
                    databaseNames = sqlConnection.Query<string>(@"
                    SELECT NAME AS DatabaseName
                    FROM SYS.DATABASES
                    WHERE NAME NOT IN ('master', 'model', 'msdb', 'tempdb')
                    ORDER BY NAME;
                    ");
                }
                catch (Exception)
                {
                    return false;
                }

                return true;
            }
        }

        public bool IsQueryableSchemaMetadata(string serverName, string databaseName, out IEnumerable<string> schemaNames)
        {
            using (var sqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog={databaseName};Integrated Security=true"))
            {
                schemaNames = null;

                try
                {
                    schemaNames = sqlConnection.Query<string>(@"
                    SELECT DISTINCT S.NAME AS SchemaName
                    FROM SYS.OBJECTS       AS O
                    INNER JOIN SYS.SCHEMAS AS S ON O.SCHEMA_ID = S.SCHEMA_ID
                    WHERE O.TYPE_DESC IN ('USER_TABLE', 'VIEW');
                    ");
                }
                catch (Exception)
                {
                    return false;
                }

                return true;
            }
        }

        public bool IsQueryableTableMetadata(string serverName, string databaseName, string schemaName, out IEnumerable<ITableMetadataNode> tableMetadataNodes)
        {
            using (var sqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog={databaseName};Integrated Security=true"))
            {
                tableMetadataNodes = null;

                try
                {
                    tableMetadataNodes = sqlConnection.Query<TableMetadataNode>($@"
                    SELECT T.TABLE_NAME  AS TableName
                         , T.TABLE_TYPE  AS TableTypeDescription
                         , O.MODIFY_DATE AS TableModificationDateTime
                    FROM INFORMATION_SCHEMA.TABLES AS T
                    INNER JOIN SYS.OBJECTS         AS O ON T.TABLE_SCHEMA = SCHEMA_NAME(O.SCHEMA_ID)
                    AND T.TABLE_NAME = O.NAME
                    WHERE T.TABLE_SCHEMA = '{schemaName}' AND O.TYPE_DESC IN ('USER_TABLE', 'VIEW');
                    ");
                }
                catch (Exception)
                {
                    return false;
                }

                return true;
            }
        }

        public bool IsQueryableColumnMetadata(string serverName, string databaseName, string schemaName, string tableName, out IEnumerable<IColumnMetadataNode> columnMetadataNodes)
        {
            using (var sqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog={databaseName};Integrated Security=true"))
            {
                columnMetadataNodes = null;

                try
                {
                    columnMetadataNodes = sqlConnection.Query<ColumnMetadataNode>($@"
                    SELECT COLUMN_NAME      AS ColumnName
                         , DATA_TYPE        AS ColumnDataType
                         , ORDINAL_POSITION AS ColumnOrdinalPosition
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{schemaName}' AND TABLE_NAME = '{tableName}';
                    ");
                }
                catch (Exception)
                {
                    return false;
                }

                return true;
            }
        }
    }
}