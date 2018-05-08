using Dapper;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace QueryDesigner.ConsoleApp
{
    public class DatabaseIntegrationExecution
    {
        public static void RestoreDatabase(string serverName, int max)
        {
            try
            {
                const int startingRowNumber = 1;

                for (var databaseIndex = startingRowNumber; databaseIndex <= max; databaseIndex++)
                {
                    DeleteDatabase(serverName, $"Database{databaseIndex}");
                }

                for (var databaseIndex = startingRowNumber; databaseIndex <= max; databaseIndex++)
                {
                    var databaseName = $"Database{databaseIndex}";

                    CreateDatabase(serverName, databaseName);

                    for (var schemaIndex = startingRowNumber; schemaIndex <= max; schemaIndex++)
                    {
                        var schemaName = $"Schema{schemaIndex}";

                        CreateSchema(serverName, schemaName, databaseName);

                        for (var tableIndex = startingRowNumber; tableIndex <= max; tableIndex++)
                        {
                            var tableName = $"Table{tableIndex}";

                            CreateTable(serverName, databaseName, tableName);
                            AddTableToSchema(serverName, databaseName, schemaName, tableName);

                            for (var rowNumber = startingRowNumber; rowNumber <= max; rowNumber++)
                            {
                                CreateColumn(serverName, databaseName, schemaName, tableName, $"Column{rowNumber}");
                            }
                        }
                    }
                }

                for (var index = startingRowNumber; index <= max; index++)
                {
                    InsertTableRows(serverName, $"Database{index}", $"Schema{index}", $"Table{index}", max);
                }
            }
            catch (Exception exception)
            {
                var message = exception.Message;

                throw;
            }
        }

        private static void DeleteDatabase(string serverName, string databaseName)
        {
            using (var sqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog=master;Integrated Security=true"))
            {
                sqlConnection.Execute($@"
                USE [master];
                IF EXISTS (SELECT Name FROM master.sys.databases WHERE Name = N'{databaseName}')
                DROP DATABASE {databaseName};");
            }
        }

        private static void CreateDatabase(string serverName, string databaseName)
        {
            using (var sqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog=master;Integrated Security=true"))
            {
                sqlConnection.Execute($@"
                USE [master];
                CREATE DATABASE {databaseName};");
            }
        }

        private static void DeleteSchema(string serverName, string schemaName)
        {
            using (var sqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog=master;Integrated Security=true"))
            {
                sqlConnection.Execute($@"
                IF EXISTS (SELECT Name FROM master.sys.schemas WHERE Name = N'{schemaName}')
                BEGIN
                    DROP SCHEMA {schemaName};
                END");
            }
        }

        private static void CreateSchema(string serverName, string schemaName, string databaseName)
        {
            using (var sqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog={databaseName};Integrated Security=true"))
            {
                sqlConnection.Execute($@"EXEC('CREATE SCHEMA {schemaName};');");
            }
        }

        private static void DeleteTable(string serverName, string databaseName, string schemaName, string tableName)
        {
            using (var sqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog={databaseName};Integrated Security=true"))
            {
                sqlConnection.Execute($@"IF OBJECT_ID('{schemaName}.{tableName}', 'U') IS NOT NULL DROP TABLE {schemaName}.{tableName};");
            }
        }

        private static void CreateTable(string serverName, string databaseName, string tableName)
        {
            using (var sqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog={databaseName};Integrated Security=true"))
            {
                sqlConnection.Execute($@"CREATE TABLE {tableName} (Identifier INT IDENTITY NOT NULL);");
            }
        }

        private static void AddTableToSchema(string serverName, string databaseName, string schemaName, string tableName)
        {
            using (var sqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog={databaseName};Integrated Security=true"))
            {
                sqlConnection.Execute($@"ALTER SCHEMA {schemaName} TRANSFER dbo.{tableName};");
            }
        }

        private static void DeleteColumn(string serverName, string databaseName, string schemaName, string tableName, string columnName)
        {
            using (var sqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog={databaseName};Integrated Security=true"))
            {
                sqlConnection.Execute($@"
                IF EXISTS(SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schemaName}' AND TABLE_NAME = '{tableName}' AND COLUMN_NAME = '{columnName}')
                BEGIN
                    ALTER TABLE {tableName} DROP COLUMN {columnName};
                END");
            }
        }

        private static void CreateColumn(string serverName, string databaseName, string schemaName, string tableName, string columnName)
        {
            using (var sqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog={databaseName};Integrated Security=true"))
            {
                sqlConnection.Execute($@"ALTER TABLE {schemaName}.{tableName} ADD {columnName} VARCHAR(255);");
            }
        }

        private static void InsertTableRows(string serverName, string databaseName, string schemaName, string tableName, int numberOfRows)
        {
            List<string> columnNames;

            using (var sqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog={databaseName};Integrated Security=true"))
            {
                columnNames = sqlConnection.Query<string>($@"SELECT DISTINCT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}';")
                    .OrderBy(columnName => columnName)
                    .TakeWhile(columnName => columnName != "Identifier")
                    .AsList();
            }

            var rowValueDictionary = new Dictionary<int, List<string>>();

            const int startingRowNumber = 1;

            foreach (var columnName in columnNames)
            {
                for (var rowNumber = startingRowNumber; rowNumber <= numberOfRows; rowNumber++)
                {
                    if (rowValueDictionary.ContainsKey(rowNumber))
                    {
                        rowValueDictionary[rowNumber].Add(columnName);
                    }
                    else
                    {
                        rowValueDictionary.Add(rowNumber, new List<string> { columnName });
                    }
                }
            }

            for (var rowValueDictionaryKey = startingRowNumber; rowValueDictionaryKey <= numberOfRows; rowValueDictionaryKey++)
            {
                using (var sqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog={databaseName};Integrated Security=true"))
                {
                    var sql = $@"
                    INSERT INTO {schemaName}.{tableName} {string.Concat("(", string.Join(" ", columnNames.Select(columnName => $"{columnName},")).TrimEnd(','), ")")}
                    VALUES {string.Concat("(", string.Join(" ", rowValueDictionary[rowValueDictionaryKey].Select(rowValue => $"'{rowValue}',")).TrimEnd(','), ");")}";

                    sqlConnection.Execute(sql);
                }
            }
        }

        private static void DeleteRowByRowIdentifier(string serverName, string databaseName, string tableName, int rowIdentifier)
        {
            using (var sqlConnection = new SqlConnection($"Data Source={serverName};Initial Catalog={databaseName};Integrated Security=true"))
            {
                sqlConnection.Execute($@"DELETE FROM {tableName} WHERE Identifier = {rowIdentifier};");
            }
        }
    }
}