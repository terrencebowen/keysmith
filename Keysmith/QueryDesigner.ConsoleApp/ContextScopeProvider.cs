using System;
using System.Collections.Generic;
using System.Linq;

namespace QueryDesigner.ConsoleApp
{
    public interface IContextScopeProvider
    {
        IContextScope GetContextScope(IReadOnlyDictionary<string, IMetadataIdentifier> metadataIdentifierByMultiPartIdentifier, IQuerySelection querySelection, string contextServerName, string contextDatabaseName);
    }

    public class ContextScopeProvider : IContextScopeProvider
    {
        public IContextScope GetContextScope(IReadOnlyDictionary<string, IMetadataIdentifier> metadataIdentifierByMultiPartIdentifier, IQuerySelection querySelection, string contextServerName, string contextDatabaseName)
        {
            string establishedServerNameContext;
            string establishedDatabaseNameContext;

            if (querySelection.IsTableSingleSelection)
            {
                var tableSingleSelection = querySelection.DistinctTables.Single();

                establishedServerNameContext = tableSingleSelection.ServerName;
                establishedDatabaseNameContext = tableSingleSelection.DatabaseName;
            }
            else if (querySelection.IsColumnSingleSelection)
            {
                var columnSingleSelection = querySelection.DistinctColumns.Single();

                establishedServerNameContext = columnSingleSelection.ServerName;
                establishedDatabaseNameContext = columnSingleSelection.DatabaseName;
            }
            else
            {
                var isUserProvidedServerContext = !string.IsNullOrEmpty(contextServerName?.Trim());

                if (isUserProvidedServerContext)
                {
                    establishedServerNameContext = contextServerName;
                }
                else
                {
                    establishedServerNameContext = DetermineContextServerName(metadataIdentifierByMultiPartIdentifier, querySelection);
                }

                var isUserProvidedDatabaseContext = !string.IsNullOrEmpty(contextDatabaseName?.Trim());

                if (isUserProvidedDatabaseContext)
                {
                    establishedDatabaseNameContext = contextDatabaseName;
                }
                else
                {
                    establishedDatabaseNameContext = DetermineContextDatabaseName(metadataIdentifierByMultiPartIdentifier, querySelection, establishedServerNameContext);
                }
            }

            var contextScope = new ContextScope(establishedServerNameContext, establishedDatabaseNameContext);

            return contextScope;
        }

        public string DetermineContextServerName(IReadOnlyDictionary<string, IMetadataIdentifier> metadataIdentifierByMultiPartIdentifier, IQuerySelection querySelection)
        {
            throw new NotImplementedException();
        }

        public string DetermineContextDatabaseName(IReadOnlyDictionary<string, IMetadataIdentifier> metadataIdentifierByMultiPartIdentifier, IQuerySelection querySelection, string establishedServerNameContext)
        {
            throw new NotImplementedException();
        }
    }
}