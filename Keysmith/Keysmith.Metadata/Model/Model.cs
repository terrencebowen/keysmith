using QuickGraph;
using System.Collections.Generic;

namespace Keysmith.Metadata.Model
{
    public interface IMetadata
    {
        IEnvironmentMetadata EnvironmentMetadata { get; }
        IHierarchyMetadata HierarchyMetadata { get; }
        IGraphMetadata GraphMetadata { get; }
    }

    public class Metadata : IMetadata
    {
        public IEnvironmentMetadata EnvironmentMetadata { get; }
        public IHierarchyMetadata HierarchyMetadata { get; }
        public IGraphMetadata GraphMetadata { get; }

        public Metadata(IEnvironmentMetadata environmentMetadata, IHierarchyMetadata hierarchyMetadata, IGraphMetadata graphMetadata)
        {
            EnvironmentMetadata = environmentMetadata;
            HierarchyMetadata = hierarchyMetadata;
            GraphMetadata = graphMetadata;
        }
    }

    public interface IEnvironmentMetadata
    {
        string LocalServerName { get; set; }
        IEnumerable<string> RemoteServerNames { get; set; }
    }

    public class EnvironmentMetadata : IEnvironmentMetadata
    {
        public string LocalServerName { get; set; }
        public IEnumerable<string> RemoteServerNames { get; set; }

        public EnvironmentMetadata(string localServerName, IEnumerable<string> remoteServerNames)
        {
            LocalServerName = localServerName;
            RemoteServerNames = remoteServerNames;
        }
    }

    public interface IHierarchyMetadata
    {
        IReadOnlyList<IServer> ServerList { get; set; }
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, IMetadataIdentifier>> ColumnRelationshipsByFromTableMultiPartIdentifierByToTableMultiPartIdentifier { get; set; }
    }

    public interface IGraphMetadata
    {
        IAdjacencyGraph<string, ITableRelationship> AdjacencyGraph { get; set; }
        IReadOnlyDictionary<string, int> WeightByTableMultiPartIdentifier { get; set; }
        IReadOnlyDictionary<string, int> WeightByColumnMultiPartIdentifier { get; set; }
    }

    public interface IWeightMetadata
    {
    }

    public interface IServer
    {
    }

    public interface IMetadataIdentifier
    {
    }

    public interface ITableRelationship
    {
    }

    public interface IAdjacencyGraph<TVertex, TEdge> : IEdge<string>
    {
    }

    public interface IConfigurationMetadata
    {
        int GetServerConnectionTimeout();
        IReadOnlyDictionary<string, string> GetIpAddressByServerName();
    }
}