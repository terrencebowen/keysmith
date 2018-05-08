using Keysmith.Metadata.Model;
using System;
using System.Collections.Generic;
using System.Net.NetworkInformation;

namespace Keysmith.Metadata
{
    public interface IMetadataRepository
    {
        IMetadata CreateMetadata();
    }

    public class MetadataRepository : IMetadataRepository
    {
        private readonly IMetadataBuilder _metadataBuilder;

        public MetadataRepository(IMetadataBuilder metadataBuilder)
        {
            _metadataBuilder = metadataBuilder;
        }

        public IMetadata CreateMetadata()
        {
            var environmentMetadata = _metadataBuilder.BuildEnvironmentMetadata();
            var hierarchyMetadata = _metadataBuilder.BuildHierarchyMetadata(environmentMetadata);
            var graphMetadata = _metadataBuilder.BuildGraphMetadata(hierarchyMetadata);
            var metadata = new Model.Metadata(environmentMetadata, hierarchyMetadata, graphMetadata);

            return metadata;
        }
    }

    public interface IMetadataBuilder
    {
        IEnvironmentMetadata BuildEnvironmentMetadata();
        IHierarchyMetadata BuildHierarchyMetadata(IEnvironmentMetadata environmentMetadata);
        IGraphMetadata BuildGraphMetadata(IHierarchyMetadata hierarchyMetadata);
    }

    public class MetadataBuilder : IMetadataBuilder
    {
        private readonly IConfigurationMetadata _configurationMetadata;
        private readonly IPingCreator _pingCreator;
        private readonly IPingReplyEvaluator _pingReplyEvaluator;

        public MetadataBuilder(IConfigurationMetadata configurationMetadata,
                               IPingCreator pingCreator,
                               IPingReplyEvaluator pingReplyEvaluator)
        {
            _configurationMetadata = configurationMetadata;
            _pingCreator = pingCreator;
            _pingReplyEvaluator = pingReplyEvaluator;
        }

        public IEnvironmentMetadata BuildEnvironmentMetadata()
        {
            const int routingNodes = 64;
            const int timeout = 1;

            var buffer = new byte[0];

            var pingOptions = new PingOptions
            {
                Ttl = routingNodes,
                DontFragment = true
            };

            var ipAddressByServerName = _configurationMetadata.GetIpAddressByServerName();
            var remoteServerNames = new List<string>();

            foreach (var ipAddressByServerNamePair in ipAddressByServerName)
            {
                var ipAddress = ipAddressByServerNamePair.Value;

                using (var ping = _pingCreator.CreatePing())
                {
                    try
                    {
                        var pingReply = ping.Send(ipAddress, timeout, buffer, pingOptions);

                        if (pingReply == null)
                        {
                            continue;
                        }

                        var isIpStatusSuccess = _pingReplyEvaluator.IsIpStatusSuccess(pingReply);

                        if (isIpStatusSuccess)
                        {
                            var serverName = ipAddressByServerNamePair.Key;

                            remoteServerNames.Add(serverName);
                        }
                    }
                    catch (NetworkInformationException)
                    {
                        continue;
                    }
                }
            }

            var machineName = Environment.MachineName;
            var localServerName = machineName.ToLower();
            var environmentMetadata = new EnvironmentMetadata(localServerName, remoteServerNames);

            return environmentMetadata;
        }

        public IHierarchyMetadata BuildHierarchyMetadata(IEnvironmentMetadata environmentMetadata)
        {
            throw new NotImplementedException();
        }

        public IGraphMetadata BuildGraphMetadata(IHierarchyMetadata hierarchyMetadata)
        {
            throw new NotImplementedException();
        }
    }

    public interface IPingCreator
    {
        Ping CreatePing();
    }

    public class PingCreator : IPingCreator
    {
        public Ping CreatePing()
        {
            throw new NotImplementedException();
        }
    }

    public interface IPingReplyEvaluator
    {
        bool IsIpStatusSuccess(PingReply pingReply);
    }

    public class PingReplyEvaluator : IPingReplyEvaluator
    {
        public bool IsIpStatusSuccess(PingReply pingReply)
        {
            throw new NotImplementedException();
        }
    }
}