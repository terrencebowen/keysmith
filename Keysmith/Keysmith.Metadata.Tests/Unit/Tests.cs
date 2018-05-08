using Keysmith.Metadata.Model;
using Ploeh.AutoFixture;
using System;
using System.Collections.Generic;
using System.Net.NetworkInformation;
using Telerik.JustMock;
using Xunit;
using Mock = Telerik.JustMock.Mock;

namespace Keysmith.Metadata.Tests.Unit
{
    public class MetadataRepositoryTests
    {
        [Fact]
        public void CreateMetadata()
        {
            const Behavior mockBehavior = Behavior.Strict;

            var metadataBuilder = Mock.Create<IMetadataBuilder>(mockBehavior);
            var testObject = new MetadataRepository(metadataBuilder);
            var environmentMetadata = Mock.Create<IEnvironmentMetadata>(mockBehavior);
            var hierarchyMetadata = Mock.Create<IHierarchyMetadata>(mockBehavior);
            var graphMetadata = Mock.Create<IGraphMetadata>(mockBehavior);
            var metadata = new Model.Metadata(environmentMetadata, hierarchyMetadata, graphMetadata);

            Mock.Arrange(() => metadataBuilder.BuildEnvironmentMetadata()).Returns(environmentMetadata);
            Mock.Arrange(() => metadataBuilder.BuildHierarchyMetadata(environmentMetadata)).Returns(hierarchyMetadata);
            Mock.Arrange(() => metadataBuilder.BuildGraphMetadata(hierarchyMetadata)).Returns(graphMetadata);

            var result = testObject.CreateMetadata();

            Assert.Same(metadata.EnvironmentMetadata, result.EnvironmentMetadata);
            Assert.Same(metadata.HierarchyMetadata, result.HierarchyMetadata);
            Assert.Same(metadata.GraphMetadata, result.GraphMetadata);
        }
    }

    public class MetadataBuilderTests
    {
        private readonly string _localServerName;
        private readonly IReadOnlyDictionary<string, string> _ipAddressByServerName;
        private readonly IEnumerator<KeyValuePair<string, string>> _ipAddressByServerNameEnumerator;
        private readonly KeyValuePair<string, string> _keyValuePair;
        private readonly string _ipAddress;
        private readonly IReadOnlyList<string> _remoteServerNames;
        private readonly Ping _ping;
        private readonly PingReply _pingReply;
        private readonly byte[] _buffer;
        private readonly PingOptions _pingOptions;
        private readonly int _timeout;

        private readonly IConfigurationMetadata _configurationMetadata;
        private readonly IPingCreator _pingCreator;
        private readonly IPingReplyEvaluator _pingReplyEvaluator;
        private readonly IMetadataBuilder _testObject;

        public MetadataBuilderTests()
        {
            const Behavior behavior = Behavior.Strict;

            var fixture = new Fixture();
            var key = fixture.Create<string>();
            var value = fixture.Create<string>();

            _localServerName = Environment.MachineName.ToLower();
            _ipAddressByServerName = Mock.Create<IReadOnlyDictionary<string, string>>(behavior);
            _ipAddressByServerNameEnumerator = Mock.Create<IEnumerator<KeyValuePair<string, string>>>(behavior);
            _keyValuePair = new KeyValuePair<string, string>(key, value);
            _ipAddress = _keyValuePair.Value;
            _remoteServerNames = new List<string> { _keyValuePair.Key };
            _ping = Mock.Create<Ping>(behavior);
            _pingReply = Mock.Create<PingReply>(behavior);
            _buffer = new byte[0];
            _pingOptions = new PingOptions(ttl: 64, dontFragment: true);
            _timeout = 1;
            _configurationMetadata = Mock.Create<IConfigurationMetadata>(behavior);
            _pingCreator = Mock.Create<IPingCreator>(behavior);
            _pingReplyEvaluator = Mock.Create<IPingReplyEvaluator>(behavior);
            _testObject = new MetadataBuilder(_configurationMetadata, _pingCreator, _pingReplyEvaluator);
        }

        [Fact]
        public void BuildEnvironmentMetadata()
        {
            Mock.Arrange(() => _ipAddressByServerName.GetEnumerator()).Returns(_ipAddressByServerNameEnumerator);
            Mock.Arrange(() => _ipAddressByServerNameEnumerator.MoveNext()).Returns(true);
            Mock.Arrange(() => _ipAddressByServerNameEnumerator.Current).Returns(_keyValuePair);
            Mock.Arrange(() => _configurationMetadata.GetIpAddressByServerName()).Returns(_ipAddressByServerName);
            Mock.Arrange(() => _pingCreator.CreatePing()).Returns(_ping);
            Mock.Arrange(() => _ping.Send(_ipAddress, _timeout, _buffer, Arg.Matches<PingOptions>(pingOptions => pingOptions.Ttl == _pingOptions.Ttl && pingOptions.DontFragment == _pingOptions.DontFragment))).Returns(_pingReply);
            Mock.Arrange(() => _pingReplyEvaluator.IsIpStatusSuccess(_pingReply)).Returns(true);

            Mock.Arrange(() => ((IDisposable)_ping).Dispose()).When(() => true).DoInstead(() =>
           {
               Mock.Arrange(() => _ipAddressByServerNameEnumerator.MoveNext()).Returns(false);
               Mock.Arrange(() => _ipAddressByServerNameEnumerator.Dispose());
           });

            var result = _testObject.BuildEnvironmentMetadata();

            Assert.Equal(_localServerName, result.LocalServerName);
            Assert.Equal(_remoteServerNames, result.RemoteServerNames);
        }

        [Fact]
        public void PingReply()
        {
            Mock.Arrange(() => _ipAddressByServerName.GetEnumerator()).Returns(_ipAddressByServerNameEnumerator);
            Mock.Arrange(() => _ipAddressByServerNameEnumerator.MoveNext()).Returns(true);
            Mock.Arrange(() => _ipAddressByServerNameEnumerator.Current).Returns(_keyValuePair);
            Mock.Arrange(() => _configurationMetadata.GetIpAddressByServerName()).Returns(_ipAddressByServerName);
            Mock.Arrange(() => _pingCreator.CreatePing()).Returns(_ping);
            Mock.Arrange(() => _ping.Send(_ipAddress, _timeout, _buffer, Arg.Matches<PingOptions>(pingOptions => pingOptions.Ttl == _pingOptions.Ttl && pingOptions.DontFragment == _pingOptions.DontFragment))).When(() => true).DoInstead(() =>
            {
                Mock.Arrange(() => ((IDisposable)_ping).Dispose());
                Mock.Arrange(() => _ipAddressByServerNameEnumerator.MoveNext()).Returns(false);
                Mock.Arrange(() => _ipAddressByServerNameEnumerator.Dispose());
            })
            .Returns(default(PingReply));

            var result = _testObject.BuildEnvironmentMetadata();

            Assert.Equal(_localServerName, result.LocalServerName);
            Assert.Empty(result.RemoteServerNames);
        }

        [Fact]
        public void IpStatus()
        {
            Mock.Arrange(() => _ipAddressByServerName.GetEnumerator()).Returns(_ipAddressByServerNameEnumerator);
            Mock.Arrange(() => _ipAddressByServerNameEnumerator.MoveNext()).Returns(true);
            Mock.Arrange(() => _ipAddressByServerNameEnumerator.Current).Returns(_keyValuePair);
            Mock.Arrange(() => _configurationMetadata.GetIpAddressByServerName()).Returns(_ipAddressByServerName);
            Mock.Arrange(() => _pingCreator.CreatePing()).Returns(_ping);
            Mock.Arrange(() => _ping.Send(_ipAddress, _timeout, _buffer, Arg.Matches<PingOptions>(pingOptions => pingOptions.Ttl == _pingOptions.Ttl && pingOptions.DontFragment == _pingOptions.DontFragment))).Returns(_pingReply);

            Mock.Arrange(() => _pingReplyEvaluator.IsIpStatusSuccess(_pingReply)).When(() => true).DoInstead(() =>
            {
                Mock.Arrange(() => ((IDisposable)_ping).Dispose());
                Mock.Arrange(() => _ipAddressByServerNameEnumerator.MoveNext()).Returns(false);
                Mock.Arrange(() => _ipAddressByServerNameEnumerator.Dispose());
            })
            .Returns(false);

            var result = _testObject.BuildEnvironmentMetadata();

            Assert.Equal(_localServerName, result.LocalServerName);
            Assert.Empty(result.RemoteServerNames);
        }

        [Fact]
        public void NetworkInformationException()
        {
            Mock.Arrange(() => _ipAddressByServerName.GetEnumerator()).Returns(_ipAddressByServerNameEnumerator);
            Mock.Arrange(() => _ipAddressByServerNameEnumerator.MoveNext()).Returns(true);
            Mock.Arrange(() => _ipAddressByServerNameEnumerator.Current).Returns(_keyValuePair);
            Mock.Arrange(() => _configurationMetadata.GetIpAddressByServerName()).Returns(_ipAddressByServerName);
            Mock.Arrange(() => _pingCreator.CreatePing()).Returns(_ping);
            Mock.Arrange(() => _ping.Send(_ipAddress, _timeout, _buffer, Arg.Matches<PingOptions>(pingOptions => pingOptions.Ttl == _pingOptions.Ttl && pingOptions.DontFragment == _pingOptions.DontFragment))).When(() => true).DoInstead(() =>
            {
                Mock.Arrange(() => ((IDisposable)_ping).Dispose());
                Mock.Arrange(() => _ipAddressByServerNameEnumerator.MoveNext()).Returns(false);
                Mock.Arrange(() => _ipAddressByServerNameEnumerator.Dispose());
            })
            .Throws<NetworkInformationException>();

            var result = _testObject.BuildEnvironmentMetadata();

            Assert.Equal(_localServerName, result.LocalServerName);
            Assert.Empty(result.RemoteServerNames);
        }

        [Fact(Skip = nameof(NotImplementedException))]
        public void BuildHierarchyMetadata()
        {
            throw new NotImplementedException();
        }

        [Fact(Skip = nameof(NotImplementedException))]
        public void BuildGraphMetadata()
        {
            throw new NotImplementedException();
        }
    }
}