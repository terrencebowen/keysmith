using System;
using System.Collections.Generic;
using Telerik.JustMock;

namespace Keysmith.Metadata.Tests.Extension
{
    public static class Mock
    {
        public static void ArrangeEnumeration(IReadOnlyDictionary<string, string> enumerableMock, KeyValuePair<string, string> current, Action action)
        {
            System.IO.File.AppendAllText(@"C:\Users\tbowen\Desktop\MessageWriter\MessageWriter\Notes\Messages.txt", $"{System.DateTime.Now:yyyyMMddhhmmssfff}.{System.IO.Path.GetFileName(System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName)}.Keysmith.Metadata.Tests.Extension.Mock.ArrangeEnumeration(System.Collections.Generic.IReadOnlyDictionary<string, string>, System.Collections.Generic.KeyValuePair<string, string>, System.Action)(enumerableMock = {Newtonsoft.Json.JsonConvert.SerializeObject(enumerableMock)}, current = {Newtonsoft.Json.JsonConvert.SerializeObject(current)}, action = {Newtonsoft.Json.JsonConvert.SerializeObject(action)}){System.Environment.NewLine}");

            System.IO.File.AppendAllText(@"C:\Users\tbowen\Desktop\MessageWriter\MessageWriter\Notes\Messages.txt", $"{System.DateTime.Now:yyyyMMddhhmmssfff}.{System.IO.Path.GetFileName(System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName)}.Keysmith.Metadata.Tests.Extension.Mock.ArrangeEnumeration(System.Collections.Generic.IReadOnlyDictionary<string, string>, System.Collections.Generic.KeyValuePair<string, string>, System.Action)(enumerableMock = {Newtonsoft.Json.JsonConvert.SerializeObject(enumerableMock)}, current = {Newtonsoft.Json.JsonConvert.SerializeObject(current)}, action = {Newtonsoft.Json.JsonConvert.SerializeObject(action)}){System.Environment.NewLine}");

            System.IO.File.AppendAllText(@"C:\Users\tbowen\Desktop\MessageWriter\MessageWriter\Notes\Messages.txt", $"{System.DateTime.Now:yyyyMMddhhmmssfff}.{System.IO.Path.GetFileName(System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName)}.Keysmith.Metadata.Tests.Extension.Mock.ArrangeEnumeration(System.Collections.Generic.IReadOnlyDictionary<string, string>, System.Collections.Generic.KeyValuePair<string, string>, System.Action)(enumerableMock = {Newtonsoft.Json.JsonConvert.SerializeObject(enumerableMock)}, current = {Newtonsoft.Json.JsonConvert.SerializeObject(current)}, action = {Newtonsoft.Json.JsonConvert.SerializeObject(action)}){System.Environment.NewLine}");

            System.IO.File.AppendAllText(@"C:\Users\tbowen\Desktop\MessageWriter\MessageWriter\Notes\Messages.txt", $"{System.DateTime.Now:yyyyMMddhhmmssfff}.{System.IO.Path.GetFileName(System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName)}.Keysmith.Metadata.Tests.Extension.Mock.ArrangeEnumeration(System.Collections.Generic.IReadOnlyDictionary<string, string>, System.Collections.Generic.KeyValuePair<string, string>, System.Action)(enumerableMock = {Newtonsoft.Json.JsonConvert.SerializeObject(enumerableMock)}, current = {Newtonsoft.Json.JsonConvert.SerializeObject(current)}, action = {Newtonsoft.Json.JsonConvert.SerializeObject(action)}){System.Environment.NewLine}");

            System.IO.File.AppendAllText(@"C:\Users\tbowen\Desktop\MessageWriter\MessageWriter\Notes\Messages.txt", $"{System.DateTime.Now:yyyyMMddhhmmssfff}.{System.IO.Path.GetFileName(System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName)}.Keysmith.Metadata.Tests.Extension.Mock.ArrangeEnumeration(System.Collections.Generic.IReadOnlyDictionary<string, string>, System.Collections.Generic.KeyValuePair<string, string>, System.Action)(enumerableMock = {Newtonsoft.Json.JsonConvert.SerializeObject(enumerableMock)}, current = {Newtonsoft.Json.JsonConvert.SerializeObject(current)}, action = {Newtonsoft.Json.JsonConvert.SerializeObject(action)}){System.Environment.NewLine}");

            System.IO.File.AppendAllText(@"C:\Users\tbowen\Desktop\MessageWriter\MessageWriter\Notes\Messages.txt", $"{System.DateTime.Now:yyyyMMddhhmmssfff}.{System.IO.Path.GetFileName(System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName)}.Keysmith.Metadata.Tests.Extension.Mock.ArrangeEnumeration(System.Collections.Generic.IReadOnlyDictionary<string, string>, System.Collections.Generic.KeyValuePair<string, string>, System.Action)(enumerableMock = {Newtonsoft.Json.JsonConvert.SerializeObject(enumerableMock)}, current = {Newtonsoft.Json.JsonConvert.SerializeObject(current)}, action = {Newtonsoft.Json.JsonConvert.SerializeObject(action)}){System.Environment.NewLine}");

            const Behavior behavior = Behavior.Strict;

            var enumeratorMock = Telerik.JustMock.Mock.Create<IEnumerator<KeyValuePair<string, string>>>(behavior);

            Telerik.JustMock.Mock.Arrange(() => enumerableMock.GetEnumerator()).Returns(enumeratorMock);
            Telerik.JustMock.Mock.Arrange(() => enumeratorMock.MoveNext()).Returns(true);
            Telerik.JustMock.Mock.Arrange(() => enumeratorMock.Current).Returns(current);

            action.Invoke();

            Telerik.JustMock.Mock.Arrange(() => enumeratorMock.MoveNext()).When(() => true).Returns(false);
            Telerik.JustMock.Mock.Arrange(() => ((IDisposable)enumeratorMock).Dispose());
        }
    }
}