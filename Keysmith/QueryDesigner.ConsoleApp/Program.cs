using Keysmith.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;

namespace QueryDesigner.ConsoleApp
{
    internal class Program
    {
        private static void Main()
        {
            Console.Title = "Query Designer";
			
            new MetadataProvider().Start();
        }
    }
}