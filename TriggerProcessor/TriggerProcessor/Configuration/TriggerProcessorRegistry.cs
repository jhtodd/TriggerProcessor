using System;

using log4net;
using StructureMap;
using TriggerProcessor.TriggerListeners;
using TriggerProcessor.TriggerListeners.XmlParsers;

namespace TriggerProcessor.Configuration
{
    public class TriggerProcessorRegistry : Registry
    {
        public TriggerProcessorRegistry()
        {
            For<ILog>().Use(c => LogManager.GetLogger(c.RootType));
            For<IProductionItemListener>().Use<ProductionItemListener>();
            
            Scan(scanner =>
            {
                scanner.AssemblyContainingType<TriggerProcessorRegistry>();
                scanner.ConnectImplementationsToTypesClosing(typeof(ITriggerXmlParser<>));
            });
        }
    }
}