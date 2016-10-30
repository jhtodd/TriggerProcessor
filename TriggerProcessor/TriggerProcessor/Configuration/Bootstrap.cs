using System;

using StructureMap;

namespace TriggerProcessor.Configuration
{
    public static class Bootstrap
    {
        public static IContainer Initialize()
        {
            return new Container(config =>
            {
                config.AddRegistry<TriggerProcessorRegistry>();
            });
        }
    }
}