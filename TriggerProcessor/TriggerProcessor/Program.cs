using System;

using log4net.Config;
using Topshelf;
using TriggerProcessor.Configuration;

namespace TriggerProcessor
{
    class Program
    {
        static void Main(string[] args)
        {
            XmlConfigurator.Configure();

            var container = Bootstrap.Initialize();

            HostFactory.Run(x =>
            {
                x.UseLog4Net();

                x.Service<TriggerProcessorService>(s =>
                {
                    s.ConstructUsing(name => container.GetInstance<TriggerProcessorService>());
                    s.WhenStarted(tc => tc.Start());
                    s.WhenStopped(tc => tc.Stop());
                });

                x.RunAsLocalSystem();

                x.SetDescription("Trigger Message Processor");
                x.SetDisplayName("BAS-HD-TriggerMessageProcessor");
                x.SetServiceName("BAS-HD-TriggerMessageProcessor");
            });

            Console.ReadLine();
        }
    }
}