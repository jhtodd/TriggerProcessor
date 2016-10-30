using System;
using System.Configuration;

using TriggerProcessor.TriggerListeners;

namespace TriggerProcessor
{
    public class TriggerProcessorService
    {
        private readonly IProductionItemListener _productionItemListener;

        public TriggerProcessorService(IProductionItemListener productionItemListener)
        {
            _productionItemListener = productionItemListener;
        }

        public void Start()
        {
            _productionItemListener.Start(new TriggerListenerOptions(
                connectionString: ConfigurationManager.ConnectionStrings["QueueDatabase"].ConnectionString,
                queueName: "ProductionItemTriggerQueue",
                maximumBatchSize: 50,
                receiveTimeout: TimeSpan.FromMilliseconds(int.Parse(ConfigurationManager.AppSettings["ReceiveTimeoutMilliseconds"]))));
        }

        public void Stop()
        {
            _productionItemListener.StopAsync().Wait();
        }
    }
}