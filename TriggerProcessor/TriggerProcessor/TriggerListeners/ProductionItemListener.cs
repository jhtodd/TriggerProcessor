using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using log4net;
using TriggerProcessor.TriggerListeners.XmlParsers;

namespace TriggerProcessor.TriggerListeners
{
    public interface IProductionItemListener : ITriggerListener
    {
    }

    public class ProductionItemListener : TriggerListener, IProductionItemListener
    {
        private readonly ITriggerXmlParser<ProductionItemRow> _parser;

        public ProductionItemListener(ILog log, ITriggerXmlParser<ProductionItemRow> parser) : base(log)
        {
            _parser = parser;
        }

        protected override Task ProcessMessagesAsync(IReadOnlyList<TriggerMessage> messages)
        {
            foreach (var message in messages)
            {
                var data = _parser.ParseTriggerXml(message.Body);

                if (data.Inserted.Any()) Log.Info($"{data.TransactionId} Inserted: {data.Inserted.Count}.");
                if (data.Deleted.Any()) Log.Info($"{data.TransactionId} Deleted: {data.Deleted.Count}.");
                if (data.Modified.Any()) Log.Info($"{data.TransactionId} Modified: {data.Modified.Count}.");
            }

            return Task.FromResult(0);
        }
    }
}