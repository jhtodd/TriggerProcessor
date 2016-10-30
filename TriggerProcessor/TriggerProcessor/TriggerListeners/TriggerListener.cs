using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

using log4net;

namespace TriggerProcessor.TriggerListeners
{
    public class TriggerMessage
    {
        public TriggerMessage(string messageType, XDocument body, Guid conversationHandle)
        {
            MessageType = messageType;
            Body = body;
            ConversationHandle = conversationHandle;
        }

        public string MessageType { get; }
        public XDocument Body { get; }
        public Guid ConversationHandle { get; }
    }

    public class TriggerListenerOptions
    {
        public TriggerListenerOptions(
            string connectionString,
            string queueName,
            int maximumBatchSize = 50,
            TimeSpan? receiveTimeout = null)
        {
            ConnectionString = connectionString;
            QueueName = queueName;
            MaximumBatchSize = maximumBatchSize;
            ReceiveTimeout = receiveTimeout ?? TimeSpan.FromMilliseconds(5000);
        }

        public string ConnectionString { get; }
        public int MaximumBatchSize { get; }
        public string QueueName { get; }
        public TimeSpan ReceiveTimeout { get; }
    }

    public interface ITriggerListener
    {
        void Start(TriggerListenerOptions options, CancellationToken cancellationToken = default(CancellationToken));
        Task StopAsync();
    }

    public abstract class TriggerListener : ITriggerListener
    {
        const string END_OF_STREAM = "ServiceBroker_EndOfStream";

        bool _done;
        Task _listenTask;
        string _receiveSql;
        bool _running;

        protected TriggerListener(ILog log)
        {
            Log = log;
        }

        public void Start(TriggerListenerOptions options, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (_running)
            {
                throw new InvalidOperationException("The message listener is already running.");
            }

            Log.Info($"Beginning listening on queue {options.QueueName}.");

            _running = true;
            InitializeSqlStatements(options);

            _listenTask = Task.Run(async () => await Listen(options, cancellationToken).ConfigureAwait(false), cancellationToken);
        }

        public async Task StopAsync()
        {
            if (!_running)
            {
                throw new InvalidOperationException("The message listener is not currently running.");
            }

            Log.Info("Stopping listening.");

            _done = true;

            // Wait for the current iteration of the message loop to finish
            await _listenTask.ConfigureAwait(false);
            _listenTask = null;

            _running = false;
        }

        protected ILog Log { get; }

        protected abstract Task ProcessMessagesAsync(IReadOnlyList<TriggerMessage> message);

        protected async Task Listen(TriggerListenerOptions options, CancellationToken cancellationToken)
        {
            var currentIteration = 0L;

            while (!_done)
            {
                try
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                    }

                    currentIteration++;

                    Log.Debug($"Iteration {currentIteration}: Beginning.");

                    // Connect to the database
                    using (var connection = await GetOpenConnectionAsync(options).ConfigureAwait(false))
                    using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
                    {
                        Log.Debug($"Iteration {currentIteration}: Connection opened.");

                        // Read messages from the Service Broker queue
                        var messages = await ReceiveMessagesAsync(transaction, currentIteration).ConfigureAwait(false) ?? new List<TriggerMessage>();

                        try
                        {
                            // If the last message is an EndOfStream message, we need to end the conversation after
                            // processing all other messages.
                            var endOfStreamMessage = (messages.LastOrDefault()?.MessageType == END_OF_STREAM) ? messages.Last() : null;

                            if (endOfStreamMessage != null)
                            {
                                messages.RemoveAt(messages.Count - 1);
                            }

                            // Process the messages.
                            if (messages.Any())
                            {
                                Log.Debug($"Iteration {currentIteration}: Preparing to process {messages.Count} message(s).");

                                await ProcessMessagesAsync(messages).ConfigureAwait(false);

                                Log.Debug($"Iteration {currentIteration}: Successfully processed {messages.Count} message(s).");
                            }

                            // If all messages were processed successfully, commit the transaction to
                            // remove the messages from the queue.
                            await CommitAsync(transaction, currentIteration).ConfigureAwait(false);

                            if (endOfStreamMessage != null)
                            {
                                await EndConversationAsync(endOfStreamMessage.ConversationHandle, connection, currentIteration).ConfigureAwait(false);
                            }
                        }
                        catch (Exception ex)
                        {
                            Log.Error(
                                $"An error occurred while processing a message from queue {options.QueueName}. Potential data inconsistency if messages were processed. Rolling back RECEIVE action.",
                                ex);

                            // If there was an error processing any of the messages, roll back the transaction
                            // to return the messages to the queue.
                            await RollbackAsync(transaction, currentIteration).ConfigureAwait(false);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Log.Error("An error occurred while processing messages.", ex);

                    await Task.Delay(1000, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        async Task CommitAsync(SqlTransaction transaction, long currentIteration)
        {
            for (var i = 0; i < 5; i++)
            {
                Log.Debug($"Iteration {currentIteration}: Committing message reception, attempt {i}.");

                try
                {
                    transaction.Commit();
                    Log.Debug($"Iteration {currentIteration}: Commit successful.");
                    return;
                }
                catch (Exception ex)
                {
                    Log.Error("An error occurred while attempting to commit the message reception.", ex);
                    Log.Debug($"Iteration {currentIteration}: Retrying commit after failed attempt {i}.");
                    await Task.Delay(1000).ConfigureAwait(false);
                }
            }

            Log.Debug($"Iteration {currentIteration}: Commit failed after 5 attempts. Giving up.");
        }

        private async Task EndConversationAsync(Guid conversationHandle, SqlConnection connection, long currentIteration)
        {
            for (var i = 0; i < 5; i++)
            {
                Log.Debug($"Iteration {currentIteration}: Ending conversation {conversationHandle}, attempt {i}.");

                try
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = "END CONVERSATION @handle";
                        command.Parameters.AddWithValue("@handle", conversationHandle);

                        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }

                    Log.Debug($"Iteration {currentIteration}: Conversation {conversationHandle} ended successfully.");
                    return;
                }
                catch (Exception ex)
                {
                    Log.Error($"An error occurred while attempting to end conversation {conversationHandle}.", ex);
                    Log.Debug($"Iteration {currentIteration}: Retrying the end conversation action after failed attempt {i}.");
                    await Task.Delay(1000).ConfigureAwait(false);
                }
            }

            Log.Debug($"Iteration {currentIteration}: End conversation action failed after 5 attempts. Giving up.");
        }

        async Task<SqlConnection> GetOpenConnectionAsync(TriggerListenerOptions options)
        {
            var connection = new SqlConnection(options.ConnectionString);

            if (connection.State == ConnectionState.Closed)
            {
                await connection.OpenAsync().ConfigureAwait(false);
            }

            return connection;
        }

        void InitializeSqlStatements(TriggerListenerOptions options)
        {
            var commandBuilder = new SqlCommandBuilder();
            var sanitizedQueueName = commandBuilder.QuoteIdentifier(options.QueueName);

            _receiveSql = $@"WAITFOR (
                                 RECEIVE TOP({options.MaximumBatchSize})
                                     message_type_name,
                                     CAST(message_body AS XML) AS message_body,
                                     conversation_handle
                                 FROM messaging.{sanitizedQueueName}
                             ), TIMEOUT {options.ReceiveTimeout.TotalMilliseconds};";
        }

        async Task<List<TriggerMessage>> ReceiveMessagesAsync(SqlTransaction transaction, long currentIteration)
        {
            using (var command = transaction.Connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = _receiveSql;

                Log.Debug($"Iteration {currentIteration}: Preparing to execute RECEIVE command.");

                using (var reader = (await command.ExecuteReaderAsync().ConfigureAwait(false)))
                {
                    var message = reader.HasRows
                        ? $"Iteration {currentIteration}: RECEIVE command executed. Message(s) returned."
                        : $"Iteration {currentIteration}: RECEIVE command executed. No messages returned.";

                    Log.Debug(message);

                    var results = new List<TriggerMessage>();

                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var messageType = reader["message_type_name"].ToString();
                        var messageBody = messageType == END_OF_STREAM ? null : XDocument.Parse(reader["message_body"].ToString());
                        var conversationHandle = (Guid)reader["conversation_handle"];

                        results.Add(new TriggerMessage(messageType, messageBody, conversationHandle));
                    }

                    if (results.Any())
                    {
                        Log.Info($"Iteration {currentIteration}: Received {results.Count} message(s).");
                    }

                    return results;
                }
            }
        }

        async Task RollbackAsync(SqlTransaction transaction, long currentIteration)
        {
            for (var i = 0; i < 5; i++)
            {
                Log.Debug($"Iteration {currentIteration}: Rolling back message reception, attempt {i}.");

                try
                {
                    transaction.Rollback();

                    Log.Debug($"Iteration {currentIteration}: Rollback successful.");
                    return;
                }
                catch (Exception ex)
                {
                    Log.Error("An error occurred while attempting to roll back the message reception.", ex);
                    Log.Debug($"Iteration {currentIteration}: Retrying rollback after failed attempt {i}.");
                    await Task.Delay(1000).ConfigureAwait(false);
                }
            }

            Log.Debug($"Iteration {currentIteration}: Rollback failed after 5 attempts. Giving up.");
        }
    }
}