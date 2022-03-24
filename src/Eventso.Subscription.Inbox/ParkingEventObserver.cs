using System;
using System.Threading;
using System.Threading.Tasks;
using Eventso.Subscription.Configurations;
using Eventso.Subscription.Observing;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Inbox
{
    public sealed class ParkingEventObserver<T> : IObserver<T>
        where T : IMessage
    {
        private readonly InboxConfiguration _inboxConfiguration;
        private readonly IObserver<T> _observer;
        private ILogger<ParkingEventObserver<T>> _logger;

        public ParkingEventObserver(
            ILoggerFactory loggerFactory,
            IMessagePipelineFactory pipelineFactory,
            HandlerConfiguration configuration,
            InboxConfiguration inboxConfiguration,
            IConsumer<T> consumer,
            IMessageHandlersRegistry messageHandlersRegistry)
        {
            _inboxConfiguration = inboxConfiguration;
            _observer = new MessageObserver<T>(
                pipelineFactory.Create(configuration), 
                consumer,
                messageHandlersRegistry,
                true, 
                new DeferredAckConfiguration(),
                loggerFactory.CreateLogger<MessageObserver<T>>());

            _logger = loggerFactory.CreateLogger<ParkingEventObserver<T>>();
        }

        public async Task OnMessageAppeared(T message, CancellationToken token)
        {
            try
            {
                await _observer.OnMessageAppeared(message, token);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                if (token.IsCancellationRequested)
                    throw;

                await Park(message.GetKey(), message.GetPayload(), ex);
            }
        }

        public void Reset()
        {
            throw new System.NotImplementedException();
        }

        private Task Park(Guid key, object value, Exception ex)
        {
            throw new System.NotImplementedException();
        }
    }
}