using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Eventso.Subscription.Configurations;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Observing
{
    public sealed class EventObserver<TEvent> : IObserver<TEvent>
        where TEvent : IEvent
    {
        private readonly IEventHandler<TEvent> _eventHandler;
        private readonly IConsumer<TEvent> _consumer;
        private readonly IMessageHandlersRegistry _messageHandlersRegistry;

        private readonly bool _skipUnknown;
        private readonly DeferredAckConfiguration _deferredAckConfiguration;
        private readonly ILogger<EventObserver<TEvent>> _logger;
        private readonly List<TEvent> _skipped = new();
        private DateTime _deferredAckStartTime;

        public EventObserver(
            IEventHandler<TEvent> eventHandler,
            IConsumer<TEvent> consumer,
            IMessageHandlersRegistry messageHandlersRegistry,
            bool skipUnknown,
            DeferredAckConfiguration deferredAckConfiguration,
            ILogger<EventObserver<TEvent>> logger)
        {
            _eventHandler = eventHandler;
            _consumer = consumer;
            _messageHandlersRegistry = messageHandlersRegistry;
            _skipUnknown = skipUnknown;
            _deferredAckConfiguration = deferredAckConfiguration;
            _logger = logger;
        }

        public async Task OnEventAppeared(TEvent @event, CancellationToken token)
        {
            if (@event.CanSkip(_skipUnknown))
            {
                DeferAck(@event);
                return;
            }

            var hasHandler = _messageHandlersRegistry.ContainsHandlersFor(
                @event.GetMessage().GetType(), out var handlerKind);

            if (!hasHandler)
            {
                DeferAck(@event);
                return;
            }

            var metadata = @event.GetMetadata();

            using var scope = metadata.Count > 0 ? _logger.BeginScope(metadata) : null;

            AckDeferredMessages();

            if ((handlerKind & HandlerKind.Single) == 0)
                throw new InvalidOperationException(
                    $"There is no single message handler for subscription {_consumer.Subscription}");

            await _eventHandler.Handle(@event, token);

            _consumer.Acknowledge(@event);
        }

        public Task OnEventTimeout(CancellationToken token)
        {
            if (_skipped.Count > 0 && IsDeferredAckThresholdCrossed())
                AckDeferredMessages();

            return Task.CompletedTask;
        }

        private void DeferAck(in TEvent skippedMessage)
        {
            if (_deferredAckConfiguration.MaxBufferSize == 0)
            {
                _consumer.Acknowledge(skippedMessage);
                return;
            }

            if (_skipped.Count == 0)
                _deferredAckStartTime = DateTime.UtcNow;

            _skipped.Add(skippedMessage);

            if (IsDeferredAckThresholdCrossed())
                AckDeferredMessages();
        }

        private bool IsDeferredAckThresholdCrossed()
        {
            return _skipped.Count >= _deferredAckConfiguration.MaxBufferSize
                   || _deferredAckConfiguration.Timeout != Timeout.InfiniteTimeSpan
                   && _deferredAckStartTime + _deferredAckConfiguration.Timeout <= DateTime.UtcNow;
        }

        private void AckDeferredMessages()
        {
            if (_skipped.Count == 0)
                return;

            _consumer.Acknowledge(_skipped);
            _skipped.Clear();
        }
    }
}