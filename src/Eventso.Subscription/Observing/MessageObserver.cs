using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Eventso.Subscription.Configurations;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Observing
{
    public sealed class MessageObserver<T> : IObserver<T>
        where T : IMessage
    {
        private readonly IMessagePipelineAction _pipelineAction;
        private readonly IConsumer<T> _consumer;
        private readonly IMessageHandlersRegistry _messageHandlersRegistry;

        private readonly bool _skipUnknown;
        private readonly DeferredAckConfiguration _deferredAckConfiguration;
        private readonly ILogger<MessageObserver<T>> _logger;
        private readonly List<T> _skipped = new();
        private DateTime _deferredAckStartTime;

        public MessageObserver(
            IMessagePipelineAction pipelineAction,
            IConsumer<T> consumer,
            IMessageHandlersRegistry messageHandlersRegistry,
            bool skipUnknown,
            DeferredAckConfiguration deferredAckConfiguration,
            ILogger<MessageObserver<T>> logger)
        {
            _pipelineAction = pipelineAction;
            _consumer = consumer;
            _messageHandlersRegistry = messageHandlersRegistry;
            _skipUnknown = skipUnknown;
            _deferredAckConfiguration = deferredAckConfiguration;
            _logger = logger;
        }

        public async Task OnMessageAppeared(T message, CancellationToken token)
        {
            if (message.CanSkip(_skipUnknown))
            {
                DeferAck(message);
                return;
            }

            var hasHandler = _messageHandlersRegistry.ContainsHandlersFor(
                message.GetPayload().GetType(), out var handlerKind);

            if (!hasHandler)
            {
                DeferAck(message);
                return;
            }

            var metadata = message.GetMetadata();

            using var scope = metadata.Count > 0 ? _logger.BeginScope(metadata) : null;

            AckDeferredMessages();

            if ((handlerKind & HandlerKind.Single) == 0)
                throw new InvalidOperationException(
                    $"There is no single message handler for subscription {_consumer.Subscription}");

            dynamic payload = message.GetPayload();

            await _pipelineAction.Invoke(payload, token);

            _consumer.Acknowledge(message);
        }

        public Task OnMessageTimeout(CancellationToken token)
        {
            if (_skipped.Count > 0 && IsDeferredAckThresholdCrossed())
                AckDeferredMessages();

            return Task.CompletedTask;
        }

        private void DeferAck(in T skippedMessage)
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