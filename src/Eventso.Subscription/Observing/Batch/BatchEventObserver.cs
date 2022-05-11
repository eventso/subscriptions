using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Eventso.Subscription.Configurations;
using Eventso.Subscription.Observing.Batch.ErrorHandling;

namespace Eventso.Subscription.Observing.Batch
{
    public sealed class BatchEventObserver<TEvent> : IObserver<TEvent>, IDisposable
        where TEvent : IEvent
    {
        private readonly IEventHandler<TEvent> _handler;
        private readonly ActionBlock<Buffer<TEvent>.Batch> _actionBlock;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly IConsumer<TEvent> _consumer;
        private readonly IMessageHandlersRegistry _messageHandlersRegistry;
        private readonly bool _skipUnknown;
        private readonly Buffer<TEvent> _buffer;
        private readonly IFailedBufferProcessor<TEvent> _failedBufferProcessor;

        private bool _completed;
        private bool _disposed;

        public BatchEventObserver(
            BatchConfiguration config,
            IEventHandler<TEvent> handler,
            IConsumer<TEvent> consumer,
            IMessageHandlersRegistry messageHandlersRegistry,
            bool skipUnknown = true)
        {
            _handler = handler;
            _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
            _messageHandlersRegistry = messageHandlersRegistry;
            _skipUnknown = skipUnknown;

            _failedBufferProcessor = ResolveFailedBufferProcessor(
                config.FailedBatchProcessingStrategy,
                handler,
                consumer);

            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(consumer.CancellationToken);

            _actionBlock = new ActionBlock<Buffer<TEvent>.Batch>(
                async batch =>
                {
                    using (batch.Events)
                        await HandleBatch(batch.Events, batch.ToBeHandledEventCount);
                },
                new ExecutionDataflowBlockOptions
                {
                    CancellationToken = _cancellationTokenSource.Token,
                    BoundedCapacity = 1
                });

            _buffer = new Buffer<TEvent>(
                config.MaxBatchSize,
                config.BatchTriggerTimeout,
                _actionBlock,
                config.GetMaxBufferSize());
        }

        public async Task OnEventAppeared(TEvent @event, CancellationToken token)
        {
            await CheckState();

            var skipped = @event.CanSkip(_skipUnknown) ||
                          !_messageHandlersRegistry.ContainsHandlersFor(@event.GetMessage().GetType(), out _);

            await _buffer.Add(@event, skipped, token);
        }

        public async Task OnEventTimeout(CancellationToken token)
        {
            await CheckState();

            await _buffer.CheckTimeout(token);
        }

        public async Task Complete(CancellationToken token)
        {
            CheckDisposed();

            if (_actionBlock.Completion.IsFaulted)
                await _actionBlock.Completion;

            _completed = true;
            await _buffer.TriggerSend(token);

            _actionBlock.Complete();
            await _actionBlock.Completion;
        }

        public void Dispose()
        {
            _disposed = true;
            _actionBlock.Complete();

            if (!_cancellationTokenSource.IsCancellationRequested)
                _cancellationTokenSource.Cancel();

            _cancellationTokenSource.Dispose();
        }

        private Task CheckState()
        {
            CheckDisposed();

            if (_completed)
                throw new InvalidOperationException("Batch observer is completed and can't accept more messages");

            if (_actionBlock.Completion.IsFaulted)
                return _actionBlock.Completion;

            _cancellationTokenSource.Token.ThrowIfCancellationRequested();

            return Task.CompletedTask;
        }

        private async Task HandleBatch(PooledList<Buffer<TEvent>.BufferedEvent> events, int toBeHandledEventCount)
        {
            if (events.Count == 0)
                return;

            try
            {
                await HandleEvents(events, toBeHandledEventCount);
            }
            catch
            {
                _consumer.Cancel();

                throw;
            }
        }

        private async Task HandleEvents(PooledList<Buffer<TEvent>.BufferedEvent> events, int toBeHandledEventCount)
        {
            try
            {
                var allEvents = new EventsCollection(events);

                if (allEvents.Count == toBeHandledEventCount)
                {
                    await _handler.Handle(allEvents, _cancellationTokenSource.Token);
                }
                else if (toBeHandledEventCount > 0)
                {
                    using var eventsToHandle = GetEventsToHandle(events, toBeHandledEventCount);

                    await _handler.Handle(eventsToHandle, _cancellationTokenSource.Token);
                }

                _consumer.Acknowledge(allEvents);
            }
            catch
            {
                await _failedBufferProcessor.Process(events, _cancellationTokenSource.Token);

                throw;
            }
        }

        private static PooledList<TEvent> GetEventsToHandle(
            IReadOnlyList<Buffer<TEvent>.BufferedEvent> bufferedEvents,
            int eventsToHandleCount)
        {
            var eventsToHandle = new PooledList<TEvent>(eventsToHandleCount);

            foreach (var bufferedEvent in bufferedEvents)
            {
                if (!bufferedEvent.Skipped)
                    eventsToHandle.Add(bufferedEvent.Event);
            }

            return eventsToHandle;
        }

        private void CheckDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException("Batch observer is disposed");
        }

        private static IFailedBufferProcessor<TEvent> ResolveFailedBufferProcessor(
            FailedBatchProcessingStrategy failedBatchProcessingStrategy,
            IEventHandler<TEvent> handler,
            IConsumer<TEvent> consumer)
        {
            switch (failedBatchProcessingStrategy)
            {
                case FailedBatchProcessingStrategy.None:
                    return new DefaultFailedBufferProcessor<TEvent>();
                case FailedBatchProcessingStrategy.Breakdown:
                    return new BreakdownFailedBufferProcessor<TEvent>(handler, consumer);
                default:
                    throw new ArgumentOutOfRangeException(nameof(failedBatchProcessingStrategy),
                        failedBatchProcessingStrategy, null);
            }
        }

        private sealed class EventsCollection : IConvertibleCollection<TEvent>
        {
            private readonly PooledList<Buffer<TEvent>.BufferedEvent> _messages;

            public EventsCollection(PooledList<Buffer<TEvent>.BufferedEvent> messages)
                => _messages = messages;

            public int Count => _messages.Count;

            public TEvent this[int index]
                => _messages[index].Event;

            public IReadOnlyCollection<TOut> Convert<TOut>(Converter<TEvent, TOut> converter)
                => _messages.Convert(i => converter(i.Event));

            public bool OnlyContainsSame<TValue>(Func<TEvent, TValue> valueConverter)
            {
                if (Count == 0)
                    return true;

                var comparer = EqualityComparer<TValue>.Default;
                var sample = valueConverter(_messages[0].Event);

                foreach (var item in _messages.Segment)
                {
                    if (!comparer.Equals(valueConverter(item.Event), sample))
                        return false;
                }

                return true;
            }

            public IEnumerator<TEvent> GetEnumerator()
            {
                foreach (var item in _messages.Segment)
                    yield return item.Event;
            }

            IEnumerator IEnumerable.GetEnumerator()
                => GetEnumerator();
        }
    }
}