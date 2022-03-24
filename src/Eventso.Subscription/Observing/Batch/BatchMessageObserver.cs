using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Eventso.Subscription.Configurations;

namespace Eventso.Subscription.Observing.Batch
{
    public sealed class BatchMessageObserver<T> : IObserver<T>, IDisposable
        where T : IMessage
    {
        private readonly IBatchHandler<T> _handler;
        private readonly ActionBlock<Buffer<T>.Batch> _actionBlock;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly IConsumer<T> _consumer;
        private readonly IMessageHandlersRegistry _messageHandlersRegistry;
        private readonly bool _skipUnknown;
        private readonly Buffer<T> _buffer;

        private bool _completed;
        private bool _disposed;

        public BatchMessageObserver(
            BatchConfiguration config,
            IBatchHandler<T> handler,
            IConsumer<T> consumer,
            IMessageHandlersRegistry messageHandlersRegistry,
            bool skipUnknown = true)
        {
            _handler = handler;
            _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
            _messageHandlersRegistry = messageHandlersRegistry;
            _skipUnknown = skipUnknown;

            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
                consumer.CancellationToken);

            _actionBlock = new ActionBlock<Buffer<T>.Batch>(
                async batch =>
                {
                    using (batch.Messages)
                        await HandleBatch(batch.Messages, batch.PayloadMessageCount);
                },
                new ExecutionDataflowBlockOptions
                {
                    CancellationToken = _cancellationTokenSource.Token,
                    BoundedCapacity = 1
                });

            _buffer = new Buffer<T>(
                config.MaxBatchSize,
                config.BatchTriggerTimeout,
                _actionBlock,
                config.GetMaxBufferSize());
        }

        public async Task OnMessageAppeared(T message, CancellationToken token)
        {
            await CheckState();

            var skipped = message.CanSkip(_skipUnknown) ||
                          !_messageHandlersRegistry.ContainsHandlersFor(message.GetPayload().GetType(), out _);

            await _buffer.Add(message, skipped, token);
        }

        public async Task OnMessageTimeout(CancellationToken token)
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

        private async Task HandleBatch(PooledList<Buffer<T>.BufferedMessage> messages, int payloadMessageCount)
        {
            if (messages.Count == 0)
                return;

            var allMessages = new MessagesCollection(messages);

            try
            {
                if (messages.Count == payloadMessageCount)
                {
                    await _handler.Handle(allMessages, _cancellationTokenSource.Token);
                }
                else if (payloadMessageCount > 0)
                {
                    using var messagesToHandle = GetMessagesToHandle(messages, payloadMessageCount);

                    await _handler.Handle(messagesToHandle, _cancellationTokenSource.Token);
                }

                _consumer.Acknowledge(allMessages);
            }
            catch
            {
                _consumer.Cancel();
                throw;
            }
        }

        private static PooledList<T> GetMessagesToHandle(
            IReadOnlyList<Buffer<T>.BufferedMessage> messages, int handleMessageCount)
        {
            var list = new PooledList<T>(handleMessageCount);

            foreach (var message in messages)
            {
                if (!message.Skipped)
                    list.Add(message.Message);
            }

            return list;
        }

        private void CheckDisposed()
        {
            if (_disposed) throw new ObjectDisposedException("Batch observer is disposed");
        }

        private sealed class MessagesCollection : IConvertibleCollection<T>
        {
            private readonly PooledList<Buffer<T>.BufferedMessage> _messages;

            public MessagesCollection(PooledList<Buffer<T>.BufferedMessage> messages)
                => _messages = messages;

            public int Count => _messages.Count;

            public T this[int index]
                => _messages[index].Message;

            public IReadOnlyCollection<TOut> Convert<TOut>(Converter<T, TOut> converter)
                => _messages.Convert(i => converter(i.Message));

            public bool OnlyContainsSame<TValue>(Func<T, TValue> valueConverter)
            {
                if (Count == 0)
                    return true;

                var comparer = EqualityComparer<TValue>.Default;
                var sample = valueConverter(_messages[0].Message);
                
                foreach (var item in _messages.Segment)
                {
                    if (!comparer.Equals(valueConverter(item.Message), sample))
                        return false;
                }

                return true;
            }

            public IEnumerator<T> GetEnumerator()
            {
                foreach (var item in _messages.Segment)
                    yield return item.Message;
            }

            IEnumerator IEnumerable.GetEnumerator()
                => GetEnumerator();
        }
    }
}