using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Eventso.Subscription.Observing.Batch
{
    internal sealed class Buffer<T>
    {
        private readonly int _maxBatchSize;
        private readonly int _maxBufferSize;
        private readonly TimeSpan _timeout;
        private readonly ITargetBlock<Batch> _target;

        private PooledList<BufferedMessage> _messages;
        private int _payloadMessagesCount;
        private DateTime _batchStartTime;
        private DateTime _bufferStartTime;

        public Buffer(
            int maxBatchSize,
            TimeSpan timeout,
            ITargetBlock<Batch> target,
            int maxBufferSize)
        {
            if (maxBatchSize <= 1)
                throw new ArgumentException("Buffer size must be greater than 1.");

            _maxBatchSize = maxBatchSize;
            _maxBufferSize = maxBufferSize;
            _timeout = timeout;
            _target = target;

            _messages = new PooledList<BufferedMessage>(maxBatchSize);
        }

        public async Task Add(T message, bool skipped, CancellationToken token)
        {
            if (_target.Completion.IsFaulted)
                await _target.Completion;

            if (_target.Completion.IsCompleted)
                throw new InvalidOperationException("Target already completed.");

            await AddMessage(new BufferedMessage(message, skipped), token);

            await CheckTimeout(token);
        }

        public Task CheckTimeout(CancellationToken token)
        {
            if (_messages.Count == 0 || _timeout == Timeout.InfiniteTimeSpan)
                return Task.CompletedTask;

            var checkTimeoutStartTime = _payloadMessagesCount == 0
                ? _bufferStartTime
                : _batchStartTime;

            return checkTimeoutStartTime + _timeout <= DateTime.UtcNow
                ? TriggerSend(token)
                : Task.CompletedTask;
        }

        public async Task TriggerSend(CancellationToken token)
        {
            if (_messages.Count == 0)
                return;

            var batch = new Batch(_messages, _payloadMessagesCount);

            _messages = new PooledList<BufferedMessage>(_maxBatchSize);
            _payloadMessagesCount = 0;

            if (!await _target.SendAsync(batch, token))
                throw new InvalidOperationException("Unable to send data to target");
        }

        private Task AddMessage(BufferedMessage message, CancellationToken token)
        {
            if (_messages.Count == 0)
                _bufferStartTime = DateTime.UtcNow;

            _messages.Add(message);

            if (!message.Skipped)
            {
                if (_payloadMessagesCount == 0)
                    _batchStartTime = DateTime.UtcNow;

                ++_payloadMessagesCount;
            }

            if (_payloadMessagesCount >= _maxBatchSize || _messages.Count >= _maxBufferSize)
                return TriggerSend(token);

            return Task.CompletedTask;
        }

        [StructLayout(LayoutKind.Auto)]
        public readonly struct BufferedMessage
        {
            public readonly T Message;
            public readonly bool Skipped;

            public BufferedMessage(T message, bool skipped)
            {
                Message = message;
                Skipped = skipped;
            }
        }

        [StructLayout(LayoutKind.Auto)]
        public readonly struct Batch
        {
            public readonly PooledList<BufferedMessage> Messages;
            public readonly int PayloadMessageCount;

            public Batch(PooledList<BufferedMessage> messages, int payloadMessageCount)
            {
                Messages = messages;
                PayloadMessageCount = payloadMessageCount;
            }
        }
    }
}