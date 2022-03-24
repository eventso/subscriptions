using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.Batch
{
    public sealed class OrderedWithinKeyBatchHandler<T> : IBatchHandler<T>
        where T : IMessage
    {
        private readonly IMessageBatchPipelineAction _pipelineAction;
        private readonly SingleTypeBatchHandler<T> _singleTypeHandler;

        public OrderedWithinKeyBatchHandler(IMessageBatchPipelineAction pipelineAction)
        {
            _pipelineAction = pipelineAction;
            _singleTypeHandler = new SingleTypeBatchHandler<T>(_pipelineAction);
        }

        public async Task Handle(IConvertibleCollection<T> messages, CancellationToken token)
        {
            if (messages.Count == 0)
                return;

            if (messages.OnlyContainsSame(m => m.GetPayload().GetType()))
            {
                await _singleTypeHandler.Handle(messages, token);

                return;
            }

            using var batches = GetBatches(messages);

            foreach (var batch in batches)
            {
                using var items = batch.Items;

                await HandleTyped((dynamic)items[0], items, token);
            }
        }

        private async Task HandleTyped<TPayload>(
            TPayload sample,
            IConvertibleCollection<object> payloads,
            CancellationToken token)
        {
            await _pipelineAction.Invoke(payloads.Convert(m => (TPayload)m), token);
        }
        
        private static PooledList<TypedBatch> GetBatches(IEnumerable<T> messages)
        {
            var streams = messages.GroupBy(m => m.GetKey(), m => m.GetPayload());
            var batches = new PooledList<TypedBatch>(4);

            foreach (var stream in streams)
            {
                var batchIndex = 0;

                foreach (var messagePayload in stream)
                {
                    while (true)
                    {
                        if (batchIndex == batches.Count)
                        {
                            batches.Add(new TypedBatch(messagePayload));
                            break;
                        }

                        var currentBatch = batches[batchIndex];

                        if (currentBatch.HasEqualType(messagePayload))
                        {
                            currentBatch.Add(messagePayload);
                            break;
                        }

                        ++batchIndex;
                    }
                }
            }

            return batches;
        }

        private readonly struct TypedBatch
        {
            private readonly Type _type;

            public readonly PooledList<object> Items;

            public TypedBatch(object item)
            {
                _type = item.GetType();
                Items = new PooledList<object>(4);
                Items.Add(item);
            }

            public bool HasEqualType(object item) =>
                _type == item.GetType();

            public void Add(object item)
            {
                if (!HasEqualType(item))
                    throw new ArgumentException("Type mismatch");

                Items.Add(item);
            }
        }
    }
}