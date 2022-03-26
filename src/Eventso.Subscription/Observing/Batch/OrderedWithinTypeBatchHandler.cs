using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.Batch
{
    public sealed class OrderedWithinTypeBatchHandler<TEvent> : IBatchHandler<TEvent>
        where TEvent : IEvent
    {
        private readonly IMessageBatchPipelineAction _pipelineAction;

        public OrderedWithinTypeBatchHandler(IMessageBatchPipelineAction pipelineAction)
        {
            _pipelineAction = pipelineAction;
        }

        public async Task Handle(IConvertibleCollection<TEvent> events, CancellationToken token)
        {
            if (events.Count == 0)
                return;

            var groupedByType = events.GroupBy(
                m => m.GetMessage().GetType(),
                m => m.GetMessage());

            foreach (var group in groupedByType)
                await HandleTyped((dynamic)group.First(), @group, token);
        }

        private Task HandleTyped<TPayload>(TPayload sample, IEnumerable<object> messages, CancellationToken token)
        {
            return _pipelineAction.Invoke(messages.Cast<TPayload>().ToArray(), token);
        }
    }
}