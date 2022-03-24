using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.Batch
{
    public sealed class OrderedWithinTypeBatchHandler<T> : IBatchHandler<T>
        where T : IMessage
    {
        private readonly IMessageBatchPipelineAction _pipelineAction;

        public OrderedWithinTypeBatchHandler(IMessageBatchPipelineAction pipelineAction)
        {
            _pipelineAction = pipelineAction;
        }

        public async Task Handle(IConvertibleCollection<T> messages, CancellationToken token)
        {
            if (messages.Count == 0)
                return;

            var groupedByType = messages.GroupBy(
                m => m.GetPayload().GetType(),
                m => m.GetPayload());

            foreach (var group in groupedByType)
                await HandleTyped((dynamic)group.First(), @group, token);
        }

        private Task HandleTyped<TPayload>(TPayload sample, IEnumerable<object> messages, CancellationToken token)
        {
            return _pipelineAction.Invoke(messages.Cast<TPayload>().ToArray(), token);
        }
    }
}