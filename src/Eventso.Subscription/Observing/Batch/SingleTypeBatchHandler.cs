using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.Batch
{
    public sealed class SingleTypeBatchHandler<T> : IBatchHandler<T>
        where T : IMessage
    {
        private readonly IMessageBatchPipelineAction _pipelineAction;

        public SingleTypeBatchHandler(IMessageBatchPipelineAction pipelineAction)
        {
            _pipelineAction = pipelineAction;
        }

        public Task Handle(IConvertibleCollection<T> messages, CancellationToken token)
        {
            if (messages.Count == 0)
                return Task.CompletedTask;

            return HandleTyped((dynamic)messages[0].GetPayload(), messages, token);
        }

        private Task HandleTyped<TPayload>(TPayload sample, IConvertibleCollection<T> convertibles, CancellationToken token)
        {
            return _pipelineAction.Invoke(convertibles.Convert(m => (TPayload)m.GetPayload()), token);
        }
    }
}