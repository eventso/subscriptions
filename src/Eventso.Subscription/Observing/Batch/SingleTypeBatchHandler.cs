using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.Batch
{
    public sealed class SingleTypeBatchHandler<TEvent> : IBatchHandler<TEvent>
        where TEvent : IEvent
    {
        private readonly IMessageBatchPipelineAction _pipelineAction;

        public SingleTypeBatchHandler(IMessageBatchPipelineAction pipelineAction)
        {
            _pipelineAction = pipelineAction;
        }

        public Task Handle(IConvertibleCollection<TEvent> events, CancellationToken token)
        {
            if (events.Count == 0)
                return Task.CompletedTask;

            return HandleTyped((dynamic)events[0].GetMessage(), events, token);
        }

        private Task HandleTyped<TPayload>(TPayload sample, IConvertibleCollection<TEvent> convertibles, CancellationToken token)
        {
            return _pipelineAction.Invoke(convertibles.Convert(m => (TPayload)m.GetMessage()), token);
        }
    }
}