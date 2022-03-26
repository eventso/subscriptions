using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.Batch
{
    public sealed class SingleTypeLastByKeyBatchHandler<TEvent> : IBatchHandler<TEvent>
        where TEvent : IEvent
    {
        private readonly IMessageBatchPipelineAction _pipelineAction;

        public SingleTypeLastByKeyBatchHandler(IMessageBatchPipelineAction pipelineAction)
        {
            _pipelineAction = pipelineAction;
        }

        public Task Handle(IConvertibleCollection<TEvent> events, CancellationToken token)
        {
            if (events.Count == 0)
                return Task.CompletedTask;

            return HandleTyped((dynamic)events[0].GetMessage(), events, token);
        }

        private Task HandleTyped<TPayload>(TPayload sample, IReadOnlyList<TEvent> messages, CancellationToken token)
        {
            var dictionary = new Dictionary<Guid, TPayload>(messages.Count);

            for (var i = 0; i < messages.Count; ++i)
            {
                var message = messages[i];
                dictionary[message.GetKey()] = (TPayload)message.GetMessage();
            }

            return _pipelineAction.Invoke(dictionary.Values, token);
        }
    }
}