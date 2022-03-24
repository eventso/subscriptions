using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.Batch
{
    public sealed class SingleTypeLastByKeyBatchHandler<T> : IBatchHandler<T>
        where T : IMessage
    {
        private readonly IMessageBatchPipelineAction _pipelineAction;

        public SingleTypeLastByKeyBatchHandler(IMessageBatchPipelineAction pipelineAction)
        {
            _pipelineAction = pipelineAction;
        }

        public Task Handle(IConvertibleCollection<T> messages, CancellationToken token)
        {
            if (messages.Count == 0)
                return Task.CompletedTask;

            return HandleTyped((dynamic)messages[0].GetPayload(), messages, token);
        }

        private Task HandleTyped<TPayload>(TPayload sample, IReadOnlyList<T> messages, CancellationToken token)
        {
            var dictionary = new Dictionary<Guid, TPayload>(messages.Count);

            for (var i = 0; i < messages.Count; ++i)
            {
                var message = messages[i];
                dictionary[message.GetKey()] = (TPayload)message.GetPayload();
            }

            return _pipelineAction.Invoke(dictionary.Values, token);
        }
    }
}