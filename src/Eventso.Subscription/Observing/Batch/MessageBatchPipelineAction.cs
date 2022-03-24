using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.Batch
{
    public sealed class MessageBatchPipelineAction : IMessageBatchPipelineAction
    {
        private readonly IMessageHandlersRegistry _handlersRegistry;
        private readonly IMessagePipelineAction _action;

        public MessageBatchPipelineAction(
            IMessageHandlersRegistry handlersRegistry,
            IMessagePipelineAction action)
        {
            _handlersRegistry = handlersRegistry;
            _action = action;
        }

        public async Task Invoke<T>(IReadOnlyCollection<T> messages, CancellationToken token)
        {
            if (!_handlersRegistry.ContainsHandlersFor(typeof(T), out var kind))
                return;

            if ((kind & HandlerKind.Batch) != 0)
                await _action.Invoke(messages, token);

            if((kind & HandlerKind.Single) != 0)
                foreach (var message in messages)
                    await _action.Invoke(message, token);
        }
    }
}