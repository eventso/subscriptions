using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;

namespace Eventso.Subscription.Hosting
{
    internal sealed class MessageHandlersRegistry : IMessageHandlersRegistry
    {
        private readonly Dictionary<Type, HandlerKind> _types;

        public static MessageHandlersRegistry Create(IEnumerable<ServiceDescriptor> registrations)
        {
            var handlers = registrations
                .Select(d => d.ServiceType)
                .Where(t => t.IsGenericType &&
                            t.GetGenericTypeDefinition() == typeof(IMessageHandler<>));

            var messageTypes = new Dictionary<Type, HandlerKind>();

            foreach (var handler in handlers)
            {
                if (handler.IsGenericTypeDefinition)
                    //open generic can handle any message type
                    return new MessageHandlersRegistry();

                var parameter = handler.GenericTypeArguments[0];

                if (parameter.IsGenericType)
                {
                    var definition = parameter.GetGenericTypeDefinition();

                    if (definition == typeof(IReadOnlyCollection<>))
                    {
                        if (parameter.IsGenericTypeDefinition)
                            //open generic can handle any message type
                            return new MessageHandlersRegistry();

                        var type = parameter.GenericTypeArguments[0];

                        messageTypes[type] =
                            messageTypes.TryGetValue(type, out var existing)
                                ? existing | HandlerKind.Batch
                                : HandlerKind.Batch;

                        continue;
                    }
                }

                messageTypes[parameter] =
                    messageTypes.TryGetValue(parameter, out var other)
                        ? other | HandlerKind.Single
                        : HandlerKind.Single;
            }

            return new MessageHandlersRegistry(messageTypes);
        }

        public MessageHandlersRegistry(Dictionary<Type, HandlerKind> messageTypes)
            => _types = messageTypes;

        public MessageHandlersRegistry()
        {
            //handle any
        }

        public bool ContainsHandlersFor(Type messageType, out HandlerKind kind)
        {
            kind = HandlerKind.Single | HandlerKind.Batch;

            return _types?.TryGetValue(messageType, out kind) ?? true;
        }
    }
}