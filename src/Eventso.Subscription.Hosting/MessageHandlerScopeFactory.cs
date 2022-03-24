using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;

namespace Eventso.Subscription.Hosting
{
    public sealed class MessageHandlerScopeFactory : IMessageHandlerScopeFactory
    {
        private readonly IServiceScopeFactory _scopeFactory;

        public MessageHandlerScopeFactory(IServiceScopeFactory scopeFactory) =>
            _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));

        public IMessageHandlerScope BeginScope() => new MessageHandlerScope(_scopeFactory.CreateScope());

        private sealed class MessageHandlerScope : IMessageHandlerScope
        {
            private readonly IServiceScope _scope;

            public MessageHandlerScope(IServiceScope scope) => _scope = scope;

            public IEnumerable<IMessageHandler<T>> Resolve<T>() => 
                _scope.ServiceProvider.GetServices<IMessageHandler<T>>();

            public void Dispose() => _scope.Dispose();
        }
    }
}