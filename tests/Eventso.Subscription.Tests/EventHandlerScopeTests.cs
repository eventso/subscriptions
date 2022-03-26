using System;
using System.Threading;
using System.Threading.Tasks;
using Eventso.Subscription.Hosting;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Eventso.Subscription.Tests
{
    public sealed class EventHandlerScopeTests
    {
        [Fact]
        public void Test()
        {
            var builder = new ServiceCollection();
            builder.AddScoped<IMessageHandler<RedMessage>, RedMessageHandler>();
            builder.AddScoped<IMessageHandler<RedMessage>, AnotherRedMessageHandler>();

            var provider = builder.BuildServiceProvider();

            var scope = new MessageHandlerScopeFactory(provider.GetRequiredService<IServiceScopeFactory>())
                .BeginScope();

            var service = scope.Resolve<RedMessage>();

            service.Should().NotBeNull().And.HaveCount(2);
        }

        public class RedMessageHandler : IMessageHandler<RedMessage>
        {
            public Task Handle(RedMessage message, CancellationToken token)
            {
                throw new NotImplementedException();
            }
        }

        public class AnotherRedMessageHandler : IMessageHandler<RedMessage>
        {
            public Task Handle(RedMessage message, CancellationToken token)
            {
                throw new NotImplementedException();
            }
        }

       
    }
}