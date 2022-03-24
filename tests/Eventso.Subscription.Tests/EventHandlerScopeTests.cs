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
            builder.AddScoped<IMessageHandler<RedEvent>, RedMessageHandler>();
            builder.AddScoped<IMessageHandler<RedEvent>, AnotherRedMessageHandler>();

            var provider = builder.BuildServiceProvider();

            var scope = new MessageHandlerScopeFactory(provider.GetRequiredService<IServiceScopeFactory>())
                .BeginScope();

            var service = scope.Resolve<RedEvent>();

            service.Should().NotBeNull().And.HaveCount(2);
        }

        public class RedMessageHandler : IMessageHandler<RedEvent>
        {
            public Task Handle(RedEvent message, CancellationToken token)
            {
                throw new NotImplementedException();
            }
        }

        public class AnotherRedMessageHandler : IMessageHandler<RedEvent>
        {
            public Task Handle(RedEvent message, CancellationToken token)
            {
                throw new NotImplementedException();
            }
        }

       
    }
}