using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using Eventso.Subscription.Observing.Batch;
using FluentAssertions;
using NSubstitute;
using Xunit;

namespace Eventso.Subscription.Tests
{
    public sealed class SingleTypeLastByKeyBatchHandlerTests
    {
        private readonly List<object> _handledEvents = new();
        private readonly List<IReadOnlyCollection<object>> _handledBatches = new();
        private readonly SingleTypeLastByKeyBatchHandler<TestEvent> _handler;
        private readonly Fixture _fixture = new();

        public SingleTypeLastByKeyBatchHandlerTests()
        {
            var action = Substitute.For<IMessageBatchPipelineAction>();
            action.Invoke<RedMessage>(default, default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c =>
                {
                    _handledEvents.AddRange(c.Arg<IReadOnlyCollection<RedMessage>>());
                    _handledBatches.Add(c.Arg<IReadOnlyCollection<RedMessage>>());
                });

            _handler = new SingleTypeLastByKeyBatchHandler<TestEvent>(action);
        }

        [Fact]
        public async Task HandlingMessages_LastByKeyHandled()
        {
            var keys = _fixture.CreateMany<Guid>(3).ToArray();

            var events = keys.SelectMany(k => _fixture.CreateMany<RedMessage>(10)
                    .Select(e => new TestEvent(k, e)))
                .OrderBy(_ => Guid.NewGuid())
                .ToConvertibleCollection();

            await _handler.Handle(events, CancellationToken.None);

            _handledEvents.Should().BeEquivalentTo(
                events
                    .GroupBy(x => x.GetKey())
                    .Select(x => x.Last().GetMessage()));

            _handledBatches.Should().HaveCount(1);
        }
    }
}