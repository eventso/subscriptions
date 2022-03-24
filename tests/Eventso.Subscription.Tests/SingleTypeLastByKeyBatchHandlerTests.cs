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
        private readonly SingleTypeLastByKeyBatchHandler<TestMessage> _handler;
        private readonly Fixture _fixture = new();

        public SingleTypeLastByKeyBatchHandlerTests()
        {
            var action = Substitute.For<IMessageBatchPipelineAction>();
            action.Invoke<RedEvent>(default, default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c =>
                {
                    _handledEvents.AddRange(c.Arg<IReadOnlyCollection<RedEvent>>());
                    _handledBatches.Add(c.Arg<IReadOnlyCollection<RedEvent>>());
                });

            _handler = new SingleTypeLastByKeyBatchHandler<TestMessage>(action);
        }

        [Fact]
        public async Task HandlingMessages_LastByKeyHandled()
        {
            var keys = _fixture.CreateMany<Guid>(3).ToArray();

            var events = keys.SelectMany(k => _fixture.CreateMany<RedEvent>(10)
                    .Select(e => new TestMessage(k, e)))
                .OrderBy(_ => Guid.NewGuid())
                .ToConvertibleCollection();

            await _handler.Handle(events, CancellationToken.None);

            _handledEvents.Should().BeEquivalentTo(
                events
                    .GroupBy(x => x.GetKey())
                    .Select(x => x.Last().GetPayload()));

            _handledBatches.Should().HaveCount(1);
        }
    }
}