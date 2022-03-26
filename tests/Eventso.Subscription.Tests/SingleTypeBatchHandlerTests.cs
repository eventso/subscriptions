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
    public sealed class SingleTypeBatchHandlerTests
    {
        private readonly List<object> _handledEvents = new();
        private readonly List<IReadOnlyCollection<object>> _handledBatches = new();
        private readonly SingleTypeBatchHandler<TestEvent> _handler;
        private readonly Fixture _fixture = new();

        public SingleTypeBatchHandlerTests()
        {
            var action = Substitute.For<IMessageBatchPipelineAction>();
            action.Invoke<RedMessage>(default, default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c =>
                {
                    _handledEvents.AddRange(c.Arg<IReadOnlyCollection<RedMessage>>());
                    _handledBatches.Add(c.Arg<IReadOnlyCollection<RedMessage>>());
                });

            _handler = new SingleTypeBatchHandler<TestEvent>(action);
        }

        [Fact]
        public async Task SingleTypeMessages_SameOrder()
        {
            var events = _fixture.CreateMany<(RedMessage e, Guid k)>(10)
                .Select(x => new TestEvent(x.k, x.e))
                .ToConvertibleCollection();

            await _handler.Handle(events, CancellationToken.None);

            _handledEvents.Should().BeEquivalentTo(
                events.Select(x => x.GetMessage()),
                c => c.WithStrictOrdering());

            _handledBatches.Should().HaveCount(1);
            _handledBatches.SelectMany(x => x).Should().HaveSameCount(events);
        }

        [Fact]
        public async Task EmptyMessages_NoBatches()
        {
            await _handler.Handle(Array.Empty<TestEvent>().ToConvertibleCollection(),
                CancellationToken.None);

            _handledEvents.Should().BeEmpty();

            _handledBatches.Should().HaveCount(0);
        }

        [Fact]
        public async Task MultiTypeMessages_Throws()
        {
            var events = _fixture.CreateMany<(RedMessage e, Guid k)>(10)
                .Select(x => new TestEvent(x.k, x.e))
                .Concat(_fixture.CreateMany<(BlueMessage e, Guid k)>(10)
                    .Select(x => new TestEvent(x.k, x.e)))
                .ToConvertibleCollection();

            Func<Task> act = () => _handler.Handle(events, CancellationToken.None);

            await act.Should().ThrowAsync<Exception>();
        }
    }
}