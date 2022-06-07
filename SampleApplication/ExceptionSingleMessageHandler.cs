using System;
using System.Threading;
using System.Threading.Tasks;
using Eventso.Subscription;

namespace SampleApplication
{
    public class ExceptionSingleMessageHandler : IMessageHandler<ExceptionSingleMessageHandler.ExceptionSingleMessage>
    {
        public const string Topic = "exception-single";
        
        public Task Handle(ExceptionSingleMessage message, CancellationToken token)
        {
            Console.WriteLine($"[{Topic}] Message received: [{message}]. Throwing exception: [{message.Bool}].");

            if (message.Bool)
                throw new Exception($"[{{Topic}}] Bool is TRUE in message [{message}].");

            return Task.CompletedTask;
        }

        public record ExceptionSingleMessage(Guid Guid, bool Bool, string String) : ISampleMessage
        {
            public override string ToString()
                => $"Guid: {Guid}, Bool: {Bool}, String: {String}";
        };
    }
}