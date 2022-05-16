using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter
{
    public interface IPoisonRecordInbox
    {
        Task Add(ConsumeResult<byte[], byte[]> consumeResult, string failureReason, CancellationToken token);
    }
}