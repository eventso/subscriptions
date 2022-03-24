using System.Threading.Tasks;

namespace Eventso.Subscription.Inbox
{
    public interface IMessagePublisher
    {
        Task Publish(byte[] message);
    }
}