namespace Eventso.Subscription.Kafka.DeadLetter;

public readonly record struct ConsumingTopic(string Topic, string GroupId);