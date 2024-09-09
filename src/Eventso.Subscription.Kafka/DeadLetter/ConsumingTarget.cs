namespace Eventso.Subscription.Kafka.DeadLetter;

public readonly record struct ConsumingTarget(string Topic, string GroupId);