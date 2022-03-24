using System;

namespace Eventso.Subscription.Tests
{
    public sealed class RedEvent
    {
        public int Field1 { get; }
        public string Field2 { get; }
        public Guid Field3 { get; }
        public DateTime Field4 { get; }
        public CustomStruct Field5 { get; }

        public RedEvent(int field1, string field2, Guid field3, DateTime field4, CustomStruct field5)
        {
            Field1 = field1;
            Field2 = field2;
            Field3 = field3;
            Field4 = field4;
            Field5 = field5;
        }
    }
}