using System;

namespace Eventso.Subscription.Tests
{
    public readonly struct CustomStruct : IEquatable<CustomStruct>
    {
        public int X { get; }

        public int Y { get; }

        public CustomStruct(int x, int y)
        {
            X = x;
            Y = y;
        }

        public bool Equals(CustomStruct other) =>
            X == other.X && Y == other.Y;

        public override bool Equals(object obj) =>
            obj is CustomStruct other && Equals(other);

        public override int GetHashCode() =>
            HashCode.Combine(X, Y);
    }
}