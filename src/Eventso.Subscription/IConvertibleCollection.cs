using System;
using System.Collections.Generic;

namespace Eventso.Subscription
{
    public interface IConvertibleCollection<out T> : IReadOnlyList<T>
    {
        IReadOnlyCollection<TOut> Convert<TOut>(Converter<T, TOut> converter);
        bool OnlyContainsSame<TValue>(Func<T, TValue> valueConverter);
    }
}