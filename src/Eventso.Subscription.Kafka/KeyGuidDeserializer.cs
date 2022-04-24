using System;
using Confluent.Kafka;
using HashDepot;

namespace Eventso.Subscription.Kafka
{
    public sealed class KeyGuidDeserializer : IDeserializer<Guid>
    {
        internal static readonly IDeserializer<Guid> Instance = new KeyGuidDeserializer(); 
        
        public Guid Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull || data.Length == 0)
                return Guid.Empty;

            if (data.Length == 16)
                return new Guid(data);

            if (data.Length > 16)
            {
                var hash = MurmurHash3.Hash128(data, 1);
                return new Guid(hash);
            }

            //less that 16 bytes
            Span<byte> key16Bytes = stackalloc byte[16];

            data.CopyTo(key16Bytes);

            return new Guid(key16Bytes);
        }
    }
}