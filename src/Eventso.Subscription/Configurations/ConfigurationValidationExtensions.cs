using System;

namespace Eventso.Subscription.Configurations
{
    public static class ConfigurationValidationExtensions
    {
        public static void Validate(this BatchConfiguration configuration)
        {
            if (configuration.MaxBatchSize <= 1)
                throw new ApplicationException(
                    $"Max batch size ({configuration.MaxBufferSize} should not be less or equal than 1.");

            if (configuration.MaxBufferSize != default && configuration.MaxBufferSize < configuration.MaxBatchSize)
                throw new ApplicationException(
                    $"Max buffer size ({configuration.MaxBufferSize} should not be less than max batch size ({configuration.MaxBatchSize}).");
        }

        public static void Validate(this DeferredAckConfiguration configuration)
        {
            if (configuration.MaxBufferSize < 0)
                throw new ApplicationException(
                    $"Max batch size ({configuration.MaxBufferSize} should not be less than 0.");
        }
    }
}