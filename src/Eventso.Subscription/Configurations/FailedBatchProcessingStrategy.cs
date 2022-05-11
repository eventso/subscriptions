namespace Eventso.Subscription.Configurations
{
    public enum FailedBatchProcessingStrategy
    {
        /// <summary>
        /// Do nothing
        /// </summary>
        None,

        /// <summary>
        /// Process batch messages one by one until the error occurs again
        /// </summary>
        Breakdown
    }
}