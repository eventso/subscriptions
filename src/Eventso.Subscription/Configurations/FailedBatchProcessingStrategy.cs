namespace Eventso.Subscription.Configurations
{
    public enum FailedBatchProcessingStrategy
    {
        /// <summary>
        /// Do nothing
        /// </summary>
        None,

        /// <summary>
        /// Set batch size to one until the error occurs again
        /// </summary>
        Breakdown
    }
}