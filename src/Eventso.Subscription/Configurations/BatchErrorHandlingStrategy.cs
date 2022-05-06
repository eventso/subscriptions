namespace Eventso.Subscription.Configurations
{
    public enum BatchErrorHandlingStrategy
    {
        /// <summary>
        /// No handling
        /// </summary>
        None,

        /// <summary>
        /// In case of error batch size will be set to one until the error occurs again
        /// </summary>
        Breakdown
    }
}