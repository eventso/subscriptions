using System.Diagnostics;

namespace Eventso.Subscription;

public static class Diagnostic
{
    public static readonly string HostConsuming = "host.consuming";
    public static readonly string PipelineHandle = "pipeline.handle";
    public static readonly string EventHandlerHandle = "eventhandler.handle";
    public static readonly string SourceName = "eventso";

    public static readonly ActivitySource ActivitySource = new(SourceName);
}