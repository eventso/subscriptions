using System.Diagnostics;

namespace Eventso.Subscription;

public static class Diagnostic
{
    public static readonly string HostConsuming = "host.consuming";
    public static readonly string PipelineHandle = "pipeline.handle";

    public static readonly ActivitySource ActivitySource = new("eventso");
}