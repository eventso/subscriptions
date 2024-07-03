using Microsoft.AspNetCore.Mvc;

namespace SampleApplication;

[Route("host")]
[ApiController]
public sealed class HostController : ControllerBase
{
    private static readonly Dictionary<int, IHost> Hosts = new();

    [HttpGet("live")]
    public IReadOnlyCollection<int> GetLiveHosts()
        => Hosts.Keys;
    
    [HttpPost("start")]
    public async Task StartHost(int hostNumber)
    {
        if (hostNumber <= 0)
            throw new Exception("Should be positive");
        
        if (Hosts.ContainsKey(hostNumber))
            return;

        var host = Host.CreateDefaultBuilder()
            .ConfigureWebHostDefaults(webBuilder =>
            {
                webBuilder
                    .UseUrls($"http://*:{5000 + hostNumber}")
                    .UseStartup<Startup>();
            })
            .Build();
        Hosts[hostNumber] = host;

        await host.StartAsync();
    }


    [HttpPost("stop")]
    public async Task StopHost(int hostNumber)
    {
        if (!Hosts.TryGetValue(hostNumber, out var host))
            return;

        await host.StopAsync();
        host.Dispose();

        Hosts.Remove(hostNumber);
    }
}