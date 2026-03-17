using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices(services =>
    {
        var credential = new DefaultAzureCredential();

        services.AddAzureClients(builder =>
        {
            builder.AddBlobServiceClient(new Uri("https://consumeddata.blob.core.windows.net"))
                   .WithCredential(credential);

            builder.AddQueueServiceClient(new Uri("https://consumeddata.queue.core.windows.net"))
                   .WithCredential(credential)
                   .ConfigureOptions(options => options.MessageEncoding = QueueMessageEncoding.Base64);
        });
    })
    .Build();

host.Run();
