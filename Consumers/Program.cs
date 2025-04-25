using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;

namespace Consumers
{
    class Program
    {
        private const string connectionString = "Endpoint=sb://psynceventhubdemo.servicebus.windows.net/;SharedAccessKeyName=psync-receiver;SharedAccessKey=VUoLno9e1FkWDh0WNbUFnHUmjI23eG+kO+AEhMJro0M=;EntityPath=psync-demo";
        private const string eventHubName = "psync-demo";
        private const string blobStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=psync;AccountKey=tgb2m3kyf5ICEqfv+nRoc9ubJYcP0OM9JQSLRUbE1PHg6mzb6x1rbmMbt2J1LX6OQi7FWWwAsUkF+AStyns7vg==;EndpointSuffix=core.windows.net";
        private const string blobContainerName = "psync-eventhub-checkpoints";
        static async Task Main()
        {
            //Default consumer group name
            // The default consumer group is "$Default"
            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            // Create a blob container client that the event processor will use 
            BlobContainerClient storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);

            // Create an event processor client to process events in the event hub
            EventProcessorClient processor = new EventProcessorClient(storageClient, consumerGroup, connectionString, eventHubName);

            // Register handlers for processing events and handling errors
            processor.ProcessEventAsync += ProcessEventHandler;
            processor.ProcessErrorAsync += ProcessErrorHandler;

            // Start the processing
            await processor.StartProcessingAsync();

            // Wait for 10 seconds for the events to be processed
            await Task.Delay(TimeSpan.FromSeconds(10));

            // Stop the processing
            await processor.StopProcessingAsync();
        }
        static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            // Write the body of the event to the console window
            Console.WriteLine("\tRecevied event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));

            // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            // Write details about the error to the console window
            Console.WriteLine($"\tPartition '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }
    }
}