using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace ServiceBusProducer
{
    public static class ServiceBusProducer
    {
        private static readonly ServiceBusClient _serviceBusClient = new ServiceBusClient(Environment.GetEnvironmentVariable("ServiceBusHostName"), new DefaultAzureCredential());
        private static readonly ServiceBusSender _topicASender = _serviceBusClient.CreateSender(Environment.GetEnvironmentVariable("TopicName"));


        [FunctionName("ServiceBusProducer")]
        public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = null)] HttpRequest req, ILogger log)
        {
            bool.TryParse(req.Query["batchMode"], out bool batchMode);
            if (int.TryParse(req.Query["numMsgs"], out int numMessages))
            {
                try
                {
                    if (!batchMode)
                    {
                        log.LogInformation("Singleton Mode");
                        for (int i = 0; i < numMessages; i++)
                        {
                            var message = new ServiceBusMessage($"Message {i}");

                            await _topicASender.SendMessageAsync(message);
                        }
                        log.LogInformation($"Sent {numMessages} messages");
                    }
                    else
                    {
                        log.LogInformation("Batch Mode");
                        var topicAMessageBatch = await _topicASender.CreateMessageBatchAsync();

                        for (int i = 0; i < numMessages; i++)
                        {
                            var message = new ServiceBusMessage($"Message {i}");
                            
                            if (!topicAMessageBatch.TryAddMessage(message))
                            {
                                await _topicASender.SendMessagesAsync(topicAMessageBatch);
                                log.LogInformation($"Topic A Batch full - sent {topicAMessageBatch.Count} messages");
                                topicAMessageBatch = await _topicASender.CreateMessageBatchAsync();

                                if (!topicAMessageBatch.TryAddMessage(message))
                                {
                                    throw new Exception("Can't create a batch at all");
                                }
                            }                            
                        }

                        await _topicASender.SendMessagesAsync(topicAMessageBatch);
                        log.LogInformation($"Final batch - sent {topicAMessageBatch.Count} messages");
                    }
                }
                catch (Exception ex)
                {
                    log.LogCritical(ex, "Couldn't produce any messages");
                    return new OkObjectResult(ex.Message);
                }
            }

            return new OkObjectResult($"Produced {numMessages} messages");
        }
    }
}
