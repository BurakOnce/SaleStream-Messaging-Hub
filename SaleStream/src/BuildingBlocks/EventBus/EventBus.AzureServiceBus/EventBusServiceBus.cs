using EventBus.Base;
using EventBus.Base.Events;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace EventBus.AzureServiceBus
{
    public class EventBusServiceBus : BaseEventBus
    {
        private ITopicClient topicClient;
        private ManagementClient managementClient;
        private ILogger logger;
        public EventBusServiceBus(EventBusConfig config, IServiceProvider serviceProvider) : base(config, serviceProvider)
        {
            logger= serviceProvider.GetService(typeof(ILogger<EventBusServiceBus>)) as ILogger<EventBusServiceBus>;
            managementClient = new ManagementClient(config.EventBusConnectionString);
            topicClient = createTopicClient();
        }

        private ITopicClient createTopicClient()
        {
            if (topicClient == null || topicClient.IsClosedOrClosing)
            {
                topicClient = new TopicClient(EventBusConfig.EventBusConnectionString,
                                              EventBusConfig.DefaultTopicName,
                                              RetryPolicy.Default
                                              );
            }
            if (!managementClient.TopicExistsAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult())
                managementClient.CreateTopicAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult();
                return topicClient; 
        }

        public override void Publish(IntegrationEvent @event)
        {
            //OrderCreatedIntegrationEvent ornek olarak
            var eventName=@event.GetType().Name;

            //sondaki IntegrationEvent kısmını keser ve sadece OrderCreated kalır 
            eventName = ProcessEventName(eventName);

            /*
             Dışardan bize gelen class'ı önce json nesnesine
             json nesnesini de bodyArr'e çevirmiş oluruz
             */
            var eventStr = JsonConvert.SerializeObject(@event);
            var bodyArr = Encoding.UTF8.GetBytes(eventStr);

            var message = new Message()
            {
                MessageId = Guid.NewGuid().ToString(),
                Body = bodyArr,
                Label=eventName
            };

            //bu tast dönücek
            topicClient.SendAsync(message).GetAwaiter().GetResult();
        }

        public override void Subscribe<T, TH>() //T Event , TH EventHandler
        {
            var eventName= typeof(T).Name;
            eventName= ProcessEventName(eventName);

            if(!SubsManager.HasSubscriptionsForEvent(eventName))
            {
                var subscriptionClient = CreateSubscriptionClientIfNotExist(eventName);

                //burdan gelen mesajları bana ilet kısmı burası (listener)
                RegisterSubscriptionClientMessageHandler(subscriptionClient);
            }
            logger.LogInformation("Subscribing to event {EventName} wiht {EventHandler}", eventName, typeof(TH).Name);
            SubsManager.AddSubscription<T,TH>();
        }

        public override void Unsubscribe<T, TH>()
        {
            var eventName = typeof(T).Name;

            try
            {
                // Subscription will be there but we don't subscribe
                var subscriptionClient = CreateSubscriptionClient(eventName);

                subscriptionClient // böyle bir subscribe var fakat bu event ile artık takip etmeyeceğimizden rule'u silmemiz yeterli
                    .RemoveRuleAsync(eventName) //rule silinir
                    .GetAwaiter()
                    .GetResult();
            }
            catch (MessagingEntityNotFoundException)
            {
                logger.LogWarning("The messaging entity {eventName} could not be found.", eventName);
            }

            logger.LogInformation("Unsubscribing from event {EventName}", eventName);

            SubsManager.RemoveSubscription<T, TH>();

        }

        private void RegisterSubscriptionClientMessageHandler(ISubscriptionClient subscriptionClient)
        {
            subscriptionClient.RegisterMessageHandler(
                async (message, token) =>//burdaki message IIntegrationEvent class'ından türemiştir
                {
                    var eventName = $"{message.Label}";
                    var messageData = Encoding.UTF8.GetString(message.Body);

                    // Ben bu mesajı aldım işim bitti deriz ve tekrar tekrar kullanım engellenir
                    if (await ProcessEvent(ProcessEventName(eventName), messageData))
                    {
                        await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
                    }
                },
                new MessageHandlerOptions(ExceptionReceivedHandler) { MaxConcurrentCalls = 10, AutoComplete = false });
        }

        private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            var ex = exceptionReceivedEventArgs.Exception;
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;

            logger.LogError(ex, "ERROR handling message: {ExceptionMessage} - Context: {@ExceptionContext}", ex.Message, context);

            return Task.CompletedTask;
        }



        private ISubscriptionClient CreateSubscriptionClientIfNotExist(string eventName)
        {
            //bir Subscription yaratacağız
            var subClient = CreateSubscriptionClient(eventName);

            var exists = managementClient.SubscriptionExistsAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();

            //önce var olup olmadığına bakarız
            if (!exists)
            {
                managementClient.CreateSubscriptionAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();
                //ilk defa yaratılıyorsa rule'unu sildik
                RemoveDefaultRule(subClient);
            }
            //Client'ın kendisi olabilir ama altında rule olmayabilir düşüncesiyle rule olup olmadığına bakarız ve oluştururuz
            CreateRuleIfNotExists(ProcessEventName(eventName), subClient);

            return subClient;

        }

        private void CreateRuleIfNotExists(string eventName, ISubscriptionClient subscriptionClient)
        {
            bool ruleExists;

            try
            {
                var rule = managementClient.GetRuleAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName), eventName).GetAwaiter().GetResult();
                ruleExists = rule != null;
            }
            catch (MessagingEntityNotFoundException)
            {
                // Rule'u get edip bakıyoruz hata alırsak rule yok demektir
                ruleExists = false;
            }

            if (!ruleExists)
            {
                //rule yoksa dışardan parametre olarak gelen subscriptionClient ile bir rule yaratırız
                subscriptionClient.AddRuleAsync(new RuleDescription
                {
                    Filter = new CorrelationFilter { Label = eventName },
                    Name = eventName
                }).GetAwaiter().GetResult();
                /*
                 Dışardan gönderilen bir event (OrderCreated mesela)
                 eventName ile Filterin içerisindeki labeldeki eventName uyuşuyor mu diye bakılır
                 */
            }
        }


        private void RemoveDefaultRule(SubscriptionClient subscriptionClient)
        {
            try
            {
                subscriptionClient
                    .RemoveRuleAsync(RuleDescription.DefaultRuleName)
                    .GetAwaiter()
                    .GetResult();
            }
            catch (MessagingEntityNotFoundException)
            {
                logger.LogWarning("The messaging entity {DefaultRuleName} Could not found.", RuleDescription.DefaultRuleName);
            }
        }
          
        private SubscriptionClient CreateSubscriptionClient(string eventName)
        {
            return new SubscriptionClient(EventBusConfig.EventBusConnectionString,
                                          EventBusConfig.DefaultTopicName,
                                          GetSubName(eventName)
                                          );
        }

        public override void Dispose()
        {
            base.Dispose();

            //Önce close sonra null, böylelikle GarbageCollector'ün yükü azalır
            topicClient.CloseAsync().GetAwaiter().GetResult();
            managementClient.CloseAsync ().GetAwaiter().GetResult();
            topicClient = null; 
            managementClient = null;
        }
    }
}
