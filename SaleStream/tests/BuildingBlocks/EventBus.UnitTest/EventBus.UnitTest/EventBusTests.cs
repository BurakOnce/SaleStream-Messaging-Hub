using EventBus.Base;
using EventBus.Base.Abstraction;
using EventBus.Factory;
using EventBus.UnitTest.Events.EventHandlers;
using EventBus.UnitTest.Events.Events;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RabbitMQ.Client;

namespace EventBus.UnitTest
{
    [TestClass]
    public class EventBusTests
    {
        ServiceCollection services;

        public EventBusTests()
        {
            services =new ServiceCollection();
            services.AddLogging(configure => configure.AddConsole());

        }

        [TestMethod]
        public void subscribe_event_on_rabbitmq_test()
        {
            //Uygulama ayaða kalkýnca IEventBus bir kere create edilecek o sebeple Singleton
            //Ne zaman EventBus eklenirse aþaðýdaki kod bloðu çalýþýr
            services.AddSingleton<IEventBus>(sp =>
            {

                return EventBusFactory.Create(GetRabbitMQConfig(), sp);

            });
            
            var sp = services.BuildServiceProvider(); //serviceProvider

            var eventBus = sp.GetRequiredService<IEventBus>();

            eventBus.Subscribe<OrderCreatedIntegrationEvent, OrderCreatedIntegrationEventHandler>(); //T,TH
            eventBus.Unsubscribe<OrderCreatedIntegrationEvent, OrderCreatedIntegrationEventHandler>(); //T,TH


        }

        [TestMethod]
        public void subscribe_event_on_azure_test()
        {
            //Uygulama ayaða kalkýnca IEventBus bir kere create edilecek o sebeple Singleton
            //Ne zaman EventBus eklenirse aþaðýdaki kod bloðu çalýþýr
            services.AddSingleton<IEventBus>(sp =>
            {
                    
                return EventBusFactory.Create(GetAzureConfig(), sp);

            });

            var sp = services.BuildServiceProvider(); //serviceProvider

            var eventBus = sp.GetRequiredService<IEventBus>();

            eventBus.Subscribe<OrderCreatedIntegrationEvent, OrderCreatedIntegrationEventHandler>(); //T,TH
            eventBus.Unsubscribe<OrderCreatedIntegrationEvent, OrderCreatedIntegrationEventHandler>(); //T,TH


        }

        [TestMethod]
        public void send_message_to_rabbitmq_test()
        {
            services.AddSingleton<IEventBus>(sp =>
            {

                return EventBusFactory.Create(GetRabbitMQConfig(), sp);

            });

            var sp = services.BuildServiceProvider(); 

            var eventBus = sp.GetRequiredService<IEventBus>();

            eventBus.Publish(new OrderCreatedIntegrationEvent(1));
        }

        [TestMethod]
        public void send_message_to_azure_test()
        {
            services.AddSingleton<IEventBus>(sp =>
            {

                return EventBusFactory.Create(GetAzureConfig(), sp);

            });

            var sp = services.BuildServiceProvider();

            var eventBus = sp.GetRequiredService<IEventBus>();

            eventBus.Publish(new OrderCreatedIntegrationEvent(1));
        }

        [TestMethod]
        public void consume_ordercreated_from_rabbitmq_test()
        {
            services.AddSingleton<IEventBus>(sp =>
            {

                return EventBusFactory.Create(GetRabbitMQConfig(), sp);

            });

            var sp = services.BuildServiceProvider(); //serviceProvider

            var eventBus = sp.GetRequiredService<IEventBus>();

            eventBus.Subscribe<OrderCreatedIntegrationEvent, OrderCreatedIntegrationEventHandler>(); 
        }

        private EventBusConfig GetAzureConfig()
        {
            return new EventBusConfig()
            {
                ConnectionRetryCount = 5,
                SubscriberClientAppName = "EventBus.UnitTest",
                DefaultTopicName = "SaleStreamTopicName",
                EventBusType = EventBusType.AzureServiceBus,
                EventNameSuffix = "IntegrationEvent",
                EventBusConnectionString = " "
            };
        }

        private EventBusConfig GetRabbitMQConfig()
        {
            return new EventBusConfig()
            {
                ConnectionRetryCount = 5,
                SubscriberClientAppName = "EventBus.UnitTest",
                DefaultTopicName = "SaleStreamTopicName",
                EventBusType = EventBusType.RabbitMQ,
                EventNameSuffix = "IntegrationEvent",
                /*
                Connection = new ConnectionFactory()
                {
                    HostName = "localhost",
                    Port=5672,
                    UserName = "guest",
                    Password = "guest",

                }*/
            };
        }
    }
}
