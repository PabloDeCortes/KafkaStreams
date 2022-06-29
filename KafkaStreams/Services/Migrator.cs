using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaStreams.Models;
using KafkaStreams.Serializers;

namespace KafkaStreams.Services
{

    public class Migrator
    {
        public async Task Execute(CancellationToken cancellationToken, params string[] topicsToCreate)
        {
            var adminConfig = new AdminClientConfig()
            {
                BootstrapServers = "localhost:9093"
            };

            using var adminClient = new AdminClientBuilder(adminConfig).Build();

            try
            {
                // create topic, catch exception and do nothing if topics are already exist. just for dev environment
                await adminClient.CreateTopicsAsync(
                    topicsToCreate.Select(t => new Confluent.Kafka.Admin.TopicSpecification
                    {
                        Name = t,
                        NumPartitions = 3
                    }));
            }
            catch
            {
                // nothing if topic already exist
            }

            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9093"
            };

            using var personProducer = new ProducerBuilder<string, Person>(config)
                .SetValueSerializer(new JsonSerializer<Person>())
                .Build();
            using var locationProducer = new ProducerBuilder<string, Location>(config)
                .SetValueSerializer(new JsonSerializer<Location>())
                .Build();
            using var jobProducer = new ProducerBuilder<string, Job>(config)
                .SetValueSerializer(new JsonSerializer<Job>())
                .Build();

            var locations = new List<Location>
            {
                new() {Id = Guid.NewGuid().ToString(), City = "New York"},
                new() {Id = Guid.NewGuid().ToString(), City = "London"},
                new() {Id = Guid.NewGuid().ToString(), City = "Paris"},
                new() {Id = Guid.NewGuid().ToString(), City = "Berlin"},
            };
            var jobs = new List<Job>
            {
                new() {Id = Guid.NewGuid().ToString(), Company = "Google"},
                new() {Id = Guid.NewGuid().ToString(), Company = "Microsoft"},
                new() {Id = Guid.NewGuid().ToString(), Company = "Apple"},
                new() {Id = Guid.NewGuid().ToString(), Company = "Facebook"},
                new() {Id = Guid.NewGuid().ToString(), Company = "Amazon"},
            };

            var people = new List<Person>
            {
                new() {Id = Guid.NewGuid().ToString(), Name = "John", LocationId = locations[0].Id, JobId = jobs[0].Id},
                new() {Id = Guid.NewGuid().ToString(), Name = "Jane", LocationId = locations[1].Id, JobId = jobs[1].Id},
                new() {Id = Guid.NewGuid().ToString(), Name = "Jack", LocationId = locations[2].Id, JobId = jobs[2].Id},
                new() {Id = Guid.NewGuid().ToString(), Name = "Jill", LocationId = locations[3].Id, JobId = jobs[3].Id},
            };

            foreach (var location in locations)
            {
                await locationProducer.ProduceAsync(
                    "locations",
                    new Message<string, Location>
                    {
                        Key = location.Id,
                        Value = location
                    },
                    cancellationToken);
            }

            // flush in the location producer. Wait all messages are persisted into the broker
            locationProducer.Flush(cancellationToken);

            foreach (var job in jobs)
            {
                await jobProducer.ProduceAsync(
                    "jobs",
                    new Message<string, Job>
                    {
                        Key = job.Id,
                        Value = job
                    },
                    cancellationToken);
            }

            // flush in the job producer. Wait all messages are persisted into the broker
            jobProducer.Flush(cancellationToken);

            foreach (var person in people)
            {
                await personProducer.ProduceAsync(
                    "persons",
                    new Message<string, Person>
                    {
                        Key = person.Id,
                        Value = person
                    },
                    cancellationToken);
            }

            // flush in the person producer. Wait all messages are persisted into the broker
            personProducer.Flush(cancellationToken);
        }
    }
}