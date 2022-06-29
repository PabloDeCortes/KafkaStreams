using Confluent.Kafka;
using KafkaStreams.Models;
using KafkaStreams.Serializers;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;

// await new Migrator().Execute(CancellationToken.None);

var config = new StreamConfig<StringSerDes, JsonSerDes>
{
    ApplicationId = "test-app",
    BootstrapServers = "localhost:9093",
    AutoOffsetReset = AutoOffsetReset.Earliest,
};

var builder = new StreamBuilder();

var personStream = builder.Stream<string, Person>("persons", new StringSerDes(), new JsonSerDes<Person>());
var locationStream = builder.Stream<string, Location>("locations", new StringSerDes(), new JsonSerDes<Location>());
var jobStream = builder.Stream<string, Job>("jobs", new StringSerDes(), new JsonSerDes<Job>());

personStream
    .SelectKey((_, v) => v.LocationId)
    .Join(locationStream.ToTable(),
        (v1, v2) => new PersonLocation
        {
            Person = v1,
            Location = v2
        })
    .SelectKey((_, v) => v.Person.JobId)
    .Join(jobStream.ToTable(), (v1, v2) => new PersonJobLocation{
        Person = v1.Person,
        Location = v1.Location,
        Job = v2
    })
    .To<StringSerDes, JsonSerDes<PersonJobLocation>>("person-job-location");

var topology = builder.Build();
var stream = new KafkaStream(topology, config);

Console.CancelKeyPress += (_, _) => {
    stream.Dispose();
};

await stream.StartAsync();