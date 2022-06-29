using System;
using System.Threading;
using Confluent.Kafka;
using KafkaStreams.Models;
using KafkaStreams.Serializers;
using KafkaStreams.Services;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

await new Migrator().Execute(CancellationToken.None, "persons", "locations", "jobs");

var config = new StreamConfig<StringSerDes, StringSerDes>
{
    ApplicationId = "test-app",
    BootstrapServers = "localhost:9093",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

var builder = new StreamBuilder();

var stringSerdes = new StringSerDes();
var personSerdes = new JsonSerDes<Person>();
var locationSerdes = new JsonSerDes<Location>();
var jobSerdes = new JsonSerDes<Job>();

var personStream = builder.Stream<string, Person>("persons", stringSerdes, personSerdes);
var locationStream = builder.Stream<string, Location>("locations", stringSerdes, locationSerdes);
var jobStream = builder.Stream<string, Job>("jobs", stringSerdes, jobSerdes);

personStream
    .SelectKey((_, v) => v.LocationId)
    .Join(locationStream.ToTable(RocksDb<string, Location>.Create().WithKeySerdes(stringSerdes).WithValueSerdes(locationSerdes)), // you have to specify the good serdes for materialize your state store inside the changelog topic
        (v1, v2) => new PersonLocation {
            Person = v1,
            Location = v2
        })
    .SelectKey((_, v) => v.Person.JobId) // here you change the key and just after you make a statefull operation (eg/ join table job), a repartition topic will be create. Key : string, Value : PersonLocation. Issue : you can't specify the value serdes for PersonLocation in join operation just before. Workaround : create a repartition topic manually
    .Repartition(Repartitioned<string,PersonLocation>.Create<StringSerDes, JsonSerDes<PersonLocation>>()) // workaround because for now, you can't specify the serdes for VR in join operation
    .Join(jobStream.ToTable(RocksDb<string, Job>.Create().WithKeySerdes(stringSerdes).WithValueSerdes(jobSerdes)), // you have to specify the good serdes for materialize your state store inside the changelog topic
        (v1, v2) => new PersonJobLocation {
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