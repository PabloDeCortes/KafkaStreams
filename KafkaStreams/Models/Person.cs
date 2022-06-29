using System;

namespace KafkaStreams.Models
{
    public class Person
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
        public string Name { get; set; }
        public string Surname { get; set; }
        public string LocationId { get; set; }
        public string JobId { get; set; }
    }
}