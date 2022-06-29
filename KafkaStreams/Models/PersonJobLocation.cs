namespace KafkaStreams.Models
{
    public class PersonJobLocation
    {
        public Person Person { get; set; }
        public Job Job { get; set; }
        public Location Location { get; set; }
    }
}