namespace KafkaStreams.Models;

public class Location
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string City { get; set; }
}