namespace KafkaStreams.Models;

public class Job
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string Company { get; set; }
}