using System.Text;
using Confluent.Kafka;
using Newtonsoft.Json;
using Streamiz.Kafka.Net.SerDes;

namespace KafkaStreams.Serializers;

public class JsonSerDes : ISerDes
{
    public object DeserializeObject(byte[] data, SerializationContext context)
    {
        return JsonConvert.DeserializeObject(Encoding.UTF8.GetString(data));
    }

    public byte[] SerializeObject(object data, SerializationContext context)
    {
        return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data));
    }

    public void Initialize(SerDesContext context)
    {
    }
}

public class JsonSerDes<T> : ISerDes<T>
{
    public void Initialize(SerDesContext context)
    {
    }
    
    public object DeserializeObject(byte[] data, SerializationContext context)
    {
        return JsonConvert.DeserializeObject(Encoding.UTF8.GetString(data));
    }

    public byte[] SerializeObject(object data, SerializationContext context)
    {
        return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data));
    }

    public T Deserialize(byte[] data, SerializationContext context)
    {
        return JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(data));
    }

    public byte[] Serialize(string data, SerializationContext context)
    {
        throw new NotImplementedException();
    }

    public byte[] Serialize(T data, SerializationContext context)
    {
        return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data));
    }
}