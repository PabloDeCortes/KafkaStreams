using System.Text;
using Confluent.Kafka;
using Newtonsoft.Json;
using Streamiz.Kafka.Net.SerDes;

namespace KafkaStreams.Serializers
{

    public class JsonSerDes<T> : AbstractSerDes<T>
    {
        public override T Deserialize(byte[] data, SerializationContext context)
        {
            return JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(data));
        }

        public override byte[] Serialize(T data, SerializationContext context)
        {
            return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data));
        }
    }
}