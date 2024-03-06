namespace Take.Elephant.Redis.Serializers
{
    public sealed class LowerValueSerializer<T> : ValueSerializer<T>
    {
        public override string Serialize(T value)
        {
            return base.Serialize(value).ToLowerInvariant();
        }
    }
}
