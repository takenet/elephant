namespace Take.Elephant
{
    /// <summary>
    /// Describes a priority item with the corresponding value
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class PriorityItem<T>
    {
        public T Item { get; set; }

        public double Score { get; set; }
    }
}