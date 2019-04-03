namespace Take.Elephant.Tests
{
    public abstract class ItemSenderReceiverQueueFacts : SenderReceiverQueueFacts<Item>
    {
        public abstract override (ISenderQueue<Item>, IReceiverQueue<Item>) Create();
    }
}