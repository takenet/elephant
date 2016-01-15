using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace Takenet.Elephant.RabbitMQ
{
    public class StorageBase<TKey> : IDisposable
    {
        protected readonly string _queueName;
        protected readonly IConnection _connection;
        protected readonly IModel _model;

        public StorageBase(string queueName, IConnectionFactory connectionFactory, bool isExclusive = false)
        {
            if (queueName == null) throw new ArgumentNullException(nameof(queueName));
            if (connectionFactory == null) throw new ArgumentNullException(nameof(connectionFactory));
            _queueName = queueName;
            _connection = connectionFactory.CreateConnection();

            _model = _connection.CreateModel();
            _model.ConfirmSelect();
            _model.QueueDeclare(_queueName, true, isExclusive, false, null);
        }

        ~StorageBase()
        {
            Dispose();
        }
        
        public string GetQueueName()
        {
            return _queueName;
        }

        public IModel GetModel()
        {
            return _model;
        }

        public uint GetConsumerCount()
        {
            return _model.ConsumerCount(_queueName);
        }

        public uint GetMessageCount()
        {
            return _model.MessageCount(_queueName);
        }

        #region IDisposable Members

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public virtual void Dispose(bool disposing)
        {
            if(disposing)
            {
                _connection.Dispose();
            }
        }

        #endregion
    }
}
