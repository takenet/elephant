using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Takenet.Elephant.RabbitMQ
{
    public class TaskBasicConsumer : IBasicConsumer
    {
        protected readonly IModel _model;
        protected readonly TaskCompletionSource<string> _tcs;
        protected readonly CancellationToken _cancellationToken;

        public event EventHandler<ConsumerEventArgs> ConsumerCancelled;

        public TaskBasicConsumer(IModel model, CancellationToken cancellationToken)
        {
            _model = model;
            _tcs = new TaskCompletionSource<string>();
            _cancellationToken = cancellationToken;
        }

        IModel IBasicConsumer.Model
        {
            get
            {
                return _model;
            }
        }

        public Task<string> GetTask()
        {
            return _tcs.Task;
        }

        public void HandleBasicCancel(string consumerTag)
        {
        }

        public void HandleBasicCancelOk(string consumerTag)
        {
        }

        public void HandleBasicConsumeOk(string consumerTag)
        {
            if (_cancellationToken != null)
            {
                _cancellationToken.Register(() =>
                {
                    _tcs.TrySetCanceled();
                    lock (_model)
                    {
                        _model.BasicCancel(consumerTag);
                    }
                });
            }
        }

        public void HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange, string routingKey, IBasicProperties properties, byte[] body)
        {
            _tcs.SetResult(Encoding.UTF8.GetString(body));
            lock (_model)
            {
                _model.BasicAck(deliveryTag, false);
                _model.BasicCancel(consumerTag);
            }
        }

        public void HandleModelShutdown(object model, ShutdownEventArgs reason)
        {
            _tcs.TrySetCanceled();
        }
    }
}
