using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Messaging;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace Takenet.Elephant.Msmq
{
    /// <summary>
    /// <see cref="IBlockingQueue{T}"/> implementation by using the Microsoft Message Queue API.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <seealso cref="Takenet.Elephant.IBlockingQueue{T}" />
    public class MsmqQueue<T> : IBlockingQueue<T>, IDisposable
    {
        private readonly ISerializer<T> _serializer;
        private readonly bool _recoverable;
        private readonly IMessageFormatter _messageFormatter;
        private readonly MessageQueue _messageQueue;

        public MsmqQueue(string path, ISerializer<T> serializer = null, IMessageFormatter messageFormatter = null, bool recoverable = true)
        {
            _serializer = serializer;
            _recoverable = recoverable;
            _messageFormatter = messageFormatter ?? new BinaryMessageFormatter();
            if (!MessageQueue.Exists(path))
            {
                MessageQueue.Create(path);
            }
            _messageQueue = new MessageQueue(path, QueueAccessMode.SendAndReceive);
        }

        ~MsmqQueue()
        {
            Dispose(false);
        }

        public Task EnqueueAsync(T item)
        {
            // Warning: This method is do not support async I/O, but runs asynchronously.
            using (var message = new Message { Recoverable = _recoverable, Formatter = _messageFormatter })
            {

                if (_serializer != null)
                {
                    message.Body = _serializer.Serialize(item);
                }
                else
                {
                    message.Body = item;
                }

                _messageQueue.Send(message);
                return TaskUtil.CompletedTask;
            }
        }

        public Task<T> DequeueOrDefaultAsync()
        {
            try
            {
                using (var message = _messageQueue.Receive(TimeSpan.FromTicks(1)))
                {
                    if (message != null)
                    {
                        var value = GetValue(message);
                        return Task.FromResult(value);
                    }
                }
            }
            catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout) {  }
            return Task.FromResult(default(T));
        }

        public async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {                        
            var cancellableTcs = new TaskCompletionSource<object>();
            cancellationToken.Register(() => cancellableTcs.TrySetCanceled());

            cancellationToken.ThrowIfCancellationRequested();

            var receiveTask = Task.Factory.FromAsync(
                _messageQueue.BeginReceive(), r => _messageQueue.EndReceive(r));

            await Task.WhenAny(receiveTask, cancellableTcs.Task).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();

            using (var message = await receiveTask.ConfigureAwait(false))
            {
                return GetValue(message);
            }
        }

        public Task<long> GetLengthAsync()
        {
            // Source: https://github.com/hibernating-rhinos/rhino-esb/blob/master/Rhino.ServiceBus/Msmq/MsmqExtensions.cs
            var props = new NativeMethods.MQMGMTPROPS { cProp = 1 };
            try
            {
                props.aPropID = Marshal.AllocHGlobal(sizeof(int));
                Marshal.WriteInt32(props.aPropID, NativeMethods.PROPID_MGMT_QUEUE_MESSAGE_COUNT);

                props.aPropVar = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(NativeMethods.MQPROPVariant)));
                Marshal.StructureToPtr(new NativeMethods.MQPROPVariant { vt = NativeMethods.VT_NULL }, props.aPropVar, false);

                props.status = Marshal.AllocHGlobal(sizeof(int));
                Marshal.WriteInt32(props.status, 0);

                int result = NativeMethods.MQMgmtGetInfo(null, "queue=" + _messageQueue.FormatName, ref props);
                if (result != 0)
                    throw new Win32Exception(result);

                if (Marshal.ReadInt32(props.status) != 0)
                {
                    return Task.FromResult<long>(0);
                }

                var propVar = (NativeMethods.MQPROPVariant)Marshal.PtrToStructure(props.aPropVar, typeof(NativeMethods.MQPROPVariant));
                if (propVar.vt != NativeMethods.VT_UI4)
                {
                    return Task.FromResult<long>(0);
                }
                else
                {
                    return Task.FromResult(Convert.ToInt64(propVar.ulVal));
                }
            }
            finally
            {
                Marshal.FreeHGlobal(props.aPropID);
                Marshal.FreeHGlobal(props.aPropVar);
                Marshal.FreeHGlobal(props.status);
            }
        }

        private T GetValue(Message message)
        {
            message.Formatter = _messageFormatter;
            if (_serializer != null)
            {
                return _serializer.Deserialize((string) message.Body);
            }

            return (T)message.Body;            
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _messageQueue?.Dispose();
            }
        }
    }


    /// <summary>
    /// Source: https://github.com/hibernating-rhinos/rhino-esb/blob/master/Rhino.ServiceBus/Msmq/NativeMethods.cs
    /// </summary>
    internal static class NativeMethods
    {
        public const int MQ_MOVE_ACCESS = 4;
        public const int MQ_DENY_NONE = 0;

        [DllImport("mqrt.dll", CharSet = CharSet.Unicode)]
        public static extern int MQOpenQueue(string formatName, int access, int shareMode, ref IntPtr hQueue);

        [DllImport("mqrt.dll")]
        public static extern int MQCloseQueue(IntPtr queue);

        [DllImport("mqrt.dll")]
        public static extern int MQMoveMessage(IntPtr sourceQueue, IntPtr targetQueue, long lookupID, IDtcTransaction transaction);

        [DllImport("mqrt.dll")]
        internal static extern int MQMgmtGetInfo([MarshalAs(UnmanagedType.BStr)]string computerName, [MarshalAs(UnmanagedType.BStr)]string objectName, ref MQMGMTPROPS mgmtProps);

        public const byte VT_NULL = 1;
        public const byte VT_UI4 = 19;
        public const int PROPID_MGMT_QUEUE_MESSAGE_COUNT = 7;

        //size must be 16
        [StructLayout(LayoutKind.Sequential)]
        internal struct MQPROPVariant
        {
            public byte vt;       //0
            public byte spacer;   //1
            public short spacer2; //2
            public int spacer3;   //4
            public uint ulVal;    //8
            public int spacer4;   //12
        }

        //size must be 16 in x86 and 28 in x64
        [StructLayout(LayoutKind.Sequential)]
        internal struct MQMGMTPROPS
        {
            public uint cProp;
            public IntPtr aPropID;
            public IntPtr aPropVar;
            public IntPtr status;

        }
    }
}
