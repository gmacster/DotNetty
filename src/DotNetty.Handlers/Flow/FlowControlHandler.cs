// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace DotNetty.Handlers.Flow
{
    using DotNetty.Codecs;
    using DotNetty.Common;
    using DotNetty.Common.Internal;
    using DotNetty.Common.Internal.Logging;
    using DotNetty.Common.Utilities;
    using DotNetty.Transport.Channels;

    /// <summary>
    /// The <see cref="FlowControlHandler"/> ensures that only one message per <see cref="Read"/> is sent downstream.
    /// Classes such as <see cref="ByteToMessageDecoder"/> or <see cref="MessageToByteEncoder{T}"/> are free to emit as
    /// many events as they like for any given input. A channel's auto reading configuration doesn't usually
    /// apply in these scenarios.This is causing problems in downstream <see cref="IChannelHandler"/>s that would
    /// like to hold subsequent events while they're processing one event. It's a common problem with the
    /// <c>HttpObjectDecoder</c> that will very often fire a <c>HttpRequest</c> that is immediately followed
    /// by a <c>ILastHttpContent</c> event.
    /// </summary>
    /// <example>
    /// <code>
    /// IChannelPipeline pipeline = ...;
    /// pipeline.AddLast(new HttpServerCodec());
    /// pipeline.AddLast(new FlowControlHandler());
    /// pipeline.AddLast(new MyExampleHandler());
    /// 
    /// class MyExampleHandler : SimpleChannelInboundHandler&lt;HttpRequest&gt;
    /// {
    ///     protected override void ChannelRead0(IChannelHandlerContext ctx, HttpRequest msg)
    ///     {
    ///         ctx.Channel.Configuration.SetOption(ChannelOption.AutoRead, false);
    ///         // The FlowControlHandler will hold any subsequent events that
    ///         // were emitted by HttpObjectDecoder until auto reading is turned
    ///         // back on or IChannel.Read() is being called.
    ///     }
    /// }
    /// </code>
    /// </example>
    /// <seealso cref="ChannelOption.AutoRead"/>
    public class FlowControlHandler : ChannelDuplexHandler
    {
        static readonly IInternalLogger Logger = InternalLoggerFactory.GetInstance<FlowControlHandler>();

        static readonly ThreadLocalPool<RecyclableQueue> Recycler = new ThreadLocalPool<RecyclableQueue>(h => new RecyclableQueue(h));

        readonly bool releaseMessages;

        RecyclableQueue queue;

        IChannelConfiguration config;

        bool shouldConsume;

        public FlowControlHandler()
            : this(true)
        {
        }

        public FlowControlHandler(bool releaseMessages)
        {
            this.releaseMessages = releaseMessages;
        }

        /// <summary>
        /// Determine if the underlying <see cref="IQueue{T}"/> is empty.
        /// This method exists for testing, debugging and inspection purposes and it is not Thread safe!
        /// </summary>
        public bool IsQueueEmpty => this.queue.IsEmpty;

        /// <summary>
        /// Releases all messages and destroys the <see cref="IQueue{T}"/>.
        /// </summary>
        void Destroy()
        {
            if (this.queue != null)
            {
                if (!this.queue.IsEmpty)
                {
                    Logger.Trace($"Non-empty queue: {this.queue}");

                    if (this.releaseMessages)
                    {
                        while (this.queue.TryDequeue(out object msg))
                        {
                            ReferenceCountUtil.SafeRelease(msg);
                        }
                    }
                }

                this.queue.Recycle();
                this.queue = null;
            }
        }

        public override void HandlerAdded(IChannelHandlerContext ctx)
        {
            this.config = ctx.Channel.Configuration;
        }

        public override void ChannelInactive(IChannelHandlerContext ctx)
        {
            this.Destroy();
            ctx.FireChannelInactive();
        }

        public override void Read(IChannelHandlerContext ctx)
        {
            if (this.Dequeue(ctx, 1) == 0)
            {
                // It seems no messages were consumed. We need to read() some
                // messages from upstream and once one arrives it need to be
                // relayed to downstream to keep the flow going.
                this.shouldConsume = true;
                ctx.Read();
            }
        }

        public override void ChannelRead(IChannelHandlerContext ctx, object msg)
        {
            if (this.queue == null)
            {
                this.queue = Recycler.Take();
            }

            this.queue.TryEnqueue(msg);

            // We just received one message. Do we need to relay it regardless
            // of the auto reading configuration? The answer is yes if this
            // method was called as a result of a prior read() call.
            int minConsume = this.shouldConsume ? 1 : 0;
            this.shouldConsume = false;

            this.Dequeue(ctx, minConsume);
        }

        public override void ChannelReadComplete(IChannelHandlerContext ctx)
        {
            // Don't relay completion events from upstream as they
            // make no sense in this context. See dequeue() where
            // a new set of completion events is being produced.
        }

        /// <summary>
        /// Dequeues one or many (or none) messages depending on the channel's auto
        /// reading state and returns the number of messages that were consumed from
        /// the internal queue.
        /// </summary>
        /// <param name="ctx">The channel handler context.</param>
        /// <param name="minConsume">
        /// Used to force <see cref="Dequeue"/> into consuming that number of messages regardless of the channel's auto
        /// reading configuration.
        /// </param>
        /// <returns>The number of dequeued messages.</returns>
        /// <seealso cref="Read"/>
        /// <seealso cref="ChannelRead"/>
        int Dequeue(IChannelHandlerContext ctx, int minConsume)
        {
            if (this.queue != null)
            {
                int consumed = 0;

                while ((consumed < minConsume || this.config.AutoRead) && this.queue.TryDequeue(out object msg))
                {
                    ++consumed;
                    ctx.FireChannelRead(msg);
                }

                // We're firing a completion event every time one (or more)
                // messages were consumed and the queue ended up being drained
                // to an empty state.
                if (this.queue.IsEmpty && consumed > 0)
                {
                    ctx.FireChannelReadComplete();
                }

                return consumed;
            }

            return 0;
        }
    }

    sealed class RecyclableQueue : CompatibleConcurrentQueue<object>
    {
        readonly ThreadLocalPool.Handle handle;

        internal RecyclableQueue(ThreadLocalPool.Handle handle)
        {
            this.handle = handle;
        }

        public void Recycle()
        {
            ((IQueue<object>)this).Clear();
            this.handle.Release(this);
        }
    }
}