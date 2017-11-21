// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace DotNetty.Handlers.Flow
{
    using System.Collections.Generic;
    using System.Linq;

    using DotNetty.Common.Internal.Logging;
    using DotNetty.Common.Utilities;
    using DotNetty.Transport.Channels;

    public class FlowControlHandler : ChannelDuplexHandler
    {
        readonly IInternalLogger logger = InternalLoggerFactory.GetInstance(typeof(FlowControlHandler).Name);

        readonly bool releaseMessages;

        Queue<object> queue;

        IChannelConfiguration config;

        bool shouldConsume;

        public bool IsQueueEmpty => this.queue.Count == 0;

        public FlowControlHandler() : this(true)
        {
        }

        public FlowControlHandler(bool releaseMessages)
        {
            this.releaseMessages = releaseMessages;
        }

        void Destroy()
        {
            if (this.queue != null)
            {
                if (queue.Any())
                {
                    logger.Trace("Non-empty queue: {}", queue);

                    if (releaseMessages)
                    {
                        object msg;
                        while ((msg = queue.Dequeue()) != null)
                        {
                            ReferenceCountUtil.SafeRelease(msg);
                        }
                    }
                }

                queue = null;
            }
        }

        int Dequeue(IChannelHandlerContext context, int minConsume)
        {
            if (queue != null)
            {
                int consumed = 0;

                while (consumed < minConsume || config.AutoRead)
                {
                    if (this.queue.Count == 0)
                    {
                        break;
                    }

                    object message = this.queue.Dequeue();
                    ++consumed;
                    context.FireChannelRead(message);
                }

                if (!queue.Any() && consumed > 0)
                {
                    context.FireChannelReadComplete();
                }

                return consumed;
            }

            return 0;
        }

        public override void HandlerAdded(IChannelHandlerContext context)
        {
            config = context.Channel.Configuration;
        }

        public override void ChannelInactive(IChannelHandlerContext context)
        {
            this.Destroy();
            context.FireChannelInactive();
        }

        public override void Read(IChannelHandlerContext context)
        {
            if (this.Dequeue(context, 1) == 0)
            {
                shouldConsume = true;
                context.Read();
            }
        }

        public override void ChannelRead(IChannelHandlerContext context, object message)
        {
            if (this.queue == null)
            {
                this.queue = new Queue<object>(2);
            }

            this.queue.Enqueue(message);

            int minConsume = shouldConsume ? 1 : 0;
            shouldConsume = false;

            this.Dequeue(context, minConsume);
        }

        public override void ChannelReadComplete(IChannelHandlerContext context)
        {
        }
    }
}
