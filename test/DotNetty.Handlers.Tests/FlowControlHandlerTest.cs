namespace DotNetty.Handlers.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;

    using DotNetty.Buffers;
    using DotNetty.Codecs;
    using DotNetty.Common.Utilities;
    using DotNetty.Handlers.Flow;
    using DotNetty.Tests.Common;
    using DotNetty.Transport.Bootstrapping;
    using DotNetty.Transport.Channels;
    using DotNetty.Transport.Channels.Sockets;

    using Xunit.Abstractions;

    using Xunit;

    public sealed class FlowControlHandlerTest : TestBase
    {
        static IEventLoopGroup group;

        public FlowControlHandlerTest(ITestOutputHelper output)
            : base(output)
        {
            group = new MultithreadEventLoopGroup();
        }

        [Fact]
        public async Task TestAutoReadingOn()
        {
            var latch = new CountdownEvent(3);

            var handler = new ActionBasedHandlerAdapter(null, (context, message) => { latch.Signal(); });

            IChannel server = await NewServer(true, handler);
            IChannel client = await NewClient(server.LocalAddress);

            try
            {
                await client.WriteAndFlushAsync(NewOneMessage());

                Assert.True(latch.Wait(1000));
            }
            finally
            {
                await client.CloseAsync();
                await server.CloseAsync();
            }
        }

        [Fact]
        public async Task TestAutoReadingOff()
        {
            var latch = new CountdownEvent(3);

            IChannel peer = null;
            var handler = new ActionBasedHandlerAdapter(
                context =>
                    {
                        peer = context.Channel;
                        context.FireChannelActive();
                    },
                (context, message) =>
                    {
                        ReferenceCountUtil.Release(message);
                        latch.Signal();
                    });

            IChannel server = await NewServer(false, handler);
            IChannel client = await NewClient(server.LocalAddress);

            try
            {
                await client.WriteAndFlushAsync(NewOneMessage());

                SpinWait.SpinUntil(() => peer != null, 1000);

                peer.Read();

                Assert.True(latch.Wait(1000));
            }
            finally
            {
                await client.CloseAsync();
                await server.CloseAsync();
            }
        }

        [Fact]
        public async Task TestFlowAutoReadingOn()
        {
            var latch = new CountdownEvent(3);

            var flow = new FlowControlHandler();

            var handler = new ActionBasedHandlerAdapter(null, (context, message) => { latch.Signal(); });

            IChannel server = await NewServer(true, flow, handler);
            IChannel client = await NewClient(server.LocalAddress);

            try
            {
                await client.WriteAndFlushAsync(NewOneMessage());

                Assert.True(latch.Wait(1000));

                Assert.True(flow.IsQueueEmpty);
            }
            finally
            {
                await client.CloseAsync();
                await server.CloseAsync();
            }
        }

        [Fact]
        public async Task TestFlowAutoReadingToggle()
        {
            var messageReceivedLatch1 = new CountdownEvent(1);
            var messageReceivedLatch2 = new CountdownEvent(1);
            var messageReceivedLatch3 = new CountdownEvent(1);
            var setAutoReadLatch1 = new CountdownEvent(1);
            var setAutoReadLatch2 = new CountdownEvent(1);
            
            var flow = new FlowControlHandler();

            IChannel peer = null;
            int messageReceivedCount = 0;
            int expectedMessageCount = 0;

            var handler = new ActionBasedHandlerAdapter(
                context =>
                    {
                        peer = context.Channel;
                        context.FireChannelActive();
                    },
                (context, message) =>
                    {
                        ReferenceCountUtil.Release(message);

                        context.Channel.Configuration.AutoRead = false;

                        if (messageReceivedCount++ != expectedMessageCount)
                        {
                            return;
                        }

                        switch (messageReceivedCount)
                        {
                            case 1:
                                messageReceivedLatch1.Signal();
                                if (setAutoReadLatch1.Wait(1000))
                                {
                                    ++expectedMessageCount;
                                }
                                break;
                            case 2:
                                messageReceivedLatch2.Signal();
                                if (setAutoReadLatch2.Wait(1000))
                                {
                                    ++expectedMessageCount;
                                }
                                break;
                            default:
                                messageReceivedLatch3.Signal();
                                break;
                        }
                    });

            IChannel server = await NewServer(true, flow, handler);
            IChannel client = await NewClient(server.LocalAddress);

            try
            {
                await client.WriteAndFlushAsync(NewOneMessage());

                // ChannelRead(1)
                Assert.True(messageReceivedLatch1.Wait(1000));

                // ChannelRead(2)
                peer.Configuration.AutoRead = true;
                setAutoReadLatch1.Signal();
                Assert.True(messageReceivedLatch2.Wait(1000));

                // ChannelRead(3)
                peer.Configuration.AutoRead = true;
                setAutoReadLatch2.Signal();
                Assert.True(messageReceivedLatch3.Wait(1000));

                Assert.True(flow.IsQueueEmpty);
            }
            finally
            {
                await client.CloseAsync();
                await server.CloseAsync();
            }
        }

        [Fact]
        public async Task TestFlowAutoReadingOff()
        {
            var messageReceivedLatch1 = new CountdownEvent(1);
            var messageReceivedLatch2 = new CountdownEvent(2);
            var messageReceivedLatch3 = new CountdownEvent(3);

            var flow = new FlowControlHandler();

            IChannel peer = null;
            var handler = new ActionBasedHandlerAdapter(
                context =>
                    {
                        peer = context.Channel;
                        context.FireChannelActive();
                    },
                (context, message) =>
                    {
                        if (!messageReceivedLatch1.IsSet)
                        {
                            messageReceivedLatch1.Signal();
                        }

                        if (!messageReceivedLatch2.IsSet)
                        {
                            messageReceivedLatch2.Signal();
                        }

                        if (!messageReceivedLatch3.IsSet)
                        {
                            messageReceivedLatch3.Signal();
                        }
                    });

            IChannel server = await NewServer(false, flow, handler);
            IChannel client = await NewClient(server.LocalAddress);

            try
            {
                await client.WriteAndFlushAsync(NewOneMessage());

                SpinWait.SpinUntil(() => peer != null, 1000);

                // ChannelRead(1)
                peer.Read();
                Assert.True(messageReceivedLatch1.Wait(1000));

                // ChannelRead(3)
                peer.Read();
                Assert.True(messageReceivedLatch2.Wait(1000));

                // ChannelRead(3)
                peer.Read();
                Assert.True(messageReceivedLatch3.Wait(1000));

                Assert.True(flow.IsQueueEmpty);
            }
            finally
            {
                await client.CloseAsync();
                await server.CloseAsync();
            }
        }

        static async Task<IChannel> NewServer(bool autoRead, params IChannelHandler[] handlers)
        {
            Assert.True(handlers.Length >= 1);

            var serverBootstrap = new ServerBootstrap();
            return await serverBootstrap.Group(group)
                       .Channel<TcpServerSocketChannel>()
                       .ChildOption(ChannelOption.AutoRead, autoRead)
                       .ChildHandler(
                           new ActionChannelInitializer<IChannel>(
                               ch =>
                                   {
                                       ch.Pipeline.AddLast(new OneByteToThreeStringsDecoder());
                                       ch.Pipeline.AddLast(handlers);
                                   }))
                       .BindAsync(IPAddress.Loopback, 8888);
        }

        static async Task<IChannel> NewClient(EndPoint server)
        {
            var bootstrap = new Bootstrap();
            return await bootstrap.Group(group)
                       .Channel<TcpSocketChannel>()
                       .Option(ChannelOption.ConnectTimeout, TimeSpan.FromSeconds(1))
                       .Handler(
                           new ActionBasedHandlerAdapter(
                               null,
                               (context, message) => Assert.True(
                                   false,
                                   "In this test the client is never receiving a message from the server.")))
                       .ConnectAsync(server);
        }

        static IByteBuffer NewOneMessage()
        {
            return Unpooled.WrappedBuffer(new byte[] { 1 });
        }

        sealed class ActionBasedHandlerAdapter : ChannelHandlerAdapter
        {
            Action<IChannelHandlerContext> ChannelActiveAction { get; }

            Action<IChannelHandlerContext, object> ChannelReadAction { get; }

            public ActionBasedHandlerAdapter(
                Action<IChannelHandlerContext> channelActiveAction,
                Action<IChannelHandlerContext, object> channelReadAction)
            {
                this.ChannelActiveAction = channelActiveAction;
                this.ChannelReadAction = channelReadAction;
            }

            public override void ChannelActive(IChannelHandlerContext context)
            {
                this.ChannelActiveAction?.Invoke(context);
            }

            public override void ChannelRead(IChannelHandlerContext context, object message)
            {
                this.ChannelReadAction?.Invoke(context, message);
            }
        }

        sealed class OneByteToThreeStringsDecoder : ByteToMessageDecoder
        {
            protected override void Decode(IChannelHandlerContext context, IByteBuffer input, List<object> output)
            {
                for (int i = 0; i < input.ReadableBytes; i++)
                {
                    output.Add("1");
                    output.Add("2");
                    output.Add("3");
                }

                input.SetReaderIndex(input.ReadableBytes);
            }
        }
    }
}
