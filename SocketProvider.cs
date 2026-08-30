using System.Buffers;
using System.Net;
using System.Net.Sockets;
using Terraria.Net;
using Terraria.Net.Sockets;

namespace FakePlayer;

public abstract class BaseSock : ISocket
{
    protected TcpClient conn;
    protected TcpListener? listener;
    protected SocketConnectionAccepted? listenerCb;
    protected RemoteAddress? remoteAddr;
    protected bool isListening;

    protected BaseSock() => conn = new TcpClient { NoDelay = true };

    protected BaseSock(TcpClient tcpClient)
    {
        conn = tcpClient;
        conn.NoDelay = true;
        if (tcpClient.Client.RemoteEndPoint is IPEndPoint ipe)
            remoteAddr = new TcpAddress(ipe.Address, ipe.Port);
    }

    void ISocket.Close()
    {
        remoteAddr = null;
        conn?.Close();
    }

    bool ISocket.IsConnected() => conn?.Client != null && conn.Connected;

    void ISocket.Connect(RemoteAddress addr)
    {
        var tcp = (TcpAddress)addr;
        conn!.Connect(tcp.Address, tcp.Port);
        remoteAddr = addr;
    }

    bool ISocket.IsDataAvailable() => conn!.GetStream().DataAvailable;

    RemoteAddress ISocket.GetRemoteAddress() => remoteAddr!;

    void ISocket.StopListening() => isListening = false;

    bool ISocket.StartListening(SocketConnectionAccepted cb)
    {
        var ip = IPAddress.Any;
        if (Terraria.Program.LaunchParameters.TryGetValue("-ip", out var val) && !IPAddress.TryParse(val, out ip))
            ip = IPAddress.Any;

        isListening = true;
        listenerCb = cb;
        listener ??= new TcpListener(ip, Terraria.Netplay.ListenPort);

        try { listener.Start(); }
        catch { return false; }

        ThreadPool.QueueUserWorkItem(_ => ListenLoop());
        return true;
    }

    internal void ListenLoop()
    {
        while (isListening && !Terraria.Netplay.Disconnect)
        {
            try
            {
                var sock = New(listener!.AcceptTcpClient());
                Console.WriteLine(Terraria.Localization.Language.GetTextValue("Net.ClientConnecting", sock.GetRemoteAddress()));
                listenerCb!(sock);
            }
            catch { }
        }
        listener!.Stop();
        Terraria.Netplay.IsListening = false;
    }

    internal abstract ISocket New(TcpClient client);
    public abstract void AsyncSend(byte[] data, int off, int len, SocketSendCallback cb, object? state = null);
    public abstract void AsyncReceive(byte[] data, int off, int len, SocketReceiveCallback cb, object? state = null);
}

public class PoolSock : BaseSock
{
    internal bool EnforceMsgSize;

    public PoolSock() : base() { }
    public PoolSock(TcpClient tc) : base(tc) { }

    internal override ISocket New(TcpClient client) =>
        new PoolSock(client) { EnforceMsgSize = this.EnforceMsgSize };

    public override async void AsyncSend(byte[] data, int off, int len, SocketSendCallback cb, object? state = null)
    {
        var cts = new CancellationTokenSource(10000);
        var buf = ArrayPool<byte>.Shared.Rent(len);
        try
        {
            Buffer.BlockCopy(data, off, buf, 0, len);
            if (EnforceMsgSize && len >= 3)
            {
                var claimed = BitConverter.ToInt16(buf, 0);
                if (claimed != len)
                    TShockAPI.TShock.Log.ConsoleWarn($"[PoolSock] Size mismatch: {len} != {claimed}");
                else
                    await conn.GetStream().WriteAsync(buf.AsMemory(0, len), cts.Token);
            }
            else
            {
                await conn.GetStream().WriteAsync(buf.AsMemory(0, len), cts.Token);
            }
            cb(state);
        }
        catch { }
        finally
        {
            ArrayPool<byte>.Shared.Return(buf);
            cts.Dispose();
        }
    }

    public override async void AsyncReceive(byte[] data, int off, int len, SocketReceiveCallback cb, object? state = null)
    {
        try
        {
            var read = await conn.GetStream().ReadAsync(data.AsMemory(off, len));
            cb(state, read);
        }
        catch { }
    }
}

public class SimpleSock : BaseSock
{
    public SimpleSock() : base() { }
    public SimpleSock(TcpClient tc) : base(tc) { }
    internal override ISocket New(TcpClient client) => new SimpleSock(client);

    public override void AsyncSend(byte[] data, int off, int len, SocketSendCallback cb, object? state = null)
    {
        cb(state);
        conn!.GetStream().Write(data, off, len);
    }

    public override void AsyncReceive(byte[] data, int off, int len, SocketReceiveCallback cb, object? state = null)
    {
        var read = conn.GetStream().Read(data, off, len);
        cb(state, read);
    }
}