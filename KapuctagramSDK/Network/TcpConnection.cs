using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Kapuctagram.Sdk.Protocol;

namespace Kapuctagram.Sdk.Network
{
    internal class TcpConnection : IDisposable
    {
        private TcpClient _client;
        private NetworkStream _stream;
        private CancellationTokenSource _cts;
        private bool _disposed;

        public event Func<(char Type, string Data), Task> OnMessageReceived;

        public async Task ConnectAsync(string host, int port)
        {
            _client = new TcpClient();
            await _client.ConnectAsync(host, port);
            _stream = _client.GetStream();
            _cts = new CancellationTokenSource();
        }

        public async Task SendMessageAsync(char type, string data)
        {
            if (_stream == null) throw new InvalidOperationException("Not connected");
            await MessageParser.WriteMessageAsync(_stream, type, data);
        }

        public async Task SendFileContentAsync(string filePath)
        {
            if (_stream == null) throw new InvalidOperationException("Not connected");
            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, true);
            await fs.CopyToAsync(_stream, 81920);
            await _stream.FlushAsync();
        }

        public void StartReceiving()
        {
            _ = Task.Run(ReceiveLoopAsync);
        }

        private async Task ReceiveLoopAsync()
        {
            try
            {
                while (!_cts.IsCancellationRequested && _client.Connected)
                {
                    var (type, data) = await MessageParser.ReadMessageAsync(_stream);
                    if (OnMessageReceived != null)
                        await OnMessageReceived.Invoke((type, data));
                }
            }
            catch (Exception ex)
            {
                
            }
        }

        public async Task DisconnectAsync()
        {
            _cts?.Cancel();
            _stream?.Dispose();
            _client?.Close();
        }

        public void Dispose()
        {
            _disposed = true;
            _cts?.Cancel();
            _stream?.Dispose();
            _client?.Dispose();
        }
    }
}