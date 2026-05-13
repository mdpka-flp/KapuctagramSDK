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
        
        private readonly SemaphoreSlim _writeLock = new SemaphoreSlim(1, 1);
        //private readonly SemaphoreSlim _receiveLock = new SemaphoreSlim(1, 1);
        //private bool _receiveEnabled = true;
        private readonly SemaphoreSlim _readLock = new SemaphoreSlim(1, 1);

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
            await _writeLock.WaitAsync();
            try
            {
                await MessageParser.WriteMessageAsync(_stream, type, data);
            }
            finally
            {
                _writeLock.Release();
            }
        }

        public async Task SendFileContentAsync(string filePath)
        {
            if (_stream == null) throw new InvalidOperationException("Not connected");
            await _writeLock.WaitAsync();
            try
            {
                using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, true);
                await fs.CopyToAsync(_stream, 81920);
                await _stream.FlushAsync();
            }
            finally
            {
                _writeLock.Release();
            }
        }
        
        public async Task DownloadFileAsync(long chatId, long fileId, string saveFilePath) 
        { 
            await _readLock.WaitAsync(); 
            try 
            { 
                await _writeLock.WaitAsync();
                try { await MessageParser.WriteMessageAsync(_stream, 'D', $"{chatId}|{fileId}"); }
                finally { _writeLock.Release(); }
                
                var (type, data) = await MessageParser.ReadMessageAsync(_stream); 
                if (type != 'D') throw new InvalidOperationException("Unexpected server response");
                
                var parts = data.Split('|'); 
                if (parts[0] == "ERROR") throw new Exception(parts[1]); 
                if (parts[0] != "OK" || !long.TryParse(parts[1], out long expectedSize)) 
                    throw new Exception("Invalid file download response"); 
                { 
                    
                    using var fs = new FileStream(saveFilePath, FileMode.Create, FileAccess.Write,
                        FileShare.Read, 81920, FileOptions.WriteThrough); 
                    byte[] buffer = new byte[81920]; 
                    long totalRead = 0;
                    
                    while (totalRead < expectedSize) 
                    { 
                        int toRead = (int)Math.Min(buffer.Length, expectedSize - totalRead); 
                        int read = await _stream.ReadAsync(buffer, 0, toRead); 
                        if (read == 0) throw new EndOfStreamException("Соединение разорвано при загрузке");
                        
                        await fs.WriteAsync(buffer, 0, read); 
                        totalRead += read; 
                    }
                    
                    if (totalRead != expectedSize) 
                        throw new Exception($"Несоответствие размера: {totalRead}/{expectedSize}");
                    
                    fs.Flush(true); 
                }
                
                var (endType, _) = await MessageParser.ReadMessageAsync(_stream); 
                if (endType != 'E') throw new InvalidOperationException($"Ожидается 'E', получено '{endType}'"); 
            }
            finally { _readLock.Release(); } 
        }
        
        public void StartReceiving()
        {
            _ = Task.Run(ReceiveLoopAsync);
        }

        private async Task ReceiveLoopAsync()
        {
            while (!_cts.IsCancellationRequested && _client.Connected)
            {
                try
                {
                    await _readLock.WaitAsync();
                    var (type, data) = await MessageParser.ReadMessageAsync(_stream);
                    _readLock.Release();
            
                    OnMessageReceived?.Invoke((type, data));
                }
                catch (Exception ex)
                {
                    if (_readLock.CurrentCount == 0) _readLock.Release();
                    Console.WriteLine($"[FATAL] ReceiveLoop: {ex.Message}");
            
                    // ⛔ ПРИ ДЕСИНХРОНИЗАЦИИ ПОТОК БОЛЬШЕ НЕ НАДЁЖЕН. 
                    // Продолжать чтение БЕСПОЛЕЗНО и ОПАСНО.
                    _cts?.Cancel(); 
                    break; // Выходим из цикла. Клиент должен переподключиться.
                }
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
            if (_disposed) return;
            _disposed = true;
            _cts?.Cancel();
            _stream?.Dispose();
            _client?.Dispose();
            _writeLock?.Dispose();
            //_receiveLock?.Dispose();
            _readLock?.Dispose();
        }
    }
}