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
        private readonly SemaphoreSlim _receiveLock = new SemaphoreSlim(1, 1);
        private bool _receiveEnabled = true;

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
            // Запрещаем фоновому циклу читать сообщения на время скачивания
            await _receiveLock.WaitAsync();
            _receiveEnabled = false;
            try
            {
                // 1. Отправляем запрос
                await _writeLock.WaitAsync();
                try
                {
                    await MessageParser.WriteMessageAsync(_stream, 'D', $"{chatId}|{fileId}");
                }
                finally
                {
                    _writeLock.Release();
                }

                // 2. Читаем ответ (должен быть 'D|OK|размер' или 'D|ERROR|...')
                var (type, data) = await MessageParser.ReadMessageAsync(_stream);
                if (type != 'D')
                    throw new InvalidOperationException("Unexpected server response");
                
                var parts = data.Split('|');
                if (parts[0] == "ERROR")
                    throw new Exception(parts[1]);
                if (parts[0] != "OK" || !long.TryParse(parts[1], out long expectedSize))
                    throw new Exception("Invalid file download response");

                // 3. Читаем файл напрямую из потока
                using var fs = new FileStream(saveFilePath, FileMode.Create, FileAccess.Write);
                byte[] buffer = new byte[81920];
                long totalRead = 0;
                while (totalRead < expectedSize)
                {
                    int toRead = (int)Math.Min(buffer.Length, expectedSize - totalRead);
                    int read = await _stream.ReadAsync(buffer, 0, toRead);
                    if (read == 0) throw new EndOfStreamException("Connection lost while downloading");
                    await fs.WriteAsync(buffer, 0, read);
                    totalRead += read;
                }
            }
            finally
            {
                _receiveEnabled = true;
                _receiveLock.Release();
            }
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
                    // Проверяем, разрешён ли приём (без захвата блокировки на всё время цикла)
                    bool enabled;
                    await _receiveLock.WaitAsync();
                    enabled = _receiveEnabled;
                    _receiveLock.Release();

                    if (!enabled)
                    {
                        await Task.Delay(10);
                        continue;
                    }

                    try
                    {
                        var (type, data) = await MessageParser.ReadMessageAsync(_stream);
                        if (OnMessageReceived != null)
                            await OnMessageReceived.Invoke((type, data));
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"ReceiveLoop error: {ex.Message}");
                        await Task.Delay(100);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ReceiveLoop fatal: {ex.Message}");
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
            _receiveLock?.Dispose();
        }
    }
}