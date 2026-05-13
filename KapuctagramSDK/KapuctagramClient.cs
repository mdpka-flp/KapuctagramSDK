using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Kapuctagram.Sdk.Models;
using Kapuctagram.Sdk.Network;

namespace Kapuctagram.Sdk
{
    public class KapuctagramClient : IDisposable
    {
        private readonly TcpConnection _connection = new TcpConnection();
        private UserInfo _currentUser;
        private bool _isAuthenticated;

        public event Func<ChatMessage, Task> OnMessageReceived;
        public event Func<long, Task> OnNewChat;  // уведомление о новом чате

        public async Task ConnectAsync(string host, int port)
        {
            await _connection.ConnectAsync(host, port);
            _connection.OnMessageReceived += OnRawMessageReceived;
            _connection.StartReceiving();
        }

        public async Task<UserInfo> AuthenticateAsync(string password, string desiredName)
        {
            if (_connection == null) throw new InvalidOperationException("Not connected");

            var tcs = new TaskCompletionSource<UserInfo>();
            async Task Handler((char Type, string Data) raw)
            {
                if (raw.Type == 'A')
                {
                    var parts = raw.Data.Split(new[] { " | " }, StringSplitOptions.None);
                    if (parts.Length == 2)
                    {
                        var user = new UserInfo { Id = parts[0], Name = parts[1] };
                        tcs.TrySetResult(user);
                    }
                    else
                    {
                        tcs.TrySetException(new Exception("Invalid authentication response"));
                    }
                    _connection.OnMessageReceived -= Handler;
                }
            }
            _connection.OnMessageReceived += Handler;
            await _connection.SendMessageAsync('A', $"{password} | {desiredName}");
            var user = await tcs.Task;
            _currentUser = user;
            _isAuthenticated = true;
            return user;
        }

        private async Task OnRawMessageReceived((char Type, string Data) raw)
        {
            switch (raw.Type)
            {
                case 'M':
                    var mParts = raw.Data.Split('|');
                    if (mParts.Length >= 3 && long.TryParse(mParts[0], out long chatIdM))
                    {
                        var msg = new ChatMessage
                        {
                            Type = 'T',
                            ChatId = chatIdM,
                            SenderName = mParts[1],
                            Text = mParts[2]
                        };
                        if (OnMessageReceived != null)
                            await OnMessageReceived.Invoke(msg);
                    }
                    break;

                case 'F':
                    var fParts = raw.Data.Split('|');
                    if (fParts.Length >= 4 && long.TryParse(fParts[0], out long chatIdF) && long.TryParse(fParts[3], out long fileId))
                    {
                        var msg = new ChatMessage
                        {
                            Type = 'F',
                            ChatId = chatIdF,
                            SenderName = fParts[1],
                            FileName = fParts[2],
                            FileId = fileId,
                            Text = $"{fParts[1]} отправил файл: {fParts[2]} (ID: {fileId})"
                        };
                        if (OnMessageReceived != null)
                            await OnMessageReceived.Invoke(msg);
                    }
                    break;

                case 'S':
                    var chatIds = raw.Data.Split(',').Where(id => long.TryParse(id, out _)).Select(long.Parse).ToList();
                    if (OnUserChatsReceived != null)
                        await OnUserChatsReceived.Invoke(chatIds);
                    break;

                case 'N':
                    if (long.TryParse(raw.Data, out long newChatId))
                    {
                        if (OnNewChat != null)
                            await OnNewChat.Invoke(newChatId);
                    }
                    break;

                case 'C':
                case 'J':
                case 'I':
                case 'K':
                case 'U':
                case 'H':
                case 'E':
                    break;
            }
        }

        public event Func<List<long>, Task> OnUserChatsReceived;

        public async Task<List<long>> GetUserChatsAsync()
        {
            if (!_isAuthenticated) throw new InvalidOperationException("Not authenticated");
            var tcs = new TaskCompletionSource<List<long>>();
            async Task Handler((char Type, string Data) raw)
            {
                if (raw.Type == 'S')
                {
                    var ids = raw.Data.Split(',').Where(id => long.TryParse(id, out _)).Select(long.Parse).ToList();
                    tcs.TrySetResult(ids);
                    _connection.OnMessageReceived -= Handler;
                }
            }
            _connection.OnMessageReceived += Handler;
            await _connection.SendMessageAsync('S', "");
            return await tcs.Task;
        }

        public async Task<ChatInfo> CreateChatAsync(string type, string name, string password, string targetUserId = null)
        {
            if (!_isAuthenticated) throw new InvalidOperationException("Not authenticated");
            var tcs = new TaskCompletionSource<ChatInfo>();
            async Task Handler((char Type, string Data) raw)
            {
                if (raw.Type == 'C')
                {
                    var parts = raw.Data.Split('|');
                    if (parts[0] == "ERROR")
                        tcs.TrySetException(new Exception(parts[1]));
                    else if (long.TryParse(parts[0], out long chatId))
                    {
                        var chat = new ChatInfo { Id = chatId, Type = type, Name = name, Password = password };
                        tcs.TrySetResult(chat);
                    }
                    _connection.OnMessageReceived -= Handler;
                }
            }
            _connection.OnMessageReceived += Handler;
            string data = $"{type}|{name}|{password}";
            if (type == "P" && !string.IsNullOrEmpty(targetUserId))
                data += $"|{targetUserId}";
            await _connection.SendMessageAsync('C', data);
            return await tcs.Task;
        }

        public async Task JoinChatAsync(long chatId)
        {
            if (!_isAuthenticated) throw new InvalidOperationException("Not authenticated");
            var tcs = new TaskCompletionSource<bool>();
            async Task Handler((char Type, string Data) raw)
            {
                if (raw.Type == 'J')
                {
                    if (raw.Data == "OK") tcs.TrySetResult(true);
                    else tcs.TrySetException(new Exception(raw.Data));
                    _connection.OnMessageReceived -= Handler;
                }
            }
            _connection.OnMessageReceived += Handler;
            await _connection.SendMessageAsync('J', chatId.ToString());
            await tcs.Task;
        }

        public async Task LeaveChatAsync(long chatId)
        {
            if (!_isAuthenticated) throw new InvalidOperationException("Not authenticated");
            await _connection.SendMessageAsync('L', chatId.ToString());
        }

        public async Task SendMessageToChatAsync(long chatId, string text)
        {
            if (!_isAuthenticated) throw new InvalidOperationException("Not authenticated");
            await _connection.SendMessageAsync('M', $"{chatId}|{text}");
        }

        public async Task SendFileToChatAsync(long chatId, string filePath)
        {
            if (!_isAuthenticated) throw new InvalidOperationException("Not authenticated");
            var fileInfo = new FileInfo(filePath);
            const long maxServerFileSize = 2L * 1024 * 1024 * 1024;
            if (fileInfo.Length > maxServerFileSize)
                throw new NotSupportedException("Файлы >2 ГБ пока не поддерживаются");
            string fileName = Path.GetFileName(filePath);
            string header = $"{chatId}|{fileName}|{fileInfo.Length}";
            await _connection.SendMessageAsync('F', header);
            await _connection.SendFileContentAsync(filePath);
        }

        public async Task<ChatInfo> GetChatInfoAsync(long chatId)
        {
            if (!_isAuthenticated) throw new InvalidOperationException("Not authenticated");
            var tcs = new TaskCompletionSource<ChatInfo>();
            async Task Handler((char Type, string Data) raw)
            {
                if (raw.Type == 'I')
                {
                    var parts = raw.Data.Split('|');
                    if (parts.Length >= 5)
                    {
                        var chat = new ChatInfo
                        {
                            Id = long.Parse(parts[0]),
                            Name = parts[1],
                            Type = parts[2],
                            AdminIds = parts[3].Split(',').Where(s => !string.IsNullOrEmpty(s)).Select(long.Parse).ToList(),
                            BannedIds = parts[4].Split(',').Where(s => !string.IsNullOrEmpty(s)).Select(long.Parse).ToList(),
                            ParticipantIds = parts[5].Split(',').Where(s => !string.IsNullOrEmpty(s)).Select(long.Parse).ToList(),
                            OwnerId = parts.Length > 6 ? long.Parse(parts[6]) : 0,
                            MaxMembers = parts.Length > 7 ? int.Parse(parts[7]) : int.MaxValue,
                            Password = parts.Length > 8 ? parts[8] : ""
                        };
                        tcs.TrySetResult(chat);
                    }
                    _connection.OnMessageReceived -= Handler;
                }
            }
            _connection.OnMessageReceived += Handler;
            await _connection.SendMessageAsync('I', chatId.ToString());
            return await tcs.Task;
        }

        public async Task<List<SearchResult>> SearchAsync(string query)
        {
            if (!_isAuthenticated) throw new InvalidOperationException("Not authenticated");
            var tcs = new TaskCompletionSource<List<SearchResult>>();
            async Task Handler((char Type, string Data) raw)
            {
                if (raw.Type == 'Q')
                {
                    var results = new List<SearchResult>();
                    var lines = raw.Data.Split('\n', StringSplitOptions.RemoveEmptyEntries);
                    foreach (var line in lines)
                    {
                        var parts = line.Split('|');
                        if (parts.Length >= 4)
                        {
                            var result = new SearchResult
                            {
                                Id = long.Parse(parts[0]),
                                Type = parts[1],
                                Name = parts[2],
                                Display = parts[3]
                            };
                            results.Add(result);
                        }
                    }
                    tcs.TrySetResult(results);
                    _connection.OnMessageReceived -= Handler;
                }
            }
            _connection.OnMessageReceived += Handler;
            await _connection.SendMessageAsync('Q', query);
            return await tcs.Task;
        }

        public async Task<List<string>> GetChatHistoryAsync(long chatId, int count = 50)
        {
            if (!_isAuthenticated) throw new InvalidOperationException("Not authenticated");
            var tcs = new TaskCompletionSource<List<string>>();
            async Task Handler((char Type, string Data) raw)
            {
                if (raw.Type == 'H')
                {
                    int firstPipe = raw.Data.IndexOf('|');
                    if (firstPipe >= 0 && long.TryParse(raw.Data.Substring(0, firstPipe), out long returnedChatId) && returnedChatId == chatId)
                    {
                        string historyPart = raw.Data.Substring(firstPipe + 1);
                        var messages = historyPart.Split('\n', StringSplitOptions.RemoveEmptyEntries).ToList();
                        tcs.TrySetResult(messages);
                    }
                    else
                    {
                        tcs.TrySetResult(new List<string>());
                    }
                    _connection.OnMessageReceived -= Handler;
                }
            }
            _connection.OnMessageReceived += Handler;
            await _connection.SendMessageAsync('H', $"{chatId}|{count}");
            return await tcs.Task;
        }

        public async Task UpdateChatSettingsAsync(long chatId, string newName, string newPassword)
        {
            if (!_isAuthenticated) throw new InvalidOperationException("Not authenticated");
            await _connection.SendMessageAsync('U', $"{chatId}|{newName}|{newPassword}");
        }

        public async Task BanUserAsync(long chatId, long userId)
        {
            if (!_isAuthenticated) throw new InvalidOperationException("Not authenticated");
            await _connection.SendMessageAsync('K', $"{chatId}|{userId}|ban");
        }

        public async Task UnbanUserAsync(long chatId, long userId)
        {
            if (!_isAuthenticated) throw new InvalidOperationException("Not authenticated");
            await _connection.SendMessageAsync('K', $"{chatId}|{userId}|unban");
        }

        public async Task AddAdminAsync(long chatId, long userId)
        {
            if (!_isAuthenticated) throw new InvalidOperationException("Not authenticated");
            await _connection.SendMessageAsync('K', $"{chatId}|{userId}|makeAdmin");
        }

        public async Task RemoveAdminAsync(long chatId, long userId)
        {
            if (!_isAuthenticated) throw new InvalidOperationException("Not authenticated");
            await _connection.SendMessageAsync('K', $"{chatId}|{userId}|removeAdmin");
        }

        public async Task DownloadFileAsync(long chatId, long fileId, string saveFilePath)
        {
            if (!_isAuthenticated) throw new InvalidOperationException("Not authenticated");
            await _connection.DownloadFileAsync(chatId, fileId, saveFilePath);
        }

        [Obsolete("Use SendMessageToChatAsync instead")]
        public async Task SendTextAsync(string text) { }

        [Obsolete("Use SendFileToChatAsync instead")]
        public async Task SendFileAsync(string filePath) { }

        public void Dispose()
        {
            _connection.Dispose();
        }
    }
}