namespace Kapuctagram.Sdk.Models
{
    public class ChatMessage
    {
        public char Type { get; set; }
        public long ChatId { get; set; }
        public long SenderId { get; set; }
        public string SenderName { get; set; }
        public string Text { get; set; }
        public string FileName { get; set; }
        public long FileSize { get; set; }
        public long FileId { get; set; }
    }
}