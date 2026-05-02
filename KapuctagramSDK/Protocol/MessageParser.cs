using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Kapuctagram.Sdk.Protocol
{
    public static class MessageParser
    {
        public static async Task<(char Type, string Data)> ReadMessageAsync(NetworkStream stream)
        {
            byte[] typeBuf = new byte[1];
            await ReadExactlyAsync(stream, typeBuf);
            char type = (char)typeBuf[0];

            byte[] lenBuf = new byte[4];
            await ReadExactlyAsync(stream, lenBuf);
            int length = BitConverter.ToInt32(lenBuf, 0);

            if (length < 0 || length > 10_000_000)
                throw new InvalidDataException("Invalid message length");

            byte[] data = new byte[length];
            await ReadExactlyAsync(stream, data);
            string dataStr = Encoding.UTF8.GetString(data);
            return (type, dataStr);
        }

        public static async Task WriteMessageAsync(NetworkStream stream, char type, string data)
        {
            byte[] typeBytes = new byte[] { (byte)type };
            byte[] dataBytes = Encoding.UTF8.GetBytes(data);
            byte[] lenBytes = BitConverter.GetBytes(dataBytes.Length);

            await stream.WriteAsync(typeBytes, 0, typeBytes.Length);
            await stream.WriteAsync(lenBytes, 0, lenBytes.Length);
            await stream.WriteAsync(dataBytes, 0, dataBytes.Length);
            await stream.FlushAsync();
        }

        private static async Task ReadExactlyAsync(Stream stream, byte[] buffer)
        {
            int total = 0;
            while (total < buffer.Length)
            {
                int read = await stream.ReadAsync(buffer, total, buffer.Length - total);
                if (read == 0) throw new EndOfStreamException("Connection closed");
                total += read;
            }
        }
    }
}