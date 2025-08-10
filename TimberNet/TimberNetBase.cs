using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

namespace TimberNet
{
    // TODO: I should create a method here that attempts to operate on a steam,
    // and handles errors uniformly if it fails.
    // Pretty much any error means the session is over, but the game should show
    // the error rather than crashing.
    public abstract class TimberNetBase
    {
        public const int HEADER_SIZE = 4;
        public const string TICKS_KEY = "ticksSinceLoad";
        public const string TYPE_KEY = "type";
        public const string SET_STATE_EVENT = "SetState";
        public const string HEARTBEAT_EVENT = "Heartbeat";
        public const int MAX_BUFFER_SIZE = 8192 * 4; // 32K
        // --- Binary frame types (first byte of uncompressed payload when length < 0) ---
        // (We keep legacy positive length header for compressed JSON; for binary heartbeat we use a synthetic negative length marker.)
        protected const byte FRAME_HEARTBEAT_V1 = 0x01; // followed by varint tick delta (usually 1)
        // For now we do not negotiate capability; servers always emit legacy JSON unless idle minimal heartbeat path is taken upstream.

        // Varint helper (7-bit encoding) used for tick delta
        protected static int WriteVarUInt32(uint value, Span<byte> buffer)
        {
            int index = 0;
            while (value >= 0x80)
            {
                buffer[index++] = (byte)(value | 0x80);
                value >>= 7;
            }
            buffer[index++] = (byte)value;
            return index;
        }
        protected static int ReadVarUInt32(byte[] buffer, int offset, out uint value)
        {
            int shift = 0;
            uint result = 0;
            int index = offset;
            while (true)
            {
                if (index >= buffer.Length) throw new Exception("VarInt truncated");
                byte b = buffer[index++];
                result |= (uint)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
                if (shift > 35) throw new Exception("VarInt too long");
            }
            value = result;
            return index - offset;
        }

        public delegate void MessageReceived(string message);
        public delegate void MapReceived(byte[] mapBytes);

        public event MessageReceived? OnLog;
        public event MessageReceived? OnError;
        public event MapReceived? OnMapReceived;

        private readonly ConcurrentQueue<string> receivedEventQueue = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> logQueue = new ConcurrentQueue<string>();
        private byte[]? mapBytes = null;

        public bool IsStopped { get; private set; } = false;

        public int Hash { get; private set; } = 17;

        public int TickCount { get; private set; }

    // --- Instrumentation counters (baseline) ---
    private long _totalBytesSent = 0;
    private long _totalPacketsSent = 0;
    private long _totalBytesRecv = 0;
    private long _totalPacketsRecv = 0;
    private long _serializeMicrosAccum = 0;
    private int _framesMeasured = 0;
    private int _lastMetricsLogTick = 0;
    // Rolling window (simple) – reset every MetricsLogIntervalTicks
    private long _windowBytesSent = 0;
    private long _windowPacketsSent = 0;
    private long _windowBytesRecv = 0;
    private long _windowPacketsRecv = 0;
    private long _windowSerializeMicros = 0;
    protected virtual int MetricsLogIntervalTicks => 200; // configurable via override if needed
    // Per-event type counters (compressed bytes)
    private readonly Dictionary<string, (long count, long bytes)> _sentEventType = new Dictionary<string, (long count, long bytes)>();
    private readonly Dictionary<string, (long count, long bytes)> _recvEventType = new Dictionary<string, (long count, long bytes)>();

        public int TicksBehind
        {
            get
            {
                if (receivedEvents.Count == 0)
                    return 0;
                return Math.Max(0, GetTick(receivedEvents.Last()) - TickCount);
            }
        }

        public bool Started { get; private set; }

        public virtual bool ShouldTick => Started;

        protected List<JObject> receivedEvents = new List<JObject>();

        public virtual void Close()
        {
            IsStopped = true;
        }

        public TimberNetBase()
        {
            Log("Started");
        }

        protected void Log(string message)
        {
            Log(message, TickCount, Hash);
        }

        protected void Log(string message, int ticks, int hash)
        {
            // Should be threadsafe
            OnLog?.Invoke($"T{ticks.ToString("D4")} [{hash.ToString("X8")}] : {message}");
            //logQueue.Enqueue($"T{ticks.ToString("D4")} [{hash.ToString("X8")}] : {message}");
        }

        public virtual void Start()
        {
            Started = true;
        }

        public static int GetTick(JObject message)
        {
            if (message[TICKS_KEY] == null)
                throw new Exception($"Message does not contain {TICKS_KEY} key");
            return message[TICKS_KEY]!.ToObject<int>();
        }

        public static string GetType(JObject message)
        {
            var type = message["type"];
            if (type == null)
                throw new Exception($"Message does not contain type key");
            return type.ToObject<string>()!;
        }

        protected void InsertInScript(JObject message, List<JObject> script)
        {
            int tick = GetTick(message);
            int index = script.FindIndex(m => GetTick(m) > tick);

            if (index == -1)
                script.Add(message);
            else
                script.Insert(index, message);
        }

        public static List<T> PopEventsForTick<T>(int tick, List<T> events, Func<T, int> getTick)
        {
            List<T> list = new List<T>();
            while (events.Count > 0)
            {
                T message = events[0];
                int delay = getTick(message);
                if (delay > tick)
                    break;

                events.RemoveAt(0);
                list.Add(message);
            }
            return list;
        }

        private List<JObject> PopEventsToProcess(List<JObject> events)
        {
            if (events.Count == 0) return new List<JObject>();
            JObject firstEvent = events[0];
            int firstEventTick = GetTick(firstEvent);
            if (firstEventTick < TickCount)
                Log($"Warning: late event {GetType(firstEvent)}: {firstEventTick} < {TickCount}");

            return PopEventsForTick(TickCount, events, GetTick);
        }

        /**
         * Process an event that the user initiated.
         */
        public virtual void DoUserInitiatedEvent(JObject message)
        {
            AddEventToHash(message);
        }

        /**
        * Process a validated event from a peer that is ready to happen on
        * the Update() thread.
        */
        protected virtual void ProcessReceivedEvent(JObject message)
        {
        }

        protected void AddEventToHash(JObject message)
        {
            if (GetType(message) == SET_STATE_EVENT)
            {
                Hash = message["hash"]!.ToObject<int>();
            }
            else
            {
                AddToHash(message.ToString());
            }
            Log($"Event: {GetType(message)}");
        }

        protected void SendLength(ISocketStream stream, int length)
        {
            byte[] buffer = BitConverter.GetBytes(length);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(buffer);
            stream.Write(buffer, 0, buffer.Length);
        }

        protected void SendEvent(ISocketStream client, JObject message)
        {
            Log($"Sending: {GetType(message)} for tick {GetTick(message)}");
            var start = System.Diagnostics.Stopwatch.GetTimestamp();
            byte[] buffer = MessageToBuffer(message);
            var end = System.Diagnostics.Stopwatch.GetTimestamp();
            AccumSerializeTime(start, end);

            try
            {
                // If this is a heartbeat event and very small, substitute with a binary heartbeat frame
                var msgType = GetType(message);
                if (msgType == HEARTBEAT_EVENT || msgType == "H")
                {
                    // Binary frame layout (uncompressed, no gzip):
                    // 4-byte length header (network order) where length is frameSize | 0x80000000 to mark raw frame (negative when int32)
                    // 1 byte frame type
                    // varint tick delta (ticksSinceLoad - TickCount) but since TickCount may be same we send absolute tick as delta from 0 for now
                    int tick = GetTick(message);
                    // For simplicity first impl encodes absolute tick (could be delta to last sent tick later)
                    Span<byte> payload = stackalloc byte[1 + 5];
                    int p = 0;
                    payload[p++] = FRAME_HEARTBEAT_V1;
                    p += WriteVarUInt32((uint)tick, payload.Slice(p));
                    int frameSize = p; // bytes after header
                    int headerValue = frameSize | unchecked((int)0x80000000); // high bit set marks raw frame (no compression)
                    byte[] outBuf = new byte[HEADER_SIZE + frameSize];
                    byte[] headerBytes = BitConverter.GetBytes(headerValue);
                    if (BitConverter.IsLittleEndian) Array.Reverse(headerBytes);
                    Array.Copy(headerBytes, 0, outBuf, 0, HEADER_SIZE);
                    for (int i = 0; i < frameSize; i++) outBuf[HEADER_SIZE + i] = payload[i];
                    client.Write(outBuf, 0, outBuf.Length);
                    RegisterSend(outBuf.Length); // already includes header
                    TryCountEvent(_sentEventType, (msgType == "H" ? "H" : HEARTBEAT_EVENT) + "(bin)", outBuf.Length);
                }
                else
                {
                    SendLength(client, buffer.Length);
                    client.Write(buffer, 0, buffer.Length);
                    RegisterSend(buffer.Length + HEADER_SIZE); // include header
                    TryCountEvent(_sentEventType, msgType, buffer.Length + HEADER_SIZE);
                }
            } catch (Exception e)
            {
                Log($"Error sending event: {e.Message}");
            }
        }

        protected bool TryReadLength(ISocketStream stream, out int length)
        {
            byte[] headerBuffer;
            try
            {
                headerBuffer = stream.ReadUntilComplete(HEADER_SIZE);
            }
            catch
            {
                length = 0;
                return false;
            }
            if (BitConverter.IsLittleEndian)
                Array.Reverse(headerBuffer);
            length = BitConverter.ToInt32(headerBuffer, 0);
            return true;
        }

        protected void StartListening(ISocketStream client, bool isClient)
        {
            //Log("Client connected");
            int messageCount = 0;
            while (client.Connected && !IsStopped)
            {
                if (!TryReadLength(client, out int messageLength)) break;

                // First message is always the file
                if (messageCount == 0 && isClient)
                {
                    if (messageLength == 0)
                    {
                        ReadErrorMessage(client);
                        return;
                    }

                    ReceiveFile(client, messageLength);
                    messageCount++;
                    continue;
                }

                if (messageLength == 0)
                {
                    Log("Received message of length 0; aborting listen");
                    break;
                }

                //Log($"Starting to read {messageLength} bytes");
                // TODO: How should this fail and not hang if map stops sending?
                bool rawFrame = (messageLength & unchecked((int)0x80000000)) != 0;
                int payloadLength = messageLength & 0x7FFFFFFF;
                byte[] buffer = client.ReadUntilComplete(payloadLength);
                if (rawFrame)
                {
                    // Interpret raw binary frame
                    if (payloadLength == 0) { Log("Empty raw frame"); continue; }
                    byte frameType = buffer[0];
                    if (frameType == FRAME_HEARTBEAT_V1)
                    {
                        // Parse varint absolute tick
                        ReadVarUInt32(buffer, 1, out uint absTick);
                        // Reconstruct minimal JSON equivalent for existing pipeline
                        var hb = new JObject();
                        hb[TICKS_KEY] = (int)absTick;
                        hb[TYPE_KEY] = HEARTBEAT_EVENT; // full name so higher layers still filter
                        string json = hb.ToString(Newtonsoft.Json.Formatting.None);
                        receivedEventQueue.Enqueue(json);
                        RegisterRecv(payloadLength + HEADER_SIZE);
                        TryCountEvent(_recvEventType, HEARTBEAT_EVENT + "(bin)", payloadLength + HEADER_SIZE);
                    }
                    else
                    {
                        Log($"Unknown frame type {frameType}; skipping");
                    }
                }
                else
                {
                    string message = BufferToStringMessage(buffer);
                    receivedEventQueue.Enqueue(message);
                    RegisterRecv(payloadLength + HEADER_SIZE);
                    try
                    {
                        var obj = Newtonsoft.Json.Linq.JObject.Parse(message);
                        TryCountEvent(_recvEventType, GetType(obj), payloadLength + HEADER_SIZE);
                    }
                    catch { }
                }
                messageCount++;
            }
        }

        protected byte[] MessageToBuffer(JObject message)
        {
            string json = message.ToString(Newtonsoft.Json.Formatting.None);
            return MessageToBuffer(json);
        }

        protected byte[] MessageToBuffer(string message)
        {
            return CompressionUtils.Compress(message);
        }

        protected string BufferToStringMessage(byte[] buffer)
        {
            return CompressionUtils.Decompress(buffer);
        }

        private void ReadErrorMessage(ISocketStream stream)
        {
            if (TryReadLength(stream, out int length))
            {
                byte[] bytes = stream.ReadUntilComplete(length);
                string message = BufferToStringMessage(bytes);
                if (OnError != null)
                {
                    OnError(message);
                }
            }
        }

        public static int CombineHash(int h1, int h2)
        {
            return h1 * 31 + h2;
        }

        private void AddToHash(string str)
        {
            AddToHash(Encoding.UTF8.GetBytes(str));
        }

        private void AddToHash(byte[] bytes)
        {
            Hash = CombineHash(Hash, GetHashCode(bytes));
        }

        public static int GetHashCode(byte[] bytes)
        {
            int code = 0;
            foreach (byte b in bytes)
            {
                code = CombineHash(code, b);
            }
            return code;
        }

        private void AddFileToHash(byte[] bytes)
        {
            AddToHash(bytes);
        }

        private void ReceiveFile(ISocketStream stream, int messageLength)
        {
            byte[] mapBytes = stream.ReadUntilComplete(messageLength);
            AddFileToHash(mapBytes);
            Log($"Received map with length {mapBytes.Length} and Hash: {GetHashCode(mapBytes).ToString("X8")}");
            this.mapBytes = mapBytes;
        }

        private void ProcessReceivedEventsQueue()
        {
            while (receivedEventQueue.TryDequeue(out string? message))
            {
                try
                {
                    ReceiveEvent(JObject.Parse(message));
                } catch (Exception e)
                {
                    Log($"Error receiving event: {e.Message}");
                }
            }
        }

        /**
         * Called when an event is received from a connected Net
         * and ready to be added to the queue for processing.
         */
        protected virtual void ReceiveEvent(JObject message)
        {
            InsertInScript(message, receivedEvents);
        }

        private void ProcessLogs()
        {
            while (logQueue.TryDequeue(out string? log))
            {
                OnLog?.Invoke(log);
            }
        }

        private void ProcessReceivedMap()
        {
            if (mapBytes == null) return;
            OnMapReceived?.Invoke(mapBytes);
            mapBytes = null;
        }

        /**
         * Updates, processing queued logs, maps and events.
         */
        public void Update()
        {
            ProcessLogs();
            if (!Started) return;
            ProcessReceivedMap();
            ProcessReceivedEventsQueue();
            MaybeLogMetrics();

        }

        private List<JObject> FilterEvents(List<JObject> events)
        {
            return events.Where(ShouldReadEvent).ToList();
        }

        private bool ShouldReadEvent(JObject message)
        {
            string type = GetType(message);
            return !(type == SET_STATE_EVENT || type == HEARTBEAT_EVENT);
        }

        /**
         * Reads received events that should be processed by the game
         * and deletes and returns.
         * Will call update before processing events.
         */
        public virtual List<JObject> ReadEvents(int ticksSinceLoad)
        {
            //if (ticksSinceLoad != TickCount) Log($"Setting ticks from {TickCount} to {ticksSinceLoad}");
            TickCount = ticksSinceLoad;
            Update();
            List<JObject> toProcess = PopEventsToProcess(receivedEvents);
            toProcess.ForEach(e => ProcessReceivedEvent(e));
            return FilterEvents(toProcess);
        }

        // --- Instrumentation helpers ---
        private void RegisterSend(int bytes)
        {
            _totalBytesSent += bytes;
            _totalPacketsSent++;
            _windowBytesSent += bytes;
            _windowPacketsSent++;
        }

        private void RegisterRecv(int bytes)
        {
            _totalBytesRecv += bytes;
            _totalPacketsRecv++;
            _windowBytesRecv += bytes;
            _windowPacketsRecv++;
        }

        private void AccumSerializeTime(long startTimestamp, long endTimestamp)
        {
            // Stopwatch timestamp to microseconds
            long freq = System.Diagnostics.Stopwatch.Frequency;
            long deltaTicks = endTimestamp - startTimestamp;
            long micros = (long)(deltaTicks * 1_000_000L / freq);
            _serializeMicrosAccum += micros;
            _windowSerializeMicros += micros;
            _framesMeasured++;
        }

        private void MaybeLogMetrics()
        {
            if (TickCount - _lastMetricsLogTick < MetricsLogIntervalTicks) return;
            _lastMetricsLogTick = TickCount;
            if (MetricsLogIntervalTicks <= 0) return;
            double avgPktSizeSent = _windowPacketsSent == 0 ? 0 : (double)_windowBytesSent / _windowPacketsSent;
            double avgPktSizeRecv = _windowPacketsRecv == 0 ? 0 : (double)_windowBytesRecv / _windowPacketsRecv;
            double avgSerializeMicros = _framesMeasured == 0 ? 0 : (double)_windowSerializeMicros / _framesMeasured;
            Log($"NET METRICS window={MetricsLogIntervalTicks} ticks | sent={_windowBytesSent}B/{_windowPacketsSent}pkts (avg {avgPktSizeSent:F1} B) | recv={_windowBytesRecv}B/{_windowPacketsRecv}pkts (avg {avgPktSizeRecv:F1} B) | serializeAvg={avgSerializeMicros:F1} us | totals sent={_totalBytesSent}B recv={_totalBytesRecv}B");
            Log(TopEventsSummary());
            // Explicit cumulative totals line for easier external parsing
            double totalSerializePerPkt = _totalPacketsSent == 0 ? 0 : (double)_serializeMicrosAccum / _totalPacketsSent;
            Log($"NET TOTALS sent={_totalBytesSent}B/{_totalPacketsSent}pkts recv={_totalBytesRecv}B/{_totalPacketsRecv}pkts serializeAvgPerSentPkt={totalSerializePerPkt:F1}us");
            _windowBytesSent = _windowPacketsSent = 0;
            _windowBytesRecv = _windowPacketsRecv = 0;
            _windowSerializeMicros = 0;
            _framesMeasured = 0;
        }

        private static void TryCountEvent(Dictionary<string,(long count,long bytes)> dict, string type, int bytes)
        {
            if (string.IsNullOrEmpty(type)) return;
            if (dict.TryGetValue(type, out var v))
                dict[type] = (v.count + 1, v.bytes + bytes);
            else
                dict[type] = (1, bytes);
        }

        private string TopEventsSummary(int topN = 5)
        {
            string Format(Dictionary<string,(long count,long bytes)> d) => string.Join(", ", d.OrderByDescending(kv => kv.Value.bytes).Take(topN)
                .Select(kv => $"{kv.Key}:{kv.Value.count}c/{kv.Value.bytes}B"));
            return $"EVENT TYPES sent[{Format(_sentEventType)}] recv[{Format(_recvEventType)}]";
        }

        public bool HasEventsForTick(int tickSinceLoad)
        {
            Update();
            return receivedEvents.Any(e => GetTick(e) == tickSinceLoad);
        }
    }
}
