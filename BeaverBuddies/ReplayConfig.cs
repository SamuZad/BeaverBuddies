﻿using Newtonsoft.Json;
using System;
using System.IO;
using Timberborn.Modding;

namespace BeaverBuddies
{
    public enum NetMode { Manual, AutoconnectClient, Record, Replay, None }

    public class ReplayConfig
    {
        public const string MODE_NONE = "none";
        public const string MODE_MANUAL = "manual";
        public const string MODE_AUTOCONNECT = "autoconnect";

        // Not implemented fully yet
        public const string MODE_RECORD = "record";
        public const string MODE_REPLAY = "replay";


        public string Mode { get; set; } = MODE_MANUAL;
        public string ClientConnectionAddress { get; set; } = "127.0.0.1";
        public int Port { get; set; } = 25565;
        public bool Verbose = true;
        public bool FirstTimer = true;

        public static bool TemporarilyDebug = false;
        public bool AlwaysDebug = false;

        [JsonIgnore]
        public bool Debug => AlwaysDebug || TemporarilyDebug;

        public bool ReportingConsent = false;

        public NetMode GetNetMode()
        {
            switch (Mode.ToLower())
            {
                case MODE_MANUAL:
                    return NetMode.Manual;
                case MODE_AUTOCONNECT:
                    return NetMode.AutoconnectClient;
                case MODE_NONE:
                    return NetMode.None;
                case MODE_RECORD:
                    return NetMode.Record;
                case MODE_REPLAY:
                    return NetMode.Replay;
                default:
                    Plugin.LogWarning("Unknown netowrking mode: " + Mode);
                    return NetMode.None;
            }
        }
    }
}
