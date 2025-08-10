using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using BeaverBuddies.Events;

namespace BeaverBuddies.IO
{
    /// <summary>
    /// Lightweight network serializer for ReplayEvent.
    /// Eliminates TypeNameHandling.All and emits a compact envelope:
    /// {"type":"ConcreteEventName","ticksSinceLoad":N, ...fields...}
    /// </summary>
    internal static class NetworkEventSerializer
    {
        private static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.None,
            Formatting = Formatting.None,
        };

        private static readonly Dictionary<string, Type> NameToType;
        private static readonly Dictionary<Type, string> TypeToName;

        static NetworkEventSerializer()
        {
            var baseType = typeof(ReplayEvent);
            var types = baseType.Assembly.GetTypes()
                .Where(t => !t.IsAbstract && baseType.IsAssignableFrom(t))
                .ToList();
            NameToType = new Dictionary<string, Type>();
            TypeToName = new Dictionary<Type, string>();
            foreach (var t in types)
            {
                string name = t.Name;
                if (!NameToType.ContainsKey(name))
                {
                    NameToType.Add(name, t);
                }
                TypeToName[t] = name;
            }
        }

        public static JObject Serialize(ReplayEvent e)
        {
            var serializer = JsonSerializer.Create(Settings);
            if (e is BeaverBuddies.GroupedEvent ge)
            {
                // Build root manually so we can inject type into each inner event
                var root = new JObject
                {
                    ["type"] = TypeToName[e.GetType()],
                    ["ticksSinceLoad"] = e.ticksSinceLoad
                };
                var arr = new JArray();
                foreach (var inner in ge.events)
                {
                    var child = JObject.FromObject(inner, serializer);
                    child["type"] = TypeToName[inner.GetType()];
                    child["ticksSinceLoad"] = inner.ticksSinceLoad;
                    arr.Add(child);
                }
                root["events"] = arr;
                return root;
            }
            else
            {
                var obj = JObject.FromObject(e, serializer);
                obj["type"] = TypeToName[e.GetType()];
                obj["ticksSinceLoad"] = e.ticksSinceLoad; // ensure presence
                return obj;
            }
        }

        public static ReplayEvent Deserialize(JObject obj)
        {
            var typeToken = obj["type"];
            if (typeToken == null) return null;
            string name = typeToken.Value<string>();
            if (!NameToType.TryGetValue(name, out var t)) return null;
            if (name == nameof(BeaverBuddies.GroupedEvent))
            {
                // Custom manual reconstruction
                var grouped = (BeaverBuddies.GroupedEvent)Activator.CreateInstance(t);
                grouped.ticksSinceLoad = obj["ticksSinceLoad"]?.Value<int>() ?? 0;
                var innerArr = obj["events"] as JArray;
                if (innerArr != null)
                {
                    foreach (var token in innerArr)
                    {
                        if (token is JObject child)
                        {
                            var inner = Deserialize(child);
                            if (inner != null)
                            {
                                grouped.events.Add(inner);
                            }
                        }
                    }
                }
                return grouped;
            }
            else
            {
                var instance = (ReplayEvent)Activator.CreateInstance(t);
                using (var reader = obj.CreateReader())
                {
                    JsonSerializer.Create(Settings).Populate(reader, instance);
                }
                return instance;
            }
        }
    }
}
