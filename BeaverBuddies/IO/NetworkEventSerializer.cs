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
            var obj = JObject.FromObject(e, serializer);
            obj["type"] = TypeToName[e.GetType()];
            obj["ticksSinceLoad"] = e.ticksSinceLoad; // ensure presence
            return obj;
        }

        public static ReplayEvent Deserialize(JObject obj)
        {
            var typeToken = obj["type"];
            if (typeToken == null) return null;
            string name = typeToken.Value<string>();
            if (!NameToType.TryGetValue(name, out var t)) return null;
            var instance = (ReplayEvent)Activator.CreateInstance(t);
            using (var reader = obj.CreateReader())
            {
                JsonSerializer.Create(Settings).Populate(reader, instance);
            }
            return instance;
        }
    }
}
