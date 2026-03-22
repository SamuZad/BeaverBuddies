using HarmonyLib;
using System;
using System.Reflection;

namespace BeaverBuddies.ModCompatibility
{
    /// <summary>
    /// Handles deterministic faction assignment for the BeaverFactions mod (Bobingabout).
    /// Patches Initialize to use RNG-based faction selection instead of texture matching,
    /// ensuring server and client assign the same faction to newborn beavers.
    /// </summary>
    public static class BeaverFactionsPatcher
    {
        private static Type _beaverFactionType;
        private static Type _beaverFactionSpecServiceType;
        private static MethodInfo _randomFactionMethod;
        private static bool _initialized;
        private static bool _isAvailable;

        public static bool IsAvailable => _isAvailable;

        /// <summary>
        /// Initialize the patcher by finding the BeaverFactions types via reflection.
        /// </summary>
        public static void Initialize(Harmony harmony)
        {
            if (_initialized) return;
            _initialized = true;

            try
            {
                _beaverFactionType = FindType("Bobingabout.BeaverFactions.BeaverFaction");
                if (_beaverFactionType == null)
                {
                    Plugin.Log("BeaverFactions mod not found, skipping patches");
                    return;
                }

                _beaverFactionSpecServiceType = FindType("Bobingabout.BeaverFactions.BeaverFactionSpecService");
                if (_beaverFactionSpecServiceType == null)
                {
                    Plugin.LogWarning("BeaverFactionSpecService not found");
                    return;
                }

                _randomFactionMethod = _beaverFactionSpecServiceType.GetMethod("RandomFaction",
                    BindingFlags.Public | BindingFlags.Instance);
                if (_randomFactionMethod == null)
                {
                    Plugin.LogWarning("RandomFaction method not found");
                    return;
                }

                // Find the parameterless Initialize method
                MethodInfo initializeMethod = null;
                foreach (var method in _beaverFactionType.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
                {
                    if (method.Name == "Initialize" && method.GetParameters().Length == 0)
                    {
                        initializeMethod = method;
                        break;
                    }
                }

                if (initializeMethod == null)
                {
                    Plugin.LogWarning("BeaverFaction.Initialize not found");
                    return;
                }

                // Patch Initialize
                var prefix = typeof(BeaverFactionsPatcher).GetMethod(nameof(InitializePrefix), BindingFlags.Static | BindingFlags.NonPublic);
                var postfix = typeof(BeaverFactionsPatcher).GetMethod(nameof(InitializePostfix), BindingFlags.Static | BindingFlags.NonPublic);
                harmony.Patch(initializeMethod, new HarmonyMethod(prefix), new HarmonyMethod(postfix));

                _isAvailable = true;
                Plugin.Log("BeaverFactions determinism patch applied");
            }
            catch (Exception ex)
            {
                Plugin.LogError($"Failed to initialize BeaverFactions patcher: {ex}");
            }
        }

        private static Type FindType(string fullTypeName)
        {
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                try
                {
                    var type = assembly.GetType(fullTypeName);
                    if (type != null) return type;
                }
                catch { }
            }
            return null;
        }

        /// <summary>
        /// For new beavers, force RandomFaction with determinism enabled.
        /// The original Initialize uses texture matching which can desync.
        /// </summary>
        private static bool InitializePrefix(object __instance)
        {
            // Check if faction is already set (loaded from save)
            var getFactionMethod = _beaverFactionType.GetMethod("GetFaction", BindingFlags.Public | BindingFlags.Instance);
            string existingFaction = getFactionMethod?.Invoke(__instance, null) as string;

            if (!string.IsNullOrEmpty(existingFaction))
            {
                // Beaver loaded from save - let original run with determinism
                DeterminismService.SetGamePatcherActive(typeof(BeaverFactionsPatcher), true);
                return true;
            }

            // New beaver - force RandomFaction instead of texture matching
            DeterminismService.SetGamePatcherActive(typeof(BeaverFactionsPatcher), true);
            try
            {
                var specServiceField = _beaverFactionType.GetField("_beaverFactionSpecService", BindingFlags.NonPublic | BindingFlags.Instance);
                var specService = specServiceField?.GetValue(__instance);
                if (specService == null) return true;

                string faction = _randomFactionMethod.Invoke(specService, null) as string;
                if (string.IsNullOrEmpty(faction)) return true;

                // Set all required fields directly (bypass SetFaction's guard)
                var myFactionIdField = _beaverFactionType.GetField("_myFactionId", BindingFlags.NonPublic | BindingFlags.Instance);
                var initializedField = _beaverFactionType.GetField("_initialized", BindingFlags.NonPublic | BindingFlags.Instance);
                var factionSpecField = _beaverFactionType.GetField("_beaverFactionSpec", BindingFlags.NonPublic | BindingFlags.Instance);
                var displayNameField = _beaverFactionType.GetField("_displayName", BindingFlags.NonPublic | BindingFlags.Instance);

                myFactionIdField?.SetValue(__instance, faction);
                initializedField?.SetValue(__instance, true);

                // Set faction spec and display name
                var getFactionSpecMethod = _beaverFactionSpecServiceType.GetMethod("GetFactionSpec", BindingFlags.Public | BindingFlags.Instance);
                var factionSpec = getFactionSpecMethod?.Invoke(specService, new object[] { faction });
                factionSpecField?.SetValue(__instance, factionSpec);

                if (factionSpec != null)
                {
                    var displayNameProp = factionSpec.GetType().GetProperty("DisplayName");
                    displayNameField?.SetValue(__instance, displayNameProp?.GetValue(factionSpec) as string ?? faction);
                }

                // Update texture
                var setTextureMethod = _beaverFactionType.GetMethod("SetTextureFromFactionId", BindingFlags.Public | BindingFlags.Instance);
                setTextureMethod?.Invoke(__instance, null);

                return false; // Skip original
            }
            finally
            {
                DeterminismService.SetGamePatcherActive(typeof(BeaverFactionsPatcher), false);
            }
        }

        private static void InitializePostfix()
        {
            DeterminismService.SetGamePatcherActive(typeof(BeaverFactionsPatcher), false);
        }
    }
}
