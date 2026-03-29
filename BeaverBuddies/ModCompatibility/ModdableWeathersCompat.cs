using BeaverBuddies.Events;
using BeaverBuddies.IO;
using HarmonyLib;
using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using UnityEngine.UIElements;

namespace BeaverBuddies.ModCompatibility
{
    /// <summary>
    /// Handles real-time sync for the ModdableWeathers mod (datvm).
    /// Individual setting changes sync immediately via WeatherSettingsChangedEvent.
    /// Weather regeneration syncs on dialog close via WeatherRegenerateEvent.
    /// </summary>
    public static class ModdableWeathersPatcher
    {
        private const string WeatherHistoryRegistryTypeName = "ModdableWeathers.Historical.WeatherHistoryRegistry";
        private const string WeatherGeneratorTypeName = "ModdableWeathers.Cycles.WeatherGenerator";
        private const string WeatherSettingsServiceTypeName = "ModdableWeathers.Weathers.Settings.ModdableWeatherSettingsService";
        private const string WeatherModifierSettingsServiceTypeName = "ModdableWeathers.WeatherModifiers.Settings.ModdableWeatherModifierSettingsService";
        private const string WeatherCycleStageDefinitionServiceTypeName = "ModdableWeathers.Historical.WeatherCycleStageDefinitionService";
        private const string SettingElementTypeName = "ModdableWeathers.UI.Settings.SettingElement";
        private const string GeneralWeatherSettingsTypeName = "ModdableWeathers.Services.GeneralWeatherSettings";
        private const string BaseWeatherSettingsInterfaceName = "ModdableWeathers.Common.Settings.IBaseWeatherSettings";
        private const string WeatherCycleStagesPanelTypeName = "ModdableWeathers.UI.Settings.WeatherCycleStagesPanel";
        private const string WeatherSettingsDialogTypeName = "ModdableWeathers.UI.Settings.WeatherSettingsDialog";
        private const string GlobalSettingsPanelTypeName = "ModdableWeathers.UI.Settings.GlobalSettingsPanel";
        private const string WeatherSettingsExportPanelTypeName = "ModdableWeathers.UI.Settings.WeatherSettingsExportPanel";

        private static Type _historyRegistryType;
        private static Type _weatherGeneratorType;
        private static Type _weatherSettingsServiceType;
        private static Type _modifierSettingsServiceType;
        private static Type _stageDefinitionServiceType;
        private static Type _settingElementType;
        private static Type _generalWeatherSettingsType;
        private static Type _baseWeatherSettingsInterface;
        private static Type _cycleStagesPanelType;
        private static Type _weatherSettingsDialogType;
        private static Type _globalSettingsPanelType;
        private static Type _exportPanelType;

        private static MethodInfo _clearFutureEntriesMethod;
        private static MethodInfo _ensureWeatherGeneratedMethod;

        private static MethodInfo _serializeWeatherSettingsMethod;
        private static MethodInfo _loadWeatherSettingsMethod;
        private static MethodInfo _serializeModifierSettingsMethod;
        private static MethodInfo _loadModifierSettingsMethod;
        private static PropertyInfo _stageDefinitionsProperty;

        private static MethodInfo _baseSettingsSerializeMethod;
        private static MethodInfo _baseSettingsDeserializeMethod;
        private static MethodInfo _cycleStagesRefreshListMethod;

        private static PropertyInfo _settingElementPropertyProp;
        private static PropertyInfo _settingElementSettingsProp;

        private static bool _initialized;
        private static bool _isAvailable;

        // Regen flow state: set by ClearFutureEntries prefix, consumed by EnsureWeatherGenerated prefix
        private static bool _regenPending;
        private static bool _deterministicRngActive;

        public static bool IsAvailable => _isAvailable;

        public static void Initialize(Harmony harmony)
        {
            if (_initialized) return;
            _initialized = true;

            try
            {
                _historyRegistryType = AccessTools.TypeByName(WeatherHistoryRegistryTypeName);
                _weatherGeneratorType = AccessTools.TypeByName(WeatherGeneratorTypeName);
                _weatherSettingsServiceType = AccessTools.TypeByName(WeatherSettingsServiceTypeName);
                _modifierSettingsServiceType = AccessTools.TypeByName(WeatherModifierSettingsServiceTypeName);
                _stageDefinitionServiceType = AccessTools.TypeByName(WeatherCycleStageDefinitionServiceTypeName);
                _settingElementType = AccessTools.TypeByName(SettingElementTypeName);
                _generalWeatherSettingsType = AccessTools.TypeByName(GeneralWeatherSettingsTypeName);
                _baseWeatherSettingsInterface = AccessTools.TypeByName(BaseWeatherSettingsInterfaceName);
                _cycleStagesPanelType = AccessTools.TypeByName(WeatherCycleStagesPanelTypeName);
                _weatherSettingsDialogType = AccessTools.TypeByName(WeatherSettingsDialogTypeName);
                _globalSettingsPanelType = AccessTools.TypeByName(GlobalSettingsPanelTypeName);
                _exportPanelType = AccessTools.TypeByName(WeatherSettingsExportPanelTypeName);

                if (_settingElementType != null)
                {
                    _settingElementPropertyProp = AccessTools.Property(_settingElementType, "Property");
                    _settingElementSettingsProp = AccessTools.Property(_settingElementType, "Settings");
                }

                if (_historyRegistryType == null)
                {
                    Plugin.Log("ModdableWeathers mod not found, skipping patches");
                    return;
                }

                _clearFutureEntriesMethod = AccessTools.Method(_historyRegistryType, "ClearFutureEntries");
                if (_weatherGeneratorType != null)
                {
                    _ensureWeatherGeneratedMethod = AccessTools.Method(_weatherGeneratorType, "EnsureWeatherGenerated");
                }

                if (_weatherSettingsServiceType != null)
                {
                    _serializeWeatherSettingsMethod = AccessTools.Method(_weatherSettingsServiceType, "SerializeSettings");
                    _loadWeatherSettingsMethod = AccessTools.Method(_weatherSettingsServiceType, "LoadSerializedSettings", new[] { typeof(string) });
                }
                if (_modifierSettingsServiceType != null)
                {
                    _serializeModifierSettingsMethod = AccessTools.Method(_modifierSettingsServiceType, "SerializeSettings");
                    _loadModifierSettingsMethod = AccessTools.Method(_modifierSettingsServiceType, "LoadSerializedSettings", new[] { typeof(string) });
                }
                if (_stageDefinitionServiceType != null)
                {
                    _stageDefinitionsProperty = AccessTools.Property(_stageDefinitionServiceType, "StagesDefinitions");
                }
                if (_baseWeatherSettingsInterface != null)
                {
                    _baseSettingsSerializeMethod = AccessTools.Method(_baseWeatherSettingsInterface, "Serialize");
                    _baseSettingsDeserializeMethod = AccessTools.Method(_baseWeatherSettingsInterface, "Deserialize");
                }
                if (_cycleStagesPanelType != null)
                {
                    _cycleStagesRefreshListMethod = AccessTools.Method(_cycleStagesPanelType, "RefreshList", new Type[0]);
                }

                PatchSettingElementCallbacks(harmony);
                PatchStageDefinitionsSetter(harmony);
                PatchDialogInit(harmony);

                if (_clearFutureEntriesMethod != null)
                {
                    harmony.Patch(_clearFutureEntriesMethod,
                        prefix: new HarmonyMethod(AccessTools.Method(typeof(ModdableWeathersPatcher), nameof(ClearFutureEntriesPrefix))));
                }

                if (_ensureWeatherGeneratedMethod != null)
                {
                    harmony.Patch(_ensureWeatherGeneratedMethod,
                        prefix: new HarmonyMethod(AccessTools.Method(typeof(ModdableWeathersPatcher), nameof(EnsureWeatherGeneratedPrefix))),
                        postfix: new HarmonyMethod(AccessTools.Method(typeof(ModdableWeathersPatcher), nameof(EnsureWeatherGeneratedPostfix))));
                }

                _isAvailable = true;
                Plugin.Log("ModdableWeathers sync patches applied");
            }
            catch (Exception ex)
            {
                Plugin.LogError($"Failed to initialize ModdableWeathers patcher: {ex}");
            }
        }

        private static void PatchDialogInit(Harmony harmony)
        {
            if (_weatherSettingsDialogType == null) return;

            var initMethod = AccessTools.Method(_weatherSettingsDialogType, "Init");
            if (initMethod != null)
            {
                harmony.Patch(initMethod,
                    postfix: new HarmonyMethod(AccessTools.Method(typeof(ModdableWeathersPatcher), nameof(DialogInitPostfix))));
                Plugin.Log("ModdableWeathers: Patched WeatherSettingsDialog.Init");
            }
        }

        /// <summary>
        /// Hides GlobalSettingsPanel and WeatherSettingsExportPanel in multiplayer.
        /// These panels modify local-only state (PlayerPrefs, file export/import)
        /// that would diverge between players.
        /// </summary>
        private static void DialogInitPostfix(object __instance)
        {
            if (EventIO.IsNull) return;
            if (!(_globalSettingsPanelType != null || _exportPanelType != null)) return;

            var dialog = __instance as VisualElement;
            if (dialog == null) return;

            var content = dialog.Q<VisualElement>("Content");
            if (content == null) return;

            foreach (var child in content.Children())
            {
                var childType = child.GetType();
                if ((_globalSettingsPanelType != null && _globalSettingsPanelType.IsAssignableFrom(childType)) ||
                    (_exportPanelType != null && _exportPanelType.IsAssignableFrom(childType)))
                {
                    child.style.display = DisplayStyle.None;
                }
            }
        }

        private static void PatchSettingElementCallbacks(Harmony harmony)
        {
            if (_settingElementType == null)
            {
                Plugin.LogWarning("ModdableWeathers: SettingElement type not found, skipping real-time setting sync");
                return;
            }

            // Find compiler-generated callback methods on SettingElement.
            // These are the lambdas in AddIntField/AddFloatField/AddBoolField that
            // call Property.SetValue(Settings, v). They're instance methods with a
            // single parameter (int, float, or bool) and CompilerGeneratedAttribute.
            var postfix = new HarmonyMethod(AccessTools.Method(typeof(ModdableWeathersPatcher), nameof(OnSettingChanged)));
            int patchedCount = 0;

            foreach (var method in _settingElementType.GetMethods(BindingFlags.Instance | BindingFlags.NonPublic))
            {
                if (!method.IsDefined(typeof(CompilerGeneratedAttribute), false)) continue;

                var parameters = method.GetParameters();
                if (parameters.Length != 1) continue;

                var paramType = parameters[0].ParameterType;
                if (paramType != typeof(int) && paramType != typeof(float) && paramType != typeof(bool)) continue;

                try
                {
                    harmony.Patch(method, postfix: postfix);
                    patchedCount++;
                }
                catch (Exception ex)
                {
                    Plugin.LogWarning($"ModdableWeathers: Failed to patch {method.Name}: {ex.Message}");
                }
            }

            Plugin.Log($"ModdableWeathers: Patched {patchedCount} SettingElement callbacks");
        }

        private static void PatchStageDefinitionsSetter(Harmony harmony)
        {
            if (_stageDefinitionServiceType == null || _stageDefinitionsProperty == null)
            {
                Plugin.LogWarning("ModdableWeathers: StageDefinitionService not found, skipping stage sync");
                return;
            }

            var setter = _stageDefinitionsProperty.GetSetMethod();
            if (setter != null)
            {
                harmony.Patch(setter,
                    postfix: new HarmonyMethod(AccessTools.Method(typeof(ModdableWeathersPatcher), nameof(OnStagesDefinitionsChanged))));
                Plugin.Log("ModdableWeathers: Patched StagesDefinitions setter");
            }
            else Plugin.LogWarning("ModdableWeathers: Could not find StagesDefinitions setter");
        }

        internal static void RefreshAllSettingElements()
        {
            if (_settingElementType == null || _settingElementPropertyProp == null || _settingElementSettingsProp == null)
                return;

            var panels = UnityEngine.UIElements.UIElementsRuntimeUtility.GetSortedPlayerPanels();
            foreach (var panel in panels)
            {
                var root = panel.visualTree;
                if (root == null) continue;

                var elements = root.Query(className: null).Build();
                foreach (var ve in elements)
                {
                    if (!_settingElementType.IsInstanceOfType(ve)) continue;

                    try
                    {
                        var prop = _settingElementPropertyProp.GetValue(ve) as PropertyInfo;
                        var settings = _settingElementSettingsProp.GetValue(ve);
                        if (prop == null || settings == null) continue;

                        var value = prop.GetValue(settings);
                        var propType = prop.PropertyType;

                        if (propType == typeof(int))
                        {
                            var field = ve.Q<IntegerField>();
                            if (field != null) field.SetValueWithoutNotify((int)value);
                        }
                        else if (propType == typeof(float))
                        {
                            var field = ve.Q<FloatField>();
                            if (field != null) field.SetValueWithoutNotify((float)value);
                        }
                        else if (propType == typeof(bool))
                        {
                            var toggle = ve.Q<Toggle>();
                            if (toggle == null) continue;

                            bool boolVal = (bool)value;
                            if (toggle.value != boolVal)
                            {
                                // Use .value (not SetValueWithoutNotify) so the
                                // OnEnabledChanged callback chain fires, updating
                                // the "Disabled" / tags text on the panel.
                                toggle.value = boolVal;
                            }
                        }
                    }
                    catch { }
                }
            }
        }

        private static void OnSettingChanged()
        {
            SyncAllSettings();
        }

        private static void OnStagesDefinitionsChanged()
        {
            SyncAllSettings();
        }

        private static void SyncAllSettings()
        {
            if (ReplayService.IsReplayingEvents) return;

            var replayService = ReplayEvent.GetReplayServiceIfReady();
            if (replayService == null) return;

            try
            {
                replayService.RecordEvent(new WeatherSettingsChangedEvent
                {
                    weatherSettingsJson = SerializeWeatherSettings(replayService),
                    modifierSettingsJson = SerializeModifierSettings(replayService),
                    stageDefinitionsJson = SerializeStageDefinitions(replayService),
                    generalSettingsJson = SerializeGeneralSettings(replayService),
                });
            }
            catch (Exception ex)
            {
                Plugin.LogError($"ModdableWeathers: Error syncing settings: {ex}");
            }
        }

        private static bool ClearFutureEntriesPrefix(object __instance, int currCycle)
        {
            if (ReplayService.IsReplayingEvents) return true;

            var replayService = ReplayEvent.GetReplayServiceIfReady();
            if (replayService == null) return true;

            _regenPending = true;

            return ReplayEvent.DoPrefix(() =>
            {
                return new WeatherRegenerateEvent
                {
                    currentCycle = currCycle,
                    weatherSettingsJson = SerializeWeatherSettings(replayService),
                    modifierSettingsJson = SerializeModifierSettings(replayService),
                    stageDefinitionsJson = SerializeStageDefinitions(replayService),
                    generalSettingsJson = SerializeGeneralSettings(replayService),
                };
            });
        }

        private static bool EnsureWeatherGeneratedPrefix(int cycle)
        {
            if (!_regenPending) return true;
            _regenPending = false;

            if (!EventIO.ShouldPlayPatchedEvents)
            {
                // Client: skip - the event replay will handle it
                return false;
            }

            DeterminismService.SetGamePatcherActive(typeof(ModdableWeathersPatcher), true);
            _deterministicRngActive = true;
            return true;
        }

        private static void EnsureWeatherGeneratedPostfix()
        {
            if (_deterministicRngActive)
            {
                _deterministicRngActive = false;
                DeterminismService.SetGamePatcherActive(typeof(ModdableWeathersPatcher), false);
            }
        }

        internal static string SerializeGeneralSettings(IReplayContext context)
        {
            if (_generalWeatherSettingsType == null || _baseSettingsSerializeMethod == null) return null;
            try
            {
                var service = GetSingleton(context, _generalWeatherSettingsType);
                if (service == null) return null;
                return _baseSettingsSerializeMethod.Invoke(service, null)?.ToString();
            }
            catch (Exception ex)
            {
                Plugin.LogWarning($"ModdableWeathers: Failed to serialize general settings: {ex.Message}");
                return null;
            }
        }

        internal static void LoadGeneralSettings(IReplayContext context, string json)
        {
            if (_generalWeatherSettingsType == null || _baseSettingsDeserializeMethod == null || string.IsNullOrEmpty(json)) return;
            try
            {
                var service = GetSingleton(context, _generalWeatherSettingsType);
                if (service == null) return;
                var jObject = Newtonsoft.Json.Linq.JObject.Parse(json);
                _baseSettingsDeserializeMethod.Invoke(service, new object[] { jObject });
            }
            catch (Exception ex)
            {
                Plugin.LogWarning($"ModdableWeathers: Failed to load general settings: {ex.Message}");
            }
        }

        internal static string SerializeWeatherSettings(IReplayContext context)
        {
            if (_weatherSettingsServiceType == null || _serializeWeatherSettingsMethod == null) return null;
            try
            {
                var service = GetSingleton(context, _weatherSettingsServiceType);
                if (service == null) return null;
                return _serializeWeatherSettingsMethod.Invoke(service, null)?.ToString();
            }
            catch (Exception ex)
            {
                Plugin.LogWarning($"ModdableWeathers: Failed to serialize weather settings: {ex.Message}");
                return null;
            }
        }

        internal static string SerializeModifierSettings(IReplayContext context)
        {
            if (_modifierSettingsServiceType == null || _serializeModifierSettingsMethod == null) return null;
            try
            {
                var service = GetSingleton(context, _modifierSettingsServiceType);
                if (service == null) return null;
                return _serializeModifierSettingsMethod.Invoke(service, null)?.ToString();
            }
            catch (Exception ex)
            {
                Plugin.LogWarning($"ModdableWeathers: Failed to serialize modifier settings: {ex.Message}");
                return null;
            }
        }

        internal static string SerializeStageDefinitions(IReplayContext context)
        {
            if (_stageDefinitionServiceType == null || _stageDefinitionsProperty == null) return null;
            try
            {
                var service = GetSingleton(context, _stageDefinitionServiceType);
                if (service == null) return null;
                var definitions = _stageDefinitionsProperty.GetValue(service);
                return Newtonsoft.Json.JsonConvert.SerializeObject(definitions);
            }
            catch (Exception ex)
            {
                Plugin.LogWarning($"ModdableWeathers: Failed to serialize stage definitions: {ex.Message}");
                return null;
            }
        }

        internal static void LoadWeatherSettings(IReplayContext context, string json)
        {
            if (_weatherSettingsServiceType == null || _loadWeatherSettingsMethod == null || string.IsNullOrEmpty(json)) return;
            try
            {
                var service = GetSingleton(context, _weatherSettingsServiceType);
                _loadWeatherSettingsMethod?.Invoke(service, new object[] { json });
            }
            catch (Exception ex)
            {
                Plugin.LogWarning($"ModdableWeathers: Failed to load weather settings: {ex.Message}");
            }
        }

        internal static void LoadModifierSettings(IReplayContext context, string json)
        {
            if (_modifierSettingsServiceType == null || _loadModifierSettingsMethod == null || string.IsNullOrEmpty(json)) return;
            try
            {
                var service = GetSingleton(context, _modifierSettingsServiceType);
                _loadModifierSettingsMethod?.Invoke(service, new object[] { json });
            }
            catch (Exception ex)
            {
                Plugin.LogWarning($"ModdableWeathers: Failed to load modifier settings: {ex.Message}");
            }
        }

        internal static void LoadStageDefinitions(IReplayContext context, string json)
        {
            if (_stageDefinitionServiceType == null || _stageDefinitionsProperty == null || string.IsNullOrEmpty(json)) return;
            try
            {
                var service = GetSingleton(context, _stageDefinitionServiceType);
                if (service == null) return;
                var definitionsType = _stageDefinitionsProperty.PropertyType;
                var definitions = Newtonsoft.Json.JsonConvert.DeserializeObject(json, definitionsType);
                _stageDefinitionsProperty.SetValue(service, definitions);
            }
            catch (Exception ex)
            {
                Plugin.LogWarning($"ModdableWeathers: Failed to load stage definitions: {ex.Message}");
            }
        }

        internal static void ClearFutureEntries(object historyRegistry, int currCycle)
        {
            _clearFutureEntriesMethod?.Invoke(historyRegistry, new object[] { currCycle });
        }

        internal static void EnsureWeatherGenerated(object generator, int cycle)
        {
            _ensureWeatherGeneratedMethod?.Invoke(generator, new object[] { cycle });
        }

        internal static void RefreshCycleStagesPanel()
        {
            if (_cycleStagesPanelType == null || _cycleStagesRefreshListMethod == null) return;

            var panels = UnityEngine.UIElements.UIElementsRuntimeUtility.GetSortedPlayerPanels();
            foreach (var panel in panels)
            {
                var root = panel.visualTree;
                if (root == null) continue;

                var elements = root.Query(className: null).Build();
                foreach (var ve in elements)
                {
                    if (_cycleStagesPanelType.IsInstanceOfType(ve))
                    {
                        try { _cycleStagesRefreshListMethod.Invoke(ve, null); }
                        catch { }
                    }
                }
            }
        }

        internal static object GetSingleton(IReplayContext context, Type type)
        {
            if (type == null) return null;
            var method = typeof(IReplayContext).GetMethod("GetSingleton")?.MakeGenericMethod(type);
            return method?.Invoke(context, null);
        }

        internal static object GetHistoryRegistry(IReplayContext context)
        {
            return GetSingleton(context, _historyRegistryType);
        }

        internal static object GetWeatherGenerator(IReplayContext context)
        {
            return GetSingleton(context, _weatherGeneratorType);
        }
    }

    /// <summary>
    /// Real-time settings sync. Sent on every individual setting change while
    /// the dialog is open. Loads settings without regenerating weather.
    /// </summary>
    [Serializable]
    class WeatherSettingsChangedEvent : ReplayEvent
    {
        public string weatherSettingsJson;
        public string modifierSettingsJson;
        public string stageDefinitionsJson;
        public string generalSettingsJson;

        public override void Replay(IReplayContext context)
        {
            ModdableWeathersPatcher.LoadWeatherSettings(context, weatherSettingsJson);
            ModdableWeathersPatcher.LoadModifierSettings(context, modifierSettingsJson);
            ModdableWeathersPatcher.LoadStageDefinitions(context, stageDefinitionsJson);
            ModdableWeathersPatcher.LoadGeneralSettings(context, generalSettingsJson);
            ModdableWeathersPatcher.RefreshAllSettingElements();
            ModdableWeathersPatcher.RefreshCycleStagesPanel();
        }

        public override string ToActionString() => "Syncing weather settings";
    }

    /// <summary>
    /// Weather regeneration sync. Sent when ClearFutureEntries is called (dialog close).
    /// Loads settings and regenerates weather with deterministic RNG.
    /// </summary>
    [Serializable]
    class WeatherRegenerateEvent : ReplayEvent
    {
        public int currentCycle;
        public string weatherSettingsJson;
        public string modifierSettingsJson;
        public string stageDefinitionsJson;
        public string generalSettingsJson;

        public override void Replay(IReplayContext context)
        {
            ModdableWeathersPatcher.LoadWeatherSettings(context, weatherSettingsJson);
            ModdableWeathersPatcher.LoadModifierSettings(context, modifierSettingsJson);
            ModdableWeathersPatcher.LoadStageDefinitions(context, stageDefinitionsJson);
            ModdableWeathersPatcher.LoadGeneralSettings(context, generalSettingsJson);

            var historyRegistry = ModdableWeathersPatcher.GetHistoryRegistry(context);
            var generator = ModdableWeathersPatcher.GetWeatherGenerator(context);

            if (historyRegistry == null || generator == null)
            {
                Plugin.LogWarning("WeatherRegenerateEvent: Could not get required singletons");
                return;
            }

            DeterminismService.SetGamePatcherActive(typeof(WeatherRegenerateEvent), true);
            try
            {
                ModdableWeathersPatcher.ClearFutureEntries(historyRegistry, currentCycle);
                ModdableWeathersPatcher.EnsureWeatherGenerated(generator, currentCycle);
            }
            finally
            {
                DeterminismService.SetGamePatcherActive(typeof(WeatherRegenerateEvent), false);
            }
        }

        public override string ToActionString() => $"Regenerating weather at cycle {currentCycle}";
    }
}
