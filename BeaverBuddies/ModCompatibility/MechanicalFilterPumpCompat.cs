using BeaverBuddies.Events;
using HarmonyLib;
using System;
using System.Reflection;
using Timberborn.BaseComponentSystem;
using UnityEngine.UIElements;

namespace BeaverBuddies.ModCompatibility
{
    /// <summary>
    /// Handles sync for the MechanicalFilterPump mod (datvm).
    /// Patches the Fragment's OnActiveChanged to sync filter settings across clients,
    /// and UpdateFragment to sync the UI checkbox from component state.
    /// </summary>
    public static class MechanicalFilterPumpPatcher
    {
        private const string ComponentTypeName = "MechanicalFilterPump.Components.MechanicalFilterPumpComponent";
        private const string FragmentTypeName = "MechanicalFilterPump.UI.MechanicalFilterPumpFragment";

        private static Type _componentType;
        private static Type _fragmentType;
        private static FieldInfo _compField;
        private static FieldInfo _chkActiveField;
        private static MethodInfo _setActiveMethod;
        private static PropertyInfo _isActiveProp;
        private static bool _initialized;
        private static bool _isAvailable;

        public static bool IsAvailable => _isAvailable;

        public static void Initialize(Harmony harmony)
        {
            if (_initialized) return;
            _initialized = true;

            try
            {
                _componentType = AccessTools.TypeByName(ComponentTypeName);
                _fragmentType = AccessTools.TypeByName(FragmentTypeName);

                if (_componentType == null || _fragmentType == null)
                {
                    Plugin.Log("MechanicalFilterPump mod not found, skipping patches");
                    return;
                }

                _compField = AccessTools.Field(_fragmentType, "comp");
                _chkActiveField = AccessTools.Field(_fragmentType, "chkActive");
                _setActiveMethod = AccessTools.Method(_componentType, "SetActive");
                _isActiveProp = AccessTools.Property(_componentType, "IsActive");

                if (_compField == null || _chkActiveField == null || _setActiveMethod == null || _isActiveProp == null)
                {
                    Plugin.LogWarning("MechanicalFilterPump: Could not find required fields/methods");
                    return;
                }

                // Patch Fragment's OnActiveChanged (private method) - intercepts checkbox clicks
                var onActiveChangedMethod = _fragmentType.GetMethod("OnActiveChanged", 
                    BindingFlags.Instance | BindingFlags.NonPublic);
                if (onActiveChangedMethod != null)
                {
                    harmony.Patch(onActiveChangedMethod,
                        prefix: new HarmonyMethod(AccessTools.Method(typeof(MechanicalFilterPumpPatcher), nameof(OnActiveChangedPrefix))));
                }
                else
                {
                    Plugin.LogWarning("MechanicalFilterPump: OnActiveChanged method not found");
                }

                // Patch UpdateFragment to sync checkbox from component state
                var updateMethod = AccessTools.Method(_fragmentType, "UpdateFragment");
                if (updateMethod != null)
                {
                    harmony.Patch(updateMethod,
                        postfix: new HarmonyMethod(AccessTools.Method(typeof(MechanicalFilterPumpPatcher), nameof(UpdateFragmentPostfix))));
                }

                _isAvailable = true;
                Plugin.Log("MechanicalFilterPump sync patches applied");
            }
            catch (Exception ex)
            {
                Plugin.LogError($"Failed to initialize MechanicalFilterPump patcher: {ex}");
            }
        }

        internal static object GetComponent(BaseComponent entity)
        {
            if (_componentType == null) return null;
            return AccessTools.Method(typeof(BaseComponent), "GetComponent", new Type[0])
                ?.MakeGenericMethod(_componentType)
                ?.Invoke(entity, null);
        }

        internal static bool GetIsActive(object component) => (bool)_isActiveProp.GetValue(component);
        internal static void CallSetActive(object component, bool value) => _setActiveMethod.Invoke(component, new object[] { value });

        /// <summary>
        /// Prefix for Fragment.OnActiveChanged - intercepts checkbox clicks.
        /// </summary>
        private static bool OnActiveChangedPrefix(object __instance, bool active)
        {
            var comp = _compField.GetValue(__instance);
            if (comp == null) return true;

            // Don't create event if value hasn't changed
            if (GetIsActive(comp) == active) return true;

            return ReplayEvent.DoEntityPrefix((BaseComponent)comp, entityID => new MechanicalFilterPumpActiveChangedEvent
            {
                entityID = entityID,
                isActive = active,
            });
        }

        /// <summary>
        /// Postfix for UpdateFragment - syncs checkbox from component state.
        /// The original UpdateFragment is empty, so this implements the sync.
        /// </summary>
        private static void UpdateFragmentPostfix(object __instance)
        {
            var comp = _compField.GetValue(__instance);
            if (comp == null) return;

            var chkActive = _chkActiveField.GetValue(__instance) as Toggle;
            if (chkActive == null) return;

            var isActive = GetIsActive(comp);
            if (chkActive.value != isActive)
            {
                chkActive.SetValueWithoutNotify(isActive);
            }
        }
    }

    [Serializable]
    class MechanicalFilterPumpActiveChangedEvent : ReplayEvent
    {
        public string entityID;
        public bool isActive;

        public override void Replay(IReplayContext context)
        {
            var entity = GetEntityComponent(context, entityID);
            if (entity == null) return;

            var component = MechanicalFilterPumpPatcher.GetComponent(entity);
            if (component == null) return;

            MechanicalFilterPumpPatcher.CallSetActive(component, isActive);
        }

        public override string ToActionString() => $"Setting mechanical filter pump {entityID} active to: {isActive}";
    }
}
