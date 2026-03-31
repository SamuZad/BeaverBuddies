using BeaverBuddies.Events;
using BeaverBuddies.IO;
using HarmonyLib;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Timberborn.BaseComponentSystem;
using Timberborn.Coordinates;
using Timberborn.EntitySystem;
using Timberborn.Goods;
using Timberborn.InventorySystem;
using Timberborn.RecoveredGoodSystem;
using Timberborn.TimeSystem;
using UnityEngine;
using static BeaverBuddies.SingletonManager;

namespace BeaverBuddies.ModCompatibility
{
    /// <summary>
    /// Handles sync for the Riverborne mod.
    /// Patches RaftDock methods to sync raft dispatch configurations across clients:
    /// - AddRaftDispatch: Create new dispatch
    /// - RemoveRaftDispatch: Delete dispatch
    /// - ReplaceRaftDispatch: Edit existing dispatch
    /// - TogglePaused: Pause/unpause dispatch
    /// Also patches raft deletion to sync across clients.
    /// </summary>
    public static class RiverbornePatcher
    {
        private const string RaftDockTypeName = "Riverborne.Core.RaftDock";
        private const string RaftDispatchTypeName = "Riverborne.Core.RaftDispatch";
        private const string RaftTypeName = "Riverborne.Core.Raft";
        private const string RaftDispatcherTypeName = "Riverborne.Core.RaftDispatcher";
        private const string DeleteRaftFragmentTypeName = "Riverborne.CoreUI.DeleteRaftFragment";

        private static Type _raftDockType;
        private static Type _raftDispatchType;
        private static Type _raftType;
        private static Type _deleteRaftFragmentType;
        private static MethodInfo _addRaftDispatchMethod;
        private static MethodInfo _removeRaftDispatchMethod;
        private static MethodInfo _replaceRaftDispatchMethod;
        private static MethodInfo _togglePausedMethod;
        private static MethodInfo _updateLastDispatchTimeMethod;
        private static MethodInfo _deleteRaftMethod;
        private static FieldInfo _deleteFragmentRaftField;
        private static PropertyInfo _raftDispatchesProperty;
        private static PropertyInfo _nameProperty;
        private static PropertyInfo _cargoProperty;
        private static PropertyInfo _intervalProperty;
        private static PropertyInfo _lastDispatchTimeProperty;
        private static PropertyInfo _isPausedProperty;
        private static ConstructorInfo _raftDispatchConstructor;
        private static FieldInfo _raftDockField;
        private static FieldInfo _dispatchesField;
        private static FieldInfo _dispatchesListViewField;

        // For deterministic raft spawning
        private static Type _raftDispatcherType;
        private static FieldInfo _lastLaunchedRaftField;
        private static readonly Dictionary<object, int> _spawnCooldowns = new();

        // Track active fragment instances and their event handlers
        private static readonly Dictionary<object, (object raftDock, EventHandler handler)> _activeFragments = new();

        private static bool _initialized;
        private static bool _isAvailable;

        public static bool IsAvailable => _isAvailable;

        public static void Initialize(Harmony harmony)
        {
            if (_initialized) return;
            _initialized = true;

            try
            {
                _raftDockType = AccessTools.TypeByName(RaftDockTypeName);
                _raftDispatchType = AccessTools.TypeByName(RaftDispatchTypeName);

                if (_raftDockType == null || _raftDispatchType == null)
                {
                    Plugin.Log("Riverborne mod not found, skipping patches");
                    return;
                }

                // Get RaftDock methods
                _addRaftDispatchMethod = AccessTools.Method(_raftDockType, "AddRaftDispatch");
                _removeRaftDispatchMethod = AccessTools.Method(_raftDockType, "RemoveRaftDispatch");
                _replaceRaftDispatchMethod = AccessTools.Method(_raftDockType, "ReplaceRaftDispatch");
                _raftDispatchesProperty = AccessTools.Property(_raftDockType, "RaftDispatches");

                // Get RaftDispatch properties and methods
                _nameProperty = AccessTools.Property(_raftDispatchType, "Name");
                _cargoProperty = AccessTools.Property(_raftDispatchType, "Cargo");
                _intervalProperty = AccessTools.Property(_raftDispatchType, "Interval");
                _lastDispatchTimeProperty = AccessTools.Property(_raftDispatchType, "LastDispatchTime");
                _isPausedProperty = AccessTools.Property(_raftDispatchType, "IsPaused");
                _togglePausedMethod = AccessTools.Method(_raftDispatchType, "TogglePaused");
                _updateLastDispatchTimeMethod = AccessTools.Method(_raftDispatchType, "UpdateLastDispatchTime");

                // Get RaftDispatch constructor
                _raftDispatchConstructor = _raftDispatchType.GetConstructor(new Type[]
                {
                    typeof(string),                    // name
                    typeof(IEnumerable<GoodAmount>),   // cargo
                    typeof(float),                     // interval
                    typeof(float),                     // lastDispatchTime
                    typeof(bool)                       // isPaused
                });

                if (_addRaftDispatchMethod == null || _removeRaftDispatchMethod == null ||
                    _replaceRaftDispatchMethod == null || _togglePausedMethod == null ||
                    _raftDispatchConstructor == null)
                {
                    Plugin.LogWarning("Riverborne: Could not find required methods");
                    return;
                }

                // Patch AddRaftDispatch
                harmony.Patch(_addRaftDispatchMethod,
                    prefix: new HarmonyMethod(AccessTools.Method(typeof(RiverbornePatcher), nameof(AddRaftDispatchPrefix))));

                // Patch RemoveRaftDispatch
                harmony.Patch(_removeRaftDispatchMethod,
                    prefix: new HarmonyMethod(AccessTools.Method(typeof(RiverbornePatcher), nameof(RemoveRaftDispatchPrefix))));

                // Patch ReplaceRaftDispatch
                harmony.Patch(_replaceRaftDispatchMethod,
                    prefix: new HarmonyMethod(AccessTools.Method(typeof(RiverbornePatcher), nameof(ReplaceRaftDispatchPrefix))));

                // Patch the UI fragment to respond to dispatch changes
                var fragmentType = AccessTools.TypeByName("Riverborne.CoreUI.RaftDockFragment");
                if (fragmentType != null)
                {
                    _raftDockField = AccessTools.Field(fragmentType, "_raftDock");
                    _dispatchesField = AccessTools.Field(fragmentType, "_dispatches");
                    _dispatchesListViewField = AccessTools.Field(fragmentType, "_dispatchesListView");

                    // Patch ToggleDispatchPause
                    var toggleMethod = AccessTools.Method(fragmentType, "ToggleDispatchPause");
                    if (toggleMethod != null && _raftDockField != null)
                    {
                        harmony.Patch(toggleMethod,
                            prefix: new HarmonyMethod(AccessTools.Method(typeof(RiverbornePatcher), nameof(ToggleDispatchPausePrefix))));
                    }

                    // Patch ShowFragment to subscribe to RaftDispatchesChanged
                    var showMethod = AccessTools.Method(fragmentType, "ShowFragment");
                    if (showMethod != null)
                    {
                        harmony.Patch(showMethod,
                            postfix: new HarmonyMethod(AccessTools.Method(typeof(RiverbornePatcher), nameof(ShowFragmentPostfix))));
                    }

                    // Patch ClearFragment to unsubscribe
                    var clearMethod = AccessTools.Method(fragmentType, "ClearFragment");
                    if (clearMethod != null)
                    {
                        harmony.Patch(clearMethod,
                            prefix: new HarmonyMethod(AccessTools.Method(typeof(RiverbornePatcher), nameof(ClearFragmentPrefix))));
                    }
                }

                // Patch DeleteRaftFragment.DeleteRaft to sync raft deletion
                _raftType = AccessTools.TypeByName(RaftTypeName);
                _deleteRaftFragmentType = AccessTools.TypeByName(DeleteRaftFragmentTypeName);
                if (_deleteRaftFragmentType != null && _raftType != null)
                {
                    _deleteRaftMethod = AccessTools.Method(_deleteRaftFragmentType, "DeleteRaft");
                    _deleteFragmentRaftField = AccessTools.Field(_deleteRaftFragmentType, "_raft");
                    if (_deleteRaftMethod != null && _deleteFragmentRaftField != null)
                    {
                        harmony.Patch(_deleteRaftMethod,
                            prefix: new HarmonyMethod(AccessTools.Method(typeof(RiverbornePatcher), nameof(DeleteRaftPrefix))));
                    }
                }

                // Patch IsDropPointBlocked to use tick-based cooldown instead of physics position
                // This fixes desync caused by physics non-determinism affecting raft spawn timing
                _raftDispatcherType = AccessTools.TypeByName(RaftDispatcherTypeName);
                if (_raftDispatcherType != null)
                {
                    var isDropPointBlockedMethod = AccessTools.Method(_raftDispatcherType, "IsDropPointBlocked");
                    _lastLaunchedRaftField = AccessTools.Field(_raftDispatcherType, "_lastLaunchedRaft");
                    if (isDropPointBlockedMethod != null && _lastLaunchedRaftField != null)
                    {
                        harmony.Patch(isDropPointBlockedMethod,
                            prefix: new HarmonyMethod(AccessTools.Method(typeof(RiverbornePatcher), nameof(IsDropPointBlockedPrefix))));
                    }
                }

                _isAvailable = true;
                Plugin.Log("Riverborne sync patches applied");
            }
            catch (Exception ex)
            {
                Plugin.LogError($"Failed to initialize Riverborne patcher: {ex}");
            }
        }

        #region Dispatch Data Serialization

        internal static RaftDispatchData SerializeDispatch(object dispatch)
        {
            if (dispatch == null) return null;

            var cargo = _cargoProperty.GetValue(dispatch);
            var cargoList = new List<CargoItem>();

            // Cargo is ImmutableArray<GoodAmount> at source, iterable as GoodAmount
            if (cargo != null)
            {
                foreach (var goodAmount in (System.Collections.IEnumerable)cargo)
                {
                    var ga = (GoodAmount)goodAmount;
                    cargoList.Add(new CargoItem { goodId = ga.GoodId, amount = ga.Amount });
                }
            }

            return new RaftDispatchData
            {
                name = (string)_nameProperty.GetValue(dispatch),
                cargo = cargoList,
                interval = (float)_intervalProperty.GetValue(dispatch),
                lastDispatchTime = (float)_lastDispatchTimeProperty.GetValue(dispatch),
                isPaused = (bool)_isPausedProperty.GetValue(dispatch)
            };
        }

        internal static object DeserializeDispatch(RaftDispatchData data)
        {
            if (data == null) return null;

            var cargo = data.cargo.Select(c => new GoodAmount(c.goodId, c.amount));
            return _raftDispatchConstructor.Invoke(new object[]
            {
                data.name,
                cargo,
                data.interval,
                data.lastDispatchTime,
                data.isPaused
            });
        }

        internal static int GetDispatchIndex(object raftDock, object dispatch)
        {
            var dispatches = _raftDispatchesProperty.GetValue(raftDock);
            int index = 0;
            foreach (var d in (System.Collections.IEnumerable)dispatches)
            {
                if (ReferenceEquals(d, dispatch)) return index;
                index++;
            }
            return -1;
        }

        internal static object GetDispatchByIndex(object raftDock, int index)
        {
            var dispatches = _raftDispatchesProperty.GetValue(raftDock);
            int i = 0;
            foreach (var d in (System.Collections.IEnumerable)dispatches)
            {
                if (i == index) return d;
                i++;
            }
            return null;
        }

        internal static object GetRaftDockComponent(BaseComponent entity)
        {
            if (_raftDockType == null) return null;
            return AccessTools.Method(typeof(BaseComponent), "GetComponent", new Type[0])
                ?.MakeGenericMethod(_raftDockType)
                ?.Invoke(entity, null);
        }

        internal static void CallAddRaftDispatch(object raftDock, object dispatch)
        {
            _addRaftDispatchMethod.Invoke(raftDock, new[] { dispatch });
        }

        internal static void CallRemoveRaftDispatch(object raftDock, object dispatch)
        {
            _removeRaftDispatchMethod.Invoke(raftDock, new[] { dispatch });
        }

        internal static void CallReplaceRaftDispatch(object raftDock, object oldDispatch, object newDispatch)
        {
            _replaceRaftDispatchMethod.Invoke(raftDock, new[] { oldDispatch, newDispatch });
        }

        internal static void CallTogglePaused(object dispatch)
        {
            _togglePausedMethod.Invoke(dispatch, null);
        }

        internal static void CallUpdateLastDispatchTime(object dispatch, float time)
        {
            _updateLastDispatchTimeMethod.Invoke(dispatch, new object[] { time });
        }

        internal static bool GetIsPaused(object dispatch)
        {
            return (bool)_isPausedProperty.GetValue(dispatch);
        }

        /// <summary>
        /// Directly adds a dispatch via the original method.
        /// During replay, the prefix passes through (IsReplayingEvents == true),
        /// so the original method runs and fires RaftDispatchesChanged.
        /// </summary>
        internal static void DirectAddDispatch(object raftDock, object dispatch)
        {
            _addRaftDispatchMethod.Invoke(raftDock, new[] { dispatch });
        }

        /// <summary>
        /// Removes dispatch at index via the original method.
        /// </summary>
        internal static void DirectRemoveDispatchAt(object raftDock, int index)
        {
            var dispatch = GetDispatchByIndex(raftDock, index);
            if (dispatch != null)
            {
                _removeRaftDispatchMethod.Invoke(raftDock, new[] { dispatch });
            }
        }

        /// <summary>
        /// Replaces dispatch at index via the original method.
        /// </summary>
        internal static void DirectReplaceDispatchAt(object raftDock, int index, object newDispatch)
        {
            var oldDispatch = GetDispatchByIndex(raftDock, index);
            if (oldDispatch != null)
            {
                _replaceRaftDispatchMethod.Invoke(raftDock, new[] { oldDispatch, newDispatch });
            }
        }

        #endregion

        #region Harmony Prefixes

        private static bool AddRaftDispatchPrefix(object __instance, object dispatch)
        {
            return ReplayEvent.DoEntityPrefix((BaseComponent)__instance, entityID =>
            {
                return new RaftDispatchAddedEvent
                {
                    entityID = entityID,
                    dispatchData = SerializeDispatch(dispatch)
                };
            });
        }

        private static bool RemoveRaftDispatchPrefix(object __instance, object dispatch)
        {
            int index = GetDispatchIndex(__instance, dispatch);
            if (index < 0) return true;

            return ReplayEvent.DoEntityPrefix((BaseComponent)__instance, entityID =>
            {
                return new RaftDispatchRemovedEvent
                {
                    entityID = entityID,
                    dispatchIndex = index
                };
            });
        }

        private static bool ReplaceRaftDispatchPrefix(object __instance, object oldDispatch, object newDispatch)
        {
            int index = GetDispatchIndex(__instance, oldDispatch);
            if (index < 0) return true;

            return ReplayEvent.DoEntityPrefix((BaseComponent)__instance, entityID =>
            {
                return new RaftDispatchReplacedEvent
                {
                    entityID = entityID,
                    dispatchIndex = index,
                    newDispatchData = SerializeDispatch(newDispatch)
                };
            });
        }

        /// <summary>
        /// Patches the UI fragment's ToggleDispatchPause method.
        /// This is called when the user clicks the pause button in the UI.
        /// </summary>
        private static bool ToggleDispatchPausePrefix(object __instance, object raftDispatch)
        {
            // Get the RaftDock from the fragment
            var raftDock = _raftDockField?.GetValue(__instance);
            if (raftDock == null) return true;

            int index = GetDispatchIndex(raftDock, raftDispatch);
            if (index < 0) return true;

            bool currentlyPaused = GetIsPaused(raftDispatch);

            return ReplayEvent.DoEntityPrefix((BaseComponent)raftDock, entityID =>
            {
                return new RaftDispatchPauseToggledEvent
                {
                    entityID = entityID,
                    dispatchIndex = index,
                    wasPaused = currentlyPaused
                };
            });
        }

        /// <summary>
        /// Subscribe to RaftDispatchesChanged when fragment shows a dock
        /// </summary>
        private static void ShowFragmentPostfix(object __instance)
        {
            var raftDock = _raftDockField?.GetValue(__instance);
            if (raftDock == null) return;

            // Unsubscribe from previous dock if any
            if (_activeFragments.TryGetValue(__instance, out var prev))
            {
                var eventInfo = _raftDockType.GetEvent("RaftDispatchesChanged");
                eventInfo?.RemoveEventHandler(prev.raftDock, prev.handler);
            }

            // Create handler that updates the fragment's ListView
            EventHandler handler = (sender, args) =>
            {
                UpdateFragmentListView(__instance);
            };

            // Subscribe to the event
            var raftDispatchesChangedEvent = _raftDockType.GetEvent("RaftDispatchesChanged");
            raftDispatchesChangedEvent?.AddEventHandler(raftDock, handler);

            _activeFragments[__instance] = (raftDock, handler);
        }

        /// <summary>
        /// Unsubscribe from RaftDispatchesChanged when fragment is cleared
        /// </summary>
        private static void ClearFragmentPrefix(object __instance)
        {
            if (_activeFragments.TryGetValue(__instance, out var data))
            {
                var eventInfo = _raftDockType.GetEvent("RaftDispatchesChanged");
                eventInfo?.RemoveEventHandler(data.raftDock, data.handler);
                _activeFragments.Remove(__instance);
            }
        }

        /// <summary>
        /// Updates the fragment's ListView with current dispatch data
        /// </summary>
        private static void UpdateFragmentListView(object fragment)
        {
            try
            {
                var raftDock = _raftDockField?.GetValue(fragment);
                if (raftDock == null) return;

                var dispatches = _dispatchesField?.GetValue(fragment) as System.Collections.IList;
                var listView = _dispatchesListViewField?.GetValue(fragment);
                if (dispatches == null || listView == null) return;

                // Get current dispatches from the dock
                var raftDispatches = _raftDispatchesProperty?.GetValue(raftDock);
                if (raftDispatches == null) return;

                // Update the fragment's _dispatches list
                dispatches.Clear();
                foreach (var d in (System.Collections.IEnumerable)raftDispatches)
                {
                    dispatches.Add(d);
                }

                // Call Rebuild() on the ListView
                var rebuildMethod = listView.GetType().GetMethod("Rebuild");
                rebuildMethod?.Invoke(listView, null);
            }
            catch (Exception ex)
            {
                Plugin.LogWarning($"Failed to update Riverborne fragment ListView: {ex.Message}");
            }
        }

        /// <summary>
        /// Patches raft deletion from the UI (DeleteRaftFragment)
        /// </summary>
        private static bool DeleteRaftPrefix(object __instance)
        {
            var raft = _deleteFragmentRaftField?.GetValue(__instance);
            if (raft == null) return true;

            return ReplayEvent.DoEntityPrefix((BaseComponent)raft, entityID =>
            {
                return new RaftDeletedEvent
                {
                    entityID = entityID
                };
            });
        }

        /// <summary>
        /// Deterministic replacement for IsDropPointBlocked.
        /// The original uses physics position which is non-deterministic.
        /// This uses a tick-based cooldown instead.
        /// </summary>
        private const int SpawnCooldownTicks = 10; // ~0.5 seconds at 20 ticks/sec

        private static bool IsDropPointBlockedPrefix(object __instance, ref bool __result)
        {
            // Skip if not in multiplayer mode
            if (EventIO.IsNull) return true;

            var lastLaunchedRaft = _lastLaunchedRaftField?.GetValue(__instance);

            // If no raft was launched, not blocked
            var raftAsObject = lastLaunchedRaft as UnityEngine.Object;
            if (raftAsObject == null || !raftAsObject)
            {
                _lastLaunchedRaftField?.SetValue(__instance, null);
                _spawnCooldowns.Remove(__instance);
                __result = false;
                return false;
            }

            // Initialize or decrement cooldown
            if (!_spawnCooldowns.TryGetValue(__instance, out int cooldown))
            {
                // First check after spawn - start cooldown
                _spawnCooldowns[__instance] = SpawnCooldownTicks;
                __result = true;
                return false;
            }

            cooldown--;
            if (cooldown <= 0)
            {
                // Cooldown expired - clear the reference and allow spawn
                _lastLaunchedRaftField?.SetValue(__instance, null);
                _spawnCooldowns.Remove(__instance);
                __result = false;
            }
            else
            {
                // Still cooling down
                _spawnCooldowns[__instance] = cooldown;
                __result = true;
            }

            return false;
        }

        /// <summary>
        /// Gets the Raft component from an entity
        /// </summary>
        internal static object GetRaftComponent(BaseComponent entity)
        {
            if (_raftType == null) return null;
            return AccessTools.Method(typeof(BaseComponent), "GetComponent", new Type[0])
                ?.MakeGenericMethod(_raftType)
                ?.Invoke(entity, null);
        }

        #endregion
    }

    #region Serializable Data Classes

    [Serializable]
    public class CargoItem
    {
        public string goodId;
        public int amount;
    }

    [Serializable]
    public class RaftDispatchData
    {
        public string name;
        public List<CargoItem> cargo;
        public float interval;
        public float lastDispatchTime;
        public bool isPaused;
    }

    #endregion

    #region Replay Events

    [Serializable]
    class RaftDispatchAddedEvent : ReplayEvent
    {
        public string entityID;
        public RaftDispatchData dispatchData;

        public override void Replay(IReplayContext context)
        {
            var entity = GetEntityComponent(context, entityID);
            if (entity == null) return;

            var raftDock = RiverbornePatcher.GetRaftDockComponent(entity);
            if (raftDock == null) return;

            var dispatch = RiverbornePatcher.DeserializeDispatch(dispatchData);
            if (dispatch == null) return;

            // Use direct method to avoid triggering our patch again and to ensure UI updates
            RiverbornePatcher.DirectAddDispatch(raftDock, dispatch);
        }

        public override string ToActionString() => $"Adding raft dispatch '{dispatchData?.name}' to dock {entityID}";
    }

    [Serializable]
    class RaftDispatchRemovedEvent : ReplayEvent
    {
        public string entityID;
        public int dispatchIndex;

        public override void Replay(IReplayContext context)
        {
            var entity = GetEntityComponent(context, entityID);
            if (entity == null) return;

            var raftDock = RiverbornePatcher.GetRaftDockComponent(entity);
            if (raftDock == null) return;

            // Use direct method to remove by index (avoids reference equality issues)
            RiverbornePatcher.DirectRemoveDispatchAt(raftDock, dispatchIndex);
        }

        public override string ToActionString() => $"Removing raft dispatch #{dispatchIndex} from dock {entityID}";
    }

    [Serializable]
    class RaftDispatchReplacedEvent : ReplayEvent
    {
        public string entityID;
        public int dispatchIndex;
        public RaftDispatchData newDispatchData;

        public override void Replay(IReplayContext context)
        {
            var entity = GetEntityComponent(context, entityID);
            if (entity == null) return;

            var raftDock = RiverbornePatcher.GetRaftDockComponent(entity);
            if (raftDock == null) return;

            var newDispatch = RiverbornePatcher.DeserializeDispatch(newDispatchData);
            if (newDispatch == null) return;

            // Use direct method to replace by index (avoids reference equality issues)
            RiverbornePatcher.DirectReplaceDispatchAt(raftDock, dispatchIndex, newDispatch);
        }

        public override string ToActionString() => $"Updating raft dispatch #{dispatchIndex} to '{newDispatchData?.name}' on dock {entityID}";
    }

    [Serializable]
    class RaftDispatchPauseToggledEvent : ReplayEvent
    {
        public string entityID;
        public int dispatchIndex;
        public bool wasPaused;

        public override void Replay(IReplayContext context)
        {
            var entity = GetEntityComponent(context, entityID);
            if (entity == null) return;

            var raftDock = RiverbornePatcher.GetRaftDockComponent(entity);
            if (raftDock == null) return;

            var dispatch = RiverbornePatcher.GetDispatchByIndex(raftDock, dispatchIndex);
            if (dispatch == null)
            {
                Plugin.LogWarning($"Could not find dispatch at index {dispatchIndex} for dock {entityID}");
                return;
            }

            // Toggle if current state matches what we recorded
            bool currentlyPaused = RiverbornePatcher.GetIsPaused(dispatch);
            if (currentlyPaused == wasPaused)
            {
                RiverbornePatcher.CallTogglePaused(dispatch);

                // If we're unpausing, also update the last dispatch time like the UI does
                if (wasPaused)
                {
                    var dayNightCycle = context.GetSingleton<IDayNightCycle>();
                    RiverbornePatcher.CallUpdateLastDispatchTime(dispatch, dayNightCycle.PartialDayNumber);
                }
            }
        }

        public override string ToActionString() => $"Toggling pause on dispatch #{dispatchIndex} (was paused: {wasPaused}) on dock {entityID}";
    }

    [Serializable]
    class RaftDeletedEvent : ReplayEvent
    {
        public string entityID;

        public override void Replay(IReplayContext context)
        {
            var entity = GetEntityComponent(context, entityID);
            if (entity == null) return;

            var raft = RiverbornePatcher.GetRaftComponent(entity);
            if (raft == null) return;

            var raftComponent = (BaseComponent)raft;
            var inventory = raftComponent.GetComponent<Inventory>();
            var cargo = inventory != null ? inventory.Stock : new Timberborn.Common.ReadOnlyList<GoodAmount>();
            var coordinates = CoordinateSystem.WorldToGridInt(raftComponent.Transform.position);

            var entityService = context.GetSingleton<EntityService>();
            entityService.Delete(raftComponent);

            var recoveredGoodSpawner = context.GetSingleton<RecoveredGoodStackSpawner>();
            recoveredGoodSpawner.AddAwaitingGoods(coordinates, cargo);
        }

        public override string ToActionString() => $"Deleting raft {entityID}";
    }

    #endregion
}
