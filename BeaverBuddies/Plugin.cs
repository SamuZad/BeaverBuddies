﻿using BeaverBuddies.Connect;
using BeaverBuddies.DesyncDetecter;
using BeaverBuddies.Steam;
using BeaverBuddies.Editor;
using BeaverBuddies.Fixes;
using BeaverBuddies.IO;
using BeaverBuddies.MultiStart;
using BeaverBuddies.Reporting;
using BeaverBuddies.Util;
using BeaverBuddies.Util.Logging;
using Bindito.Core;
using HarmonyLib;
using System.Diagnostics;
using System.Reflection;
using Timberborn.ModManagerScene;
using Timberborn.SceneLoading;
using Timberborn.StartingLocationSystem;
using Timberborn.TemplateSystem;
using Timberborn.TutorialSystemUI;

namespace BeaverBuddies
{
    [Context("Game")]
    public class ReplayConfigurator : IConfigurator
    {

        public void Configure(IContainerDefinition containerDefinition)
        {
            if (EventIO.Config.GetNetMode() == NetMode.None) return;

            // Reset everything before loading singletons
            SingletonManager.Reset();

            Plugin.Log($"Registering In Game Services");

            // Add client connection Singletons, since we can now
            // connect from the in-game Options menu (even if we're not
            // playing co-op right now).
            containerDefinition.Bind<ClientConnectionService>().AsSingleton();
            containerDefinition.Bind<ClientConnectionUI>().AsSingleton();
            containerDefinition.Bind<ConfigIOService>().AsSingleton();
            containerDefinition.Bind<SteamOverlayConnectionService>().AsSingleton();
            containerDefinition.Bind<RegisteredLocalizationService>().AsSingleton(); 
            containerDefinition.Bind<Settings>().AsSingleton();

            MultiStartConfigurator.Configure(containerDefinition);

            // EventIO gets set before load, so if it's null, this is a regular
            // game, so don't initialize these services.
            if (EventIO.IsNull) return;

            Plugin.Log("Registering Co-op services");
            //containerDefinition.Bind<ServerConnectionService>().AsSingleton();
            containerDefinition.Bind<ReplayService>().AsSingleton();
            containerDefinition.Bind<RecordToFileService>().AsSingleton();
            containerDefinition.Bind<TickProgressService>().AsSingleton();
            containerDefinition.Bind<TickingService>().AsSingleton();
            containerDefinition.Bind<DeterminismService>().AsSingleton();
            containerDefinition.Bind<TickReplacerService>().AsSingleton();
            containerDefinition.Bind<RehostingService>().AsSingleton();
            containerDefinition.Bind<ReportingService>().AsSingleton();
            containerDefinition.Bind<LateTickableBuffer>().AsSingleton();

            if (EventIO.Config.Debug)
            {
                Plugin.Log("Debug Mode Active; registering DesyncDetecterService");
                containerDefinition.Bind<DesyncDetecterService>().AsSingleton();
            }

        }
    }

    [Context("MainMenu")]
    public class ConnectionMenuConfigurator : IConfigurator
    {
        public void Configure(IContainerDefinition containerDefinition)
        {
            if (EventIO.Config.GetNetMode() == NetMode.None) return;

            // This will be called if the player exits to the main menu,
            // so it's best to reset everything.
            SingletonManager.Reset();
            EventIO.Reset();

            Plugin.Log($"Registering Main Menu Services");
            containerDefinition.Bind<ClientConnectionService>().AsSingleton();
            containerDefinition.Bind<ClientConnectionUI>().AsSingleton();
            containerDefinition.Bind<FirstTimerService>().AsSingleton();
            containerDefinition.Bind<ConfigIOService>().AsSingleton();
            containerDefinition.Bind<RegisteredLocalizationService>().AsSingleton();
            containerDefinition.Bind<MultiplayerMapMetadataService>().AsSingleton();
            containerDefinition.Bind<Settings>().AsSingleton();

            //new ReportingService().PostDesync("test").ContinueWith(result => Plugin.Log($"Posted: {result.Result}"));
            containerDefinition.Bind<SteamOverlayConnectionService>().AsSingleton();

            //ReflectionUtils.PrintChildClasses(typeof(MonoBehaviour), 
            //    "Start", "Awake", "Update", "FixedUpdate", "LateUpdate", "OnEnable", "OnDisable", "OnDestroy");
            //ReflectionUtils.PrintChildClasses(typeof(IUpdatableSingleton));
            //ReflectionUtils.PrintChildClasses(typeof(ILateUpdatableSingleton));
            //ReflectionUtils.PrintChildClasses(typeof(IBatchControlRowItem));
            //ReflectionUtils.PrintChildClasses(typeof(IUpdateableBatchControlRowItem));
            //ReflectionUtils.PrintChildClasses(typeof(IParallelTickableSingleton));
            //ReflectionUtils.FindStaticFields();
            //ReflectionUtils.FindHashSetFields();
        }
    }

    [HarmonyPatch]
    public class Plugin : IModStarter
    {
        public static readonly string Version = Assembly.GetExecutingAssembly().GetName().Version.ToString();
        public const string Name = "BeaverBuddies";
        public const string ID = "beaverbuddies";

        private static ILogger logger;

        public void StartMod()
        {
            logger = new UnityLogger();

            // Create a default config temporarily
            // Will be loaded later by ConfigIOService
            ReplayConfig config = new ReplayConfig();

            EventIO.Config = config;
            
            Log($"{Name} v{Version} is loaded!");

            if (config.GetNetMode() == NetMode.None) return;

            Harmony harmony = new Harmony(ID);
            harmony.PatchAll();

            Log(UnityEngine.Application.consoleLogPath);
        }

        public static string GetWithDate(string message)
        {
            return $"[{System.DateTime.Now.ToString("HH-mm-ss.ff")}] {message}";
        }

        public static void Log(string message)
        {
            if (!EventIO.Config.Verbose) return;
            logger.LogInfo(GetWithDate(message));
        }

        public static void LogWarning(string message)
        {
            logger.LogWarning(GetWithDate(message));
        }

        public static void LogError(string message)
        {
            logger.LogError(GetWithDate(message));
        }

        public static void LogStackTrace()
        {
            logger.LogInfo(new StackTrace().ToString());
        }
    }
}
