using System.Collections.Concurrent;
using System.Collections.Generic;

namespace System.Data.MonetDb.Helpers.Mapi.Protocols
{
    internal static class MapiProtocolFactory
    {
        private readonly static IDictionary<int, Type> Protocols = new ConcurrentDictionary<int, Type>();
        private readonly static object SyncLock = new object();

        /// <summary>
        /// Register protocol
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="version"></param>
        public static void Register<T>(int version) where T : IMapiProtocol
        {
            lock (SyncLock)
            {
                if (!Protocols.ContainsKey(version))
                {
                    Protocols.Add(version, typeof(T));
                }
            }
        }

        /// <summary>
        /// Get protocol implementation by version
        /// </summary>
        /// <param name="version">Protocol version</param>
        /// <returns>IMapiProtocol implementation instance or null</returns>
        public static IMapiProtocol GetProtocol(int version)
        {
            lock (SyncLock)
            {
                Type t;

                if (!Protocols.TryGetValue(version, out t))
                    return null;

                return Activator.CreateInstance(t) as IMapiProtocol;
            }
        }
    }
}
