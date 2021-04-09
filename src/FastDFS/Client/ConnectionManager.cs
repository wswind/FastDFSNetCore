using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

namespace FastDFS.Client
{
    /// <summary>
    /// 取消static实例，由Map管理多实例
    /// </summary>
    public class ConnectionManager
    {
        public ConnectionManager(IEnumerable<EndPoint> trackers)
        {
            foreach (EndPoint point in trackers)
            {
                if (!TrackerPools.ContainsKey(point))
                {
                    TrackerPools.Add(point, new Pool(point, FDFSConfig.TrackerMaxConnection));
                    _listTrackers.Add(point);
                }
            }
        }

        private List<EndPoint> _listTrackers = new List<EndPoint>();
        private readonly Dictionary<EndPoint, Pool> TrackerPools = new Dictionary<EndPoint, Pool>();
        private readonly ConcurrentDictionary<EndPoint, Pool> StorePools = new ConcurrentDictionary<EndPoint, Pool>();

        internal Task<Connection> GetStorageConnectionAsync(EndPoint endPoint)
        {
            return StorePools.GetOrAdd(endPoint, (ep) => new Pool(ep, FDFSConfig.StorageMaxConnection)).GetConnectionAsync();
        }

        internal Task<Connection> GetTrackerConnectionAsync()
        {
            int num = new Random().Next(TrackerPools.Count);
            Pool pool = TrackerPools[_listTrackers[num]];
            return pool.GetConnectionAsync();
        }

        public static Dictionary<string, ConnectionManager> ManagerMap = new Dictionary<string, ConnectionManager>();

        public static void Initialize(IEnumerable<EndPoint> trackers, string server)
        {
            if(!ManagerMap.ContainsKey(server))
            {
                var manager = new ConnectionManager(trackers);
                ManagerMap.Add(server, manager);
            }
        }

        public static ConnectionManager GetManager(string server)
        {
            return ManagerMap[server];
        }

    }
}