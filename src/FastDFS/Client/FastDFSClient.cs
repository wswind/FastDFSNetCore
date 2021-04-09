using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace FastDFS.Client
{
    public class FastDFSClient
    {
        public static Task<FDFSFileInfo> GetFileInfoAsync(StorageNode storageNode, string fileName, string server)
        {
            return QUERY_FILE_INFO.Instance.GetRequest(storageNode.EndPoint,
                    storageNode.GroupName,
                    fileName)
                .GetResponseAsync<FDFSFileInfo>(server);
        }

        public static Task<StorageNode> GetStorageNodeAsync(string server)
        {
            return GetStorageNodeAsync(null, server);
        }

        public static async Task<StorageNode> GetStorageNodeAsync(string groupName, string server)
        {
            var request = string.IsNullOrWhiteSpace(groupName) ?
                            QUERY_STORE_WITHOUT_GROUP_ONE.Instance.GetRequest()
                            : QUERY_STORE_WITH_GROUP_ONE.Instance.GetRequest(groupName);
            var response = await request.GetResponseAsync<QUERY_STORE_WITH_GROUP_ONE.Response>(server);

            var point = new IPEndPoint(IPAddress.Parse(response.IPStr), response.Port);
            return new StorageNode
            {
                GroupName = response.GroupName,
                EndPoint = point,
                StorePathIndex = response.StorePathIndex
            };
        }

        public static async Task RemoveFileAsync(string groupName, string fileName, string server)
        {
            var response = await QUERY_UPDATE.Instance
                .GetRequest(groupName, fileName)
                .GetResponseAsync<QUERY_UPDATE.Response>(server);

            var point = new IPEndPoint(IPAddress.Parse(response.IPStr), response.Port);
            await DELETE_FILE.Instance
                .GetRequest(point, groupName, fileName)
                .GetResponseAsync<EmptyFDFSResponse>(server);
        }

        public static async Task<string> UploadFileAsync(StorageNode storageNode, Stream contentStream, string fileExt, string server)
        {
            var response = await UPLOAD_FILE.Instance.GetRequest(storageNode,
                    fileExt,
                    contentStream)
                .GetResponseAsync<UPLOAD_FILE.Response>(server);
            return response.FileName;
        }

        public static async Task<string> UploadFileAsync(StorageNode storageNode, byte[] contentByte, string fileExt, string server)
        {
            using (var contentStream = new MemoryStream(contentByte, false))
            {
                return await UploadFileAsync(storageNode, contentStream, fileExt, server);
            }
        }

        public static async Task<string> UploadAppenderFileAsync(StorageNode storageNode, Stream contentStream, string fileExt, string server)
        {
            var response = await UPLOAD_APPEND_FILE.Instance.GetRequest(storageNode,
                    fileExt,
                    contentStream)
                .GetResponseAsync<UPLOAD_APPEND_FILE.Response>(server);
            return response.FileName;
        }

        public static async Task<string> UploadAppenderFileAsync(StorageNode storageNode, byte[] contentByte, string fileExt, string server)
        {
            using (var contentStream = new MemoryStream(contentByte, false))
            {
                return await UploadAppenderFileAsync(storageNode, contentStream, fileExt, server);
            }
        }

        public static async Task AppendFileAsync(string groupName, string fileName, Stream contentStream, string server)
        {
            var response = await QUERY_UPDATE.Instance
                .GetRequest(groupName, fileName)
                .GetResponseAsync<QUERY_UPDATE.Response>(server);

            var point = new IPEndPoint(IPAddress.Parse(response.IPStr), response.Port);

            await APPEND_FILE.Instance.GetRequest(point,
                    fileName,
                    contentStream)
                .GetResponseAsync<APPEND_FILE.Response>(server);
        }

        public static async Task AppendFileAsync(string groupName, string fileName, byte[] contentByte, string server)
        {
            using (var contentStream = new MemoryStream(contentByte, false))
            {
                await AppendFileAsync(groupName, fileName, contentStream, server);
            }
        }

        public static async Task<byte[]> DownloadFileAsync(StorageNode storageNode, string fileName, string server,
            long offset = 0,
            long length = 0)
        {
            using (var memoryStream = new MemoryStream(length > 0 ? (int)length : 0))
            {
                var downloadStream = new StreamDownloadCallback(memoryStream);
                await DownloadFileAsync(storageNode, fileName, downloadStream, server, offset, length);
                return memoryStream.ToArray();
            }
        }

        public static async Task DownloadFileAsync(StorageNode storageNode, string fileName,
            IDownloadCallback downloadCallback, string server,
            long offset = 0,
            long length = 0)
        {
            await DOWNLOAD_FILE.Instance
                .GetRequest(storageNode,
                    fileName,
                    Tuple.Create(offset, length),
                    downloadCallback)
                .GetResponseAsync<DOWNLOAD_FILE.Response>(server);
        }
    }
}