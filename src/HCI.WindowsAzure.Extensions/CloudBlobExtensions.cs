using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using HCI.WindowsAzure.Extensions.Common;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace HCI.WindowsAzure.Extensions
{
    public static class CloudBlobExtensions
    {
        /// <summary>
        /// Return a List of BlogSegments from <paramref name="blobContainer" />
        /// </summary>
        /// <param name="blobContainer"></param>
        /// <param name="prefix">       </param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="StorageException"></exception>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async Task<IReadOnlyList<IListBlobItem>> ListBlobsAsync(this CloudBlobContainer blobContainer, string prefix)
        {
            Guard.Null(blobContainer, nameof(blobContainer));
            Guard.NullOrEmpty(prefix, nameof(prefix));

            BlobContinuationToken continuationToken = null;

            var results = new List<IListBlobItem>();

            try
            {
                do
                {
                    var response = await blobContainer.ListBlobsSegmentedAsync(prefix, continuationToken);
                    continuationToken = response.ContinuationToken;
                    results.AddRange(response.Results);
                }
                while (continuationToken != null);

                return results;
            }
            catch (StorageException)
            {
                return default;
            }
        }
    }
}
