using System;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using HCI.WindowsAzure.Extensions.Common;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;

namespace HCI.WindowsAzure.Extensions
{
    /// <summary>
    /// Extension Methods for <see cref="CloudStorageAccount" /> These methods are aimed to reduce
    /// boilerplate code when trying to access a <see cref="CloudTable" />,
    /// <see cref="CloudBlob" />, or <see cref="CloudBlobContainer" />
    /// </summary>
    public static class CloudStorageAccountExtensions
    {
        /// <summary>
        /// Gets the cloud BLOB.
        /// </summary>
        /// <param name="cloudStorageAccount">The cloud storage account.</param>
        /// <param name="containerName">      Name of the container.</param>
        /// <param name="blobName">           Name of the BLOB.</param>

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static CloudBlob GetCloudBlob(this CloudStorageAccount cloudStorageAccount, string containerName, string blobName)
        {
            Guard.Null(cloudStorageAccount, nameof(cloudStorageAccount));
            Guard.NullOrEmpty(containerName, nameof(containerName));
            Guard.NullOrEmpty(blobName, nameof(blobName));

            try
            {
                var blobClient = cloudStorageAccount?.CreateCloudBlobClient();

                var blobContainer = blobClient?.GetContainerReference(containerName);

                return blobContainer?.GetBlobReference(blobName);
            }
            catch (StorageException)
            {
                return default;
            }
        }

        /// <summary>
        /// Retrieve a <see cref="CloudBlobContainer" /> reference from the
        /// <see cref="CloudStorageAccount" /> by <paramref name="containerName" />.
        /// </summary>
        /// <param name="cloudStorageAccount"></param>
        /// <param name="containerName">      </param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="StorageException"></exception>
        public static CloudBlobContainer GetCloudBlobContainer(this CloudStorageAccount cloudStorageAccount, string containerName)
        {
            Guard.Null(cloudStorageAccount, nameof(cloudStorageAccount));
            Guard.NullOrEmpty(containerName, nameof(containerName));

            try
            {
                var blobClient = cloudStorageAccount?.CreateCloudBlobClient();

                return blobClient?.GetContainerReference(containerName);
            }
            catch (StorageException)
            {
                return default;
            }
        }

        /// <summary>
        /// Retrieve a <see cref="CloudTable" /> reference from the
        /// <see cref="CloudStorageAccount" /> by <paramref name="tableName" />.
        /// </summary>
        /// <param name="cloudStorageAccount"></param>
        /// <param name="tableName">          </param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="StorageException"></exception>

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static CloudTable GetCloudTable(this CloudStorageAccount cloudStorageAccount, string tableName, bool optimizeConnection = true)
        {
            Guard.Null(cloudStorageAccount, nameof(cloudStorageAccount));
            Guard.NullOrEmpty(tableName, nameof(tableName));

            try
            {
                if (optimizeConnection)
                {
                    cloudStorageAccount.OptimizePerformance();
                }

                var tableClient = cloudStorageAccount?.CreateCloudTableClient();

                return tableClient?.GetTableReference(tableName);
            }
            catch (StorageException)
            {
                return default;
            }
        }

        /// <summary>
        /// Gets the cloud table asynchronous, and will create if the <paramref name="tableName" />
        /// doesn't exist.
        /// </summary>
        /// <param name="cloudStorageAccount">The cloud storage account.</param>
        /// <param name="tableName">          Name of the table.</param>
        /// <param name="create">             if set to <c>true</c> [create].</param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="StorageException"></exception>
        public static Task<CloudTable> GetCloudTableAsync(this CloudStorageAccount cloudStorageAccount, string tableName, bool create)
        {
            Guard.Null(cloudStorageAccount, nameof(cloudStorageAccount));
            Guard.NullOrEmpty(tableName, nameof(tableName));

            try
            {
                var tableClient = cloudStorageAccount?.CreateCloudTableClient();

                var table = tableClient?.GetTableReference(tableName);

                if (create)
                {
                    table?.CreateIfNotExistsAsync();
                }

                return Task.FromResult(table);
            }
            catch (StorageException)
            {
                return Task.FromResult<CloudTable>(default);
            }
        }

        /// <summary>
        /// Settings to improve performance
        /// </summary>
        /// <exception cref="ArgumentNullException"></exception>

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void OptimizePerformance(this CloudStorageAccount cloudStorageAccount, TableStorageOptions options = null)
        {
            Guard.Null(cloudStorageAccount, nameof(cloudStorageAccount));

            var tableServicePoint = ServicePointManager.FindServicePoint(cloudStorageAccount?.TableEndpoint);

            if (options is null)
            {
                options = new TableStorageOptions();
            }

            tableServicePoint.UseNagleAlgorithm = options.UseNagleAlgorithm;
            tableServicePoint.Expect100Continue = options.Expect100Continue;
            tableServicePoint.ConnectionLimit = options.ConnectionLimit;
        }

        internal sealed class TableStorageOptions
        {
            public int ConnectionLimit { get; internal set; } = 10;
            public bool Expect100Continue { get; internal set; } = false;
            public int Retries { get; set; } = 3;
            public double RetryWaitTimeInSeconds { get; set; } = 1;
            public bool UseNagleAlgorithm { get; internal set; } = false;
        }
    }
}
