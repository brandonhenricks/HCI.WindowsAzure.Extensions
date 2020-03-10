using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using HCI.WindowsAzure.Extensions.Common;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace HCI.WindowsAzure.Extensions
{
    /// <summary>
    /// Extension Methods for <see cref="CloudTable" /> to help reduce boilerplate for CRUD
    /// TableStorage actions.
    /// </summary>
    public static class CloudTableExtensions
    {
        /// <summary>
        /// Delete <see cref="TableEntity" /> from Table Storage.
        /// </summary>
        /// <param name="cloudTable"> </param>
        /// <param name="tableEntity"></param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="StorageException"></exception>
        public static Task<TableResult> DeleteData(this CloudTable cloudTable, ITableEntity tableEntity)
        {
            Guard.Null(cloudTable, nameof(cloudTable));
            Guard.Null(tableEntity, nameof(tableEntity));

            try
            {
                var updateOperation = TableOperation.Delete(tableEntity);

                return cloudTable.ExecuteAsync(updateOperation);
            }
            catch (StorageException)
            {
                return Task.FromResult<TableResult>(default);
            }
        }

        /// <summary>
        /// Delete <see cref="ITableEntity" /> by <paramref name="partitionKey" /> and <paramref name="rowKey" />.
        /// </summary>
        /// <param name="cloudTable">  </param>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey">      </param>
        /// <exception cref="StorageException"></exception>
        /// <exception cref="ArgumentNullException"></exception>
        public static Task<TableResult> DeleteData(this CloudTable cloudTable, string partitionKey, string rowKey)
        {
            Guard.Null(cloudTable, nameof(cloudTable));
            Guard.NullOrEmpty(partitionKey, nameof(partitionKey));
            Guard.NullOrEmpty(rowKey, nameof(rowKey));

            try
            {
                var tableEntity = new DynamicTableEntity(partitionKey, rowKey)
                {
                    ETag = "*"
                };

                var updateOperation = TableOperation.Delete(tableEntity);

                return cloudTable.ExecuteAsync(updateOperation);
            }
            catch (StorageException)
            {
                return Task.FromResult<TableResult>(default);
            }
        }

        /// <summary>
        /// Executes the <paramref name="query" /> on <paramref name="cloudTable" /> as a <see cref="TableQuerySegment{TElement}" />.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="cloudTable">       </param>
        /// <param name="query">            </param>
        /// <param name="cancellationToken"></param>
        /// <exception cref="StorageException"></exception>
        /// <exception cref="ArgumentNullException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async Task<IReadOnlyList<T>> ExecuteQueryAsync<T>(this CloudTable cloudTable, TableQuery<T> query, CancellationToken cancellationToken = default) where T : ITableEntity, new()
        {
            Guard.Null(cloudTable, nameof(cloudTable));
            Guard.Null(query, nameof(query));

            var runningQuery = new TableQuery<T>()
            {
                FilterString = query.FilterString,
                SelectColumns = query.SelectColumns
            };

            var results = new List<T>();
            TableContinuationToken token = null;

            try
            {
                do
                {
                    runningQuery.TakeCount = query.TakeCount - results.Count;

                    var queryResult = await cloudTable
                        .ExecuteQuerySegmentedAsync(runningQuery, token)
                        .ConfigureAwait(false);

                    results.Capacity += queryResult.Results.Count;

                    token = queryResult.ContinuationToken;

                    results.AddRange(queryResult);
                } while (token != null && !cancellationToken.IsCancellationRequested && (query.TakeCount == null || results.Count < query.TakeCount.Value));

                return results;
            }
            catch (StorageException)
            {
                return default;
            }
        }

        /// <summary>
        /// Return Data by <see cref="ITableEntity.PartitionKey" /> and <see cref="ITableEntity.RowKey" />
        /// </summary>
        /// <param name="cloudTable">  </param>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey">      </param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="StorageException"></exception>
        public static Task<TableResult> GetData(this CloudTable cloudTable, string partitionKey, string rowKey)
        {
            Guard.Null(cloudTable, nameof(cloudTable));
            Guard.NullOrEmpty(partitionKey, nameof(partitionKey));
            Guard.NullOrEmpty(rowKey, nameof(rowKey));

            try
            {
                var findOperation = TableOperation.Retrieve(partitionKey, rowKey);

                return cloudTable.ExecuteAsync(findOperation);
            }
            catch (StorageException)
            {
                return Task.FromResult<TableResult>(default);
            }
        }

        /// <summary>
        /// Return an object of <typeparamref name="T" /> by <paramref name="partitionKey" />, <paramref name="rowKey" />.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="cloudTable">   </param>
        /// <param name="partitionKey"> </param>
        /// <param name="rowKey">       </param>
        /// <param name="selectColumns"></param>
        /// <exception cref="StorageException"></exception>
        /// <exception cref="ArgumentNullException"></exception>

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async Task<T> GetData<T>(this CloudTable cloudTable, string partitionKey, string rowKey, IList<string> selectColumns = null) where T : ITableEntity, new()
        {
            Guard.Null(cloudTable, nameof(cloudTable));
            Guard.NullOrEmpty(partitionKey, nameof(partitionKey));
            Guard.NullOrEmpty(rowKey, nameof(rowKey));

            try
            {
                var findOperation = TableOperation.Retrieve<T>(partitionKey, rowKey, selectColumns?.ToList());

                var tableResult = await cloudTable
                    .ExecuteAsync(findOperation)
                    .ConfigureAwait(false);

                return (T)tableResult.Result;
            }
            catch (StorageException)
            {
                return default;
            }
        }

        /// <summary>
        /// Return a <see cref="List{T}" /> by <paramref name="query" />.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="cloudTable"></param>
        /// <param name="query">     </param>
        /// <exception cref="StorageException"></exception>
        /// <exception cref="ArgumentNullException"></exception>

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async Task<IReadOnlyList<T>> GetData<T>(this CloudTable cloudTable, TableQuery<T> query) where T : ITableEntity, IEquatable<T>, new()
        {
            Guard.Null(cloudTable, nameof(cloudTable));
            Guard.Null(query, nameof(query));

            TableContinuationToken token = null;
            TableQuerySegment<T> queryResult;
            var dataSet = new HashSet<T>();

            try
            {
                do
                {
                    queryResult = await cloudTable
                        .ExecuteQuerySegmentedAsync(query, token)
                        .ConfigureAwait(false);

                    dataSet.AddRange(queryResult);

                    token = queryResult.ContinuationToken;
                } while (token != null);

                return dataSet.ToList();
            }
            catch (StorageException)
            {
                return default;
            }
        }

        /// <summary>
        /// Return an object of <typeparamref name="T" /> by <paramref name="partitionKey" />,
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="cloudTable">  The cloud table.</param>
        /// <param name="query">       The query.</param>
        /// <param name="filterClause">The filter clause.</param>
        /// <exception cref="StorageException"></exception>
        /// <exception cref="ArgumentNullException"></exception>
        public static async Task<IReadOnlyList<T>> GetData<T>(this CloudTable cloudTable, TableQuery<T> query, Func<T, bool> filterClause = null) where T : ITableEntity, new()
        {
            Guard.Null(cloudTable, nameof(cloudTable));
            Guard.Null(query, nameof(query));

            TableContinuationToken token = null;
            TableQuerySegment<T> queryResult;
            var dataList = new List<T>();

            try
            {
                do
                {
                    queryResult = await cloudTable
                        .ExecuteQuerySegmentedAsync(query, token)
                        .ConfigureAwait(false);

                    dataList.AddRange(queryResult);

                    token = queryResult.ContinuationToken;
                } while (token != null);

                return filterClause != null ? dataList.Where(filterClause).ToList() : dataList;
            }
            catch (StorageException)
            {
                return default;
            }
        }

        /// <summary>
        /// Gets the filtered data from <paramref name="query" />.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="cloudTable">The cloud table.</param>
        /// <param name="query">     The query.</param>
        /// <param name="expression">The filter clause.</param>
        /// <exception cref="StorageException"></exception>
        /// <exception cref="ArgumentNullException"></exception>

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async Task<IEnumerable<T>> GetFilterData<T>(this CloudTable cloudTable, TableQuery<T> query, Expression<Func<T, bool>> expression) where T : ITableEntity, new()
        {
            if (cloudTable is null) return Enumerable.Empty<T>();
            Guard.Null(query, nameof(query));

            TableContinuationToken token = null;
            TableQuerySegment<T> queryResult;
            var dataSet = new HashSet<T>();
            var expressFunc = expression.Compile();

            try
            {
                do
                {
                    queryResult = await cloudTable
                        .ExecuteQuerySegmentedAsync(query, token)
                        .ConfigureAwait(false);

                    dataSet.AddRange(queryResult);

                    token = queryResult.ContinuationToken;
                } while (token != null);

                return dataSet.Where(expressFunc);
            }
            catch (StorageException)
            {
                return Enumerable.Empty<T>();
            }
            catch (Exception)
            {
                return Enumerable.Empty<T>();
            }
        }

        /// <summary>
        /// Gets the filtered data from <paramref name="query" /> and applies <paramref name="filterClause" />.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="cloudTable">  </param>
        /// <param name="query">       </param>
        /// <param name="filterClause"></param>
        /// <exception cref="StorageException"></exception>
        /// <exception cref="ArgumentNullException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async Task<IReadOnlyList<T>> GetFilteredData<T>(this CloudTable cloudTable, TableQuery<T> query, Func<T, bool> filterClause) where T : ITableEntity, IEquatable<T>, new()
        {
            Guard.Null(cloudTable, nameof(cloudTable));
            Guard.Null(query, nameof(query));

            try
            {
                var expFilter = filterClause.FuncToExpression();

                var data = await cloudTable.GetFilterData(query, expFilter);

                return data.ToList();
            }
            catch (Exception)
            {
                return default;
            }
        }

        /// <summary>
        /// Executes a <paramref name="query" /> selecting only the
        /// <see cref="ITableEntity.PartitionKey" /> to see if there is data.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="cloudTable"></param>
        /// <param name="query">     </param>
        /// <exception cref="StorageException"></exception>
        /// <exception cref="ArgumentNullException"></exception>
        public static async Task<bool> HasData<T>(this CloudTable cloudTable, TableQuery<T> query) where T : ITableEntity, new()
        {
            Guard.Null(cloudTable, nameof(cloudTable));
            Guard.Null(query, nameof(query));

            TableContinuationToken token = null;
            TableQuerySegment<T> queryResult;

            var dataList = new HashSet<T>();

            if (query.SelectColumns?.Count > 0)
            {
                query.SelectColumns.Clear();
            }

            query.SelectColumns = new List<string>()
            {
                nameof(ITableEntity.PartitionKey)
            };

            try
            {
                do
                {
                    queryResult = await cloudTable
                        .ExecuteQuerySegmentedAsync(query, token, null, null)
                        .ConfigureAwait(false);

                    dataList.AddRange(queryResult);

                    token = queryResult.ContinuationToken;
                } while (token != null);

                return dataList.Count > 0;
            }
            catch (StorageException)
            {
                return default;
            }
        }

        /// <summary>
        /// Executes a <paramref name="query" /> selecting only the
        /// <see cref="ITableEntity.PartitionKey" /> to see if there is data.
        /// </summary>
        /// <param name="cloudTable"></param>
        /// <param name="query">     </param>
        /// <exception cref="StorageException"></exception>
        /// <exception cref="ArgumentNullException"></exception>

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async Task<bool> HasData(this CloudTable cloudTable, TableQuery query)
        {
            Guard.Null(cloudTable, nameof(cloudTable));
            Guard.Null(query, nameof(query));

            TableContinuationToken token = null;
            TableQuerySegment queryResult;
            var dataList = new HashSet<ITableEntity>();

            if (query.SelectColumns?.Count > 0)
            {
                query.SelectColumns.Clear();
            }

            query.SelectColumns = new List<string>()
            {
                nameof(ITableEntity.PartitionKey)
            };

            try
            {
                do
                {
                    queryResult = await cloudTable
                        .ExecuteQuerySegmentedAsync(query, token, null, null)
                        .ConfigureAwait(false);

                    dataList.AddRange(queryResult);

                    token = queryResult.ContinuationToken;
                } while (token != null);

                return dataList.Count > 0;
            }
            catch (StorageException)
            {
                return false;
            }
        }

        /// <summary>
        /// Executes a <see cref="TableQuery" /> selecting only the
        /// <see cref="ITableEntity.PartitionKey" /> to see if there is data.
        /// </summary>
        /// <param name="cloudTable">  </param>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey">      </param>
        /// <exception cref="StorageException"></exception>
        /// <exception cref="ArgumentNullException"></exception>

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async Task<bool> HasData(this CloudTable cloudTable, string partitionKey, string rowKey)
        {
            Guard.Null(cloudTable, nameof(cloudTable));
            Guard.NullOrEmpty(partitionKey, nameof(partitionKey));
            Guard.NullOrEmpty(rowKey, nameof(rowKey));

            try
            {
                var findOperation = TableOperation.Retrieve<ITableEntity>(partitionKey, rowKey, new List<string>() { nameof(ITableEntity.PartitionKey) });

                var tableResult = await cloudTable
                    .ExecuteAsync(findOperation)
                    .ConfigureAwait(false);

                return tableResult.HttpStatusCode < 300;
            }
            catch (StorageException)
            {
                return false;
            }
        }

        /// <summary>
        /// Performs a <see cref="TableOperation.Insert(ITableEntity)" /> on the
        /// <paramref name="cloudTable" /> with <see cref="ITableEntity" />
        /// </summary>
        /// <param name="cloudTable"> </param>
        /// <param name="tableEntity"></param>
        /// <exception cref="StorageException"></exception>
        /// <exception cref="ArgumentNullException"></exception>

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Task<TableResult> InsertData(this CloudTable cloudTable, ITableEntity tableEntity)
        {
            Guard.Null(cloudTable, nameof(cloudTable));
            Guard.Null(tableEntity, nameof(tableEntity));

            try
            {
                var updateOperation = TableOperation.Insert(tableEntity);

                return cloudTable.ExecuteAsync(updateOperation);
            }
            catch (StorageException)
            {
                return Task.FromResult<TableResult>(default);
            }
        }

        /// <summary>
        /// Inserts or Merge the <paramref name="tableEntity" /> data.
        /// </summary>
        /// <param name="cloudTable"> </param>
        /// <param name="tableEntity"></param>
        /// <exception cref="StorageException"></exception>
        /// <exception cref="ArgumentNullException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Task<TableResult> InsertOrMergeData(this CloudTable cloudTable, ITableEntity tableEntity)
        {
            Guard.Null(cloudTable, nameof(cloudTable));
            Guard.Null(tableEntity, nameof(tableEntity));

            try
            {
                var updateOperation = TableOperation.InsertOrMerge(tableEntity);

                return cloudTable.ExecuteAsync(updateOperation);
            }
            catch (StorageException)
            {
                return Task.FromResult<TableResult>(default);
            }
        }

        /// <summary>
        /// Inserts or Replace the <paramref name="tableEntity" /> data.
        /// </summary>
        /// <param name="cloudTable"> </param>
        /// <param name="tableEntity"></param>
        /// <exception cref="StorageException"></exception>
        /// <exception cref="ArgumentNullException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Task<TableResult> InsertOrReplaceData(this CloudTable cloudTable, ITableEntity tableEntity)
        {
            Guard.Null(cloudTable, nameof(cloudTable));
            Guard.Null(tableEntity, nameof(tableEntity));

            try
            {
                var updateOperation = TableOperation.InsertOrReplace(tableEntity);

                return cloudTable.ExecuteAsync(updateOperation);
            }
            catch (StorageException)
            {
                return Task.FromResult<TableResult>(default);
            }
        }

        /// <summary>
        /// Merges the <paramref name="tableEntity" /> data.
        /// </summary>
        /// <param name="cloudTable"> </param>
        /// <param name="tableEntity"></param>
        /// <exception cref="StorageException"></exception>
        /// <exception cref="ArgumentNullException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Task<TableResult> MergeData(this CloudTable cloudTable, ITableEntity tableEntity)
        {
            Guard.Null(cloudTable, nameof(cloudTable));
            Guard.Null(tableEntity, nameof(tableEntity));

            try
            {
                var mergeOperation = TableOperation.Merge(tableEntity);
                return cloudTable.ExecuteAsync(mergeOperation);
            }
            catch (StorageException)
            {
                return Task.FromResult<TableResult>(default);
            }
        }

        /// <summary>
        /// Replaces the <paramref name="tableEntity" /> data.
        /// </summary>
        /// <param name="cloudTable"> The cloud table.</param>
        /// <param name="tableEntity">The table entity.</param>
        /// <exception cref="ArgumentNullException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Task<TableResult> ReplaceData(this CloudTable cloudTable, ITableEntity tableEntity)
        {
            Guard.Null(cloudTable, nameof(cloudTable));
            Guard.Null(tableEntity, nameof(tableEntity));

            try
            {
                var replaceOperation = TableOperation.Replace(tableEntity);
                return cloudTable.ExecuteAsync(replaceOperation);
            }
            catch (StorageException)
            {
                return Task.FromResult<TableResult>(default);
            }
        }

        /// <summary>
        /// Converts <paramref name="method" /> to <see cref="Expression{TDelegate}" />
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="method"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static Expression<Func<T, TResult>> FuncToExpression<T, TResult>(this Func<T, TResult> method)
        {
            return x => method(x);
        }
    }
}
