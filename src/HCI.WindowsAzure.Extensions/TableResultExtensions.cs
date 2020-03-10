using System;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using HCI.WindowsAzure.Extensions.Common;
using Microsoft.WindowsAzure.Storage.Table;

namespace HCI.WindowsAzure.Extensions
{
    /// <summary>
    /// Reactive Extension Methods for <see cref="TableResult" />.
    /// </summary>
    public static class TableResultExtensions
    {
        /// <summary>
        /// Determine if operation completed successfully by evaluating <see cref="TableResult.HttpStatusCode" />.
        /// </summary>
        /// <param name="tableResult"></param>

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsSuccess(this TableResult tableResult)
        {
            if (tableResult is null)
            {
                return false;
            }

            return tableResult.HttpStatusCode >= 200 && tableResult.HttpStatusCode < 300;
        }

        /// <summary>
        /// Perform an action based on <paramref name="condition" />.
        /// </summary>
        /// <param name="tableResult">  </param>
        /// <param name="condition">    </param>
        /// <param name="successAction"></param>
        public static void When(this TableResult tableResult, bool condition, Expression<Action<TableResult>> successAction)
        {
            Guard.Null(tableResult, nameof(tableResult));

            if (condition)
            {
                var expressionAction = Expression.Lambda<Action>(successAction).Compile();
                expressionAction();
            }
        }

        /// <summary>
        /// Perform an action based on <paramref name="condition" />.
        /// </summary>
        /// <param name="tableResult">  </param>
        /// <param name="condition">    </param>
        /// <param name="successAction"></param>
        /// <param name="failAction">   </param>
        public static void When(this TableResult tableResult, bool condition, Expression<Action<TableResult>> successAction, Expression<Action<TableResult>> failAction)
        {
            Guard.Null(tableResult, nameof(tableResult));

            if (condition)
            {
                var expressionAction = Expression.Lambda<Action>(successAction).Compile();
                expressionAction();
            }
            else
            {
                var expressionFailAction = Expression.Lambda<Action>(failAction).Compile();
                expressionFailAction();
            }
        }

        /// <summary>
        /// Perform an action based on <paramref name="condition" />
        /// </summary>
        /// <param name="tableResult">  </param>
        /// <param name="condition">    </param>
        /// <param name="successAction"></param>
        /// <exception cref="ArgumentNullException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void When(this TableResult tableResult, bool condition, Action<TableResult> successAction)
        {
            Guard.Null(tableResult, nameof(tableResult));

            if (condition)
            {
                successAction(tableResult);
            }
        }

        /// <summary>
        /// Perform an action based on <paramref name="condition" />.
        /// </summary>
        /// <param name="tableResult">  </param>
        /// <param name="condition">    </param>
        /// <param name="successAction"></param>
        /// <exception cref="ArgumentNullException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void When(this TableResult tableResult, bool condition, Action successAction)
        {
            Guard.Null(tableResult, nameof(tableResult));

            if (condition)
            {
                successAction();
            }
        }

        /// <summary>
        /// Perform an action based on <paramref name="condition" />.
        /// </summary>
        /// <param name="tableResult">  </param>
        /// <param name="condition">    </param>
        /// <param name="successAction"></param>
        /// <param name="failAction">   </param>
        /// <exception cref="ArgumentNullException"></exception>

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void When(this TableResult tableResult, bool condition, Action successAction, Action failAction)
        {
            Guard.Null(tableResult, nameof(tableResult));

            if (condition)
            {
                successAction();
            }
            else
            {
                failAction();
            }
        }

        /// <summary>
        /// Perform an action based on <paramref name="condition" />.
        /// </summary>
        /// <param name="tableResult">  </param>
        /// <param name="condition">    </param>
        /// <param name="successAction"></param>
        /// <param name="failAction">   </param>
        /// <exception cref="ArgumentNullException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void When(this TableResult tableResult, Func<TableResult, bool> condition, Action successAction, Action failAction)
        {
            Guard.Null(tableResult, nameof(tableResult));

            if (condition.Invoke(tableResult))
            {
                successAction();
            }
            else
            {
                failAction();
            }
        }

        /// <summary>
        /// Perform an action based on <paramref name="conditions" />
        /// </summary>
        /// <param name="tableResult">  </param>
        /// <param name="conditions">   </param>
        /// <param name="successAction"></param>
        /// <param name="failAction">   </param>
        /// <exception cref="ArgumentNullException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void When(this TableResult tableResult, Func<TableResult, bool>[] conditions, Action successAction, Action failAction)
        {
            Guard.Null(tableResult, nameof(tableResult));

            if (conditions.All(condition => condition(tableResult)))
            {
                successAction();
            }
            else
            {
                failAction();
            }
        }
    }
}
