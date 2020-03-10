using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace HCI.WindowsAzure.Extensions
{
    internal static class EnumerableExtensions
    {
        /// <summary>
        /// Adds the collection of <paramref name="items" /> to <paramref name="this" />.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="this"> Current <see cref="HashSet{T}" /></param>
        /// <param name="items">The items to add.</param>

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool AddRange<T>(this HashSet<T> @this, IEnumerable<T> items)
        {
            bool allAdded = true;

            foreach (T item in items)
            {
                allAdded &= @this.Add(item);
            }

            return allAdded;
        }
    }
}
