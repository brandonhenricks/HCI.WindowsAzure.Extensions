using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace HCI.WindowsAzure.Extensions.Common
{
    /// <summary>
    /// Argument Guard Class.
    /// </summary>
    internal static class Guard
    {
        /// <summary>
        /// Checks Null value of the specified object.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <exception cref="ArgumentNullException">obj</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Null(object obj)
        {
            if (obj is null)
            {
                throw new ArgumentNullException(nameof(obj));
            }
        }

        /// <summary>
        /// Checks Null value of the specified object.
        /// </summary>
        /// <param name="obj">    The object.</param>
        /// <param name="message">The message.</param>
        /// <exception cref="ArgumentNullException">obj</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Null(object obj, string message)
        {
            if (obj is null)
            {
                throw new ArgumentNullException(message);
            }
        }

        /// <summary>
        /// Checks Null or Empty value of the specified string.
        /// </summary>
        /// <param name="str">The string.</param>
        /// <exception cref="ArgumentNullException">str</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void NullOrEmpty(string str)
        {
            if (string.IsNullOrEmpty(str))
            {
                throw new ArgumentNullException(nameof(str));
            }
        }

        /// <summary>
        /// Checks Null or Empty value of the specified string.
        /// </summary>
        /// <param name="str">    The string.</param>
        /// <param name="message">The message.</param>
        /// <exception cref="ArgumentNullException">str</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void NullOrEmpty(string str, string message)
        {
            if (string.IsNullOrEmpty(str))
            {
                throw new ArgumentNullException(message);
            }
        }

        /// <summary>
        /// Checks Null or Empty value of the specified <see cref="IEnumerable{T}" />.
        /// </summary>
        /// <param name="source">The enumerable.</param>
        /// <exception cref="ArgumentNullException">source</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void NullOrEmpty<T>(IEnumerable<T> source)
        {
            if (!source.Any())
            {
                throw new ArgumentNullException(nameof(source));
            }
        }

        /// <summary>
        /// Checks Null or Empty value of the specified <see cref="IEnumerable{T}" />.
        /// </summary>
        /// <param name="source"> The enumerable.</param>
        /// <param name="message">The message.</param>
        /// <exception cref="ArgumentNullException">source</exception>

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void NullOrEmpty<T>(IEnumerable<T> source, string message)
        {
            if (!source.Any())
            {
                throw new ArgumentNullException(message);
            }
        }

        /// <summary>
        /// Checks Null or Empty value of the specified <see cref="IList{T}" />.
        /// </summary>
        /// <param name="source">The list.</param>
        /// <exception cref="ArgumentNullException">source</exception>

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void NullOrEmpty<T>(IList<T> source)
        {
            if (source?.Count == 0)
            {
                throw new ArgumentNullException(nameof(source));
            }
        }

        /// <summary>
        /// Checks Null or Empty value of the specified <see cref="IList{T}" />.
        /// </summary>
        /// <param name="source"> The list.</param>
        /// <param name="message">The message.</param>
        /// <exception cref="ArgumentNullException">source</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void NullOrEmpty<T>(IList<T> source, string message)
        {
            if (source?.Count == 0)
            {
                throw new ArgumentNullException(message);
            }
        }
    }
}
