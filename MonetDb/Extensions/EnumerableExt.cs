using System.Collections.Generic;
using System.Linq;

namespace System.Data.MonetDb.Extensions
{
    internal static class EnumerableExt
    {
        /// <summary>
        /// Checks, whether source contains one of the listed values
        /// </summary>
        /// <param name="source">Collection of values to look for</param>
        /// <param name="values">Collection of values to look with</param>
        /// <param name="which">If return is <c>true</c> then first 
        /// interfered value will be assigned to this parameter</param>
        /// <returns>Returns <c>true</c> if on of the values contains on of the source value</returns>
        public static bool In<T>(this IEnumerable<T> source, out T which, params T[] values)
        {
            which = default(T);

            foreach (var value in values.Where(source.Contains))
            {
                which = value;
                return true;
            }

            return false;
        }

        /// <summary>
        /// Checks, whether source exists in the listed values
        /// </summary>
        /// <param name="source">String to look for</param>
        /// <param name="values">Collection of values to look with</param>
        /// <param name="which">If return is <c>true</c> then first 
        /// interfered value will be assigned to this parameter</param>
        /// <returns>Returns <c>true</c> if on of the values contains on of the source value</returns>
        public static bool In(this string source, out string which, params string[] values)
        {
            which = null;

            foreach (var str in values.Where(str => source.Equals(str)))
            {
                which = str;
                return true;
            }

            return false;
        }
    }
}
