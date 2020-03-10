using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.WindowsAzure.Storage.Table;

namespace HCI.WindowsAzure.Extensions
{
    /// <summary>
    /// Extensions methods for <see cref="DynamicTableEntity" />.
    /// </summary>
    public static class DynamicTableEntityExtensions
    {
        /// <summary>
        /// Convert <see cref="DynamicTableEntity" /> to a concrete class.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T Convert<T>(this DynamicTableEntity entity) where T : new()
        {
            if (entity is null) return default;

            var obj = Activator.CreateInstance<T>();

            var objectProperties = obj.GetType().GetProperties();

            var entityProps = entity.GetType().GetProperties();

            // Class Properties

            foreach (var prop in entityProps)
            {
                var objectProperty = Array.Find(objectProperties, v => v.Match(prop.Name));

                if (objectProperty is null) continue;

                try
                {
                    var value = prop.GetValue(entity);

                    if (value is null) continue;

                    objectProperty.SetValue(obj, value);
                }
                catch (Exception)
                {
                    continue;
                }
            }

            // DynamicTable Properties
            foreach (var prop in entity.Properties)
            {
                if (prop.Value is null) continue;

                var objectProperty = Array.Find(objectProperties, v => v.Match(prop.Key));

                if (objectProperty is null) continue;

                try
                {
                    objectProperty.SetValue(obj, prop.Value.PropertyAsObject);
                }
                catch (Exception)
                {
                    continue;
                }
            }

            return obj;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool Match(this PropertyInfo prop, string propertyName)
        {
            return prop.Name.Equals(propertyName, StringComparison.OrdinalIgnoreCase);
        }
    }
}
