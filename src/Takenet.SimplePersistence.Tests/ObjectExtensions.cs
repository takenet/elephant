using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;

namespace Takenet.SimplePersistence.Tests
{
    public static class ObjectExtensions
    {
        /// <summary>    
        /// This dictionary caches the delegates for each 'to-clone' type.    
        /// </summary>    
        private static Dictionary<Type, Delegate> _cachedIL = new Dictionary<Type, Delegate>();        

        /// <summary>    
        /// Generic cloning method that clones an object using IL.    
        /// Only the first call of a certain type will hold back performance.    
        /// After the first call, the compiled IL is executed.    
        /// </summary>    
        /// <typeparam name="T">Type of object to clone</typeparam>    
        /// <param name="value">Object to clone</param>    
        /// <returns>Cloned object</returns>    
        public static T Clone<T>(this T value)
        {
            Delegate myExec = null;
            if (!_cachedIL.TryGetValue(typeof(T), out myExec))
            {
                // Create ILGenerator (both DM declarations work)
                // DynamicMethod dymMethod = new DynamicMethod("DoClone", typeof(T), 
                //      new Type[] { typeof(T) }, true);
                DynamicMethod dymMethod = new DynamicMethod("DoClone", typeof(T),
                    new Type[] { typeof(T) }, Assembly.GetExecutingAssembly().ManifestModule, true);
                ConstructorInfo cInfo = value.GetType().GetConstructor(new Type[] { });
                ILGenerator generator = dymMethod.GetILGenerator();
                LocalBuilder lbf = generator.DeclareLocal(typeof(T));
                generator.Emit(OpCodes.Newobj, cInfo);
                generator.Emit(OpCodes.Stloc_0);
                foreach (FieldInfo field in value.GetType().GetFields(
                    System.Reflection.BindingFlags.Instance
                    | System.Reflection.BindingFlags.NonPublic
                    | System.Reflection.BindingFlags.Public))
                {
                    generator.Emit(OpCodes.Ldloc_0);
                    generator.Emit(OpCodes.Ldarg_0);
                    generator.Emit(OpCodes.Ldfld, field);
                    generator.Emit(OpCodes.Stfld, field);
                }
                generator.Emit(OpCodes.Ldloc_0);
                generator.Emit(OpCodes.Ret);
                myExec = dymMethod.CreateDelegate(typeof(Func<T, T>));
                _cachedIL.Add(typeof(T), myExec);
            }
            return ((Func<T, T>)myExec)(value);
        }
    }
}