using System.Collections.Concurrent;
using System.Reflection;
using System.Reflection.Emit;

namespace ArrowCollection;

/// <summary>
/// Provides high-performance field access through IL-emitted delegates.
/// This class creates and caches optimized getter and setter delegates for field access,
/// bypassing reflection overhead after initial setup.
/// </summary>
/// <remarks>
/// Inspired by the Orleans serialization framework's approach to field access.
/// </remarks>
public static class FieldAccessor
{
    private static readonly ConcurrentDictionary<FieldInfo, Delegate> _getters = new();
    private static readonly ConcurrentDictionary<FieldInfo, Delegate> _setters = new();

    /// <summary>
    /// Gets a typed getter delegate for the specified field.
    /// </summary>
    /// <typeparam name="TDeclaring">The type that declares the field.</typeparam>
    /// <typeparam name="TField">The field type.</typeparam>
    /// <param name="field">The field to create a getter for.</param>
    /// <returns>A delegate that gets the field value from an instance.</returns>
    public static Func<TDeclaring, TField> GetGetter<TDeclaring, TField>(FieldInfo field)
    {
        return (Func<TDeclaring, TField>)_getters.GetOrAdd(field, f => CreateGetter<TDeclaring, TField>(f));
    }

    /// <summary>
    /// Gets a typed setter delegate for the specified field.
    /// </summary>
    /// <typeparam name="TDeclaring">The type that declares the field.</typeparam>
    /// <typeparam name="TField">The field type.</typeparam>
    /// <param name="field">The field to create a setter for.</param>
    /// <returns>A delegate that sets the field value on an instance.</returns>
    public static Action<TDeclaring, TField> GetSetter<TDeclaring, TField>(FieldInfo field)
    {
        return (Action<TDeclaring, TField>)_setters.GetOrAdd(field, f => CreateSetter<TDeclaring, TField>(f));
    }

    /// <summary>
    /// Gets an untyped getter delegate for the specified field.
    /// </summary>
    /// <param name="field">The field to create a getter for.</param>
    /// <returns>A delegate that gets the field value from an instance.</returns>
    public static Func<object, object?> GetGetter(FieldInfo field)
    {
        return (Func<object, object?>)_getters.GetOrAdd(field, CreateUntypedGetter);
    }

    /// <summary>
    /// Gets an untyped setter delegate for the specified field.
    /// </summary>
    /// <param name="field">The field to create a setter for.</param>
    /// <returns>A delegate that sets the field value on an instance.</returns>
    public static Action<object, object?> GetSetter(FieldInfo field)
    {
        return (Action<object, object?>)_setters.GetOrAdd(field, CreateUntypedSetter);
    }

    private static Func<TDeclaring, TField> CreateGetter<TDeclaring, TField>(FieldInfo field)
    {
        var declaringType = field.DeclaringType ?? throw new ArgumentException("Field must have a declaring type.", nameof(field));
        
        var method = new DynamicMethod(
            name: $"Get_{field.DeclaringType!.Name}_{field.Name}",
            returnType: typeof(TField),
            parameterTypes: [typeof(TDeclaring)],
            owner: typeof(FieldAccessor),
            skipVisibility: true);

        var il = method.GetILGenerator();
        
        // Load the instance
        il.Emit(OpCodes.Ldarg_0);
        
        // If TDeclaring is object but field is on a value type or different type, we need to cast/unbox
        if (typeof(TDeclaring) == typeof(object) && declaringType.IsValueType)
        {
            il.Emit(OpCodes.Unbox, declaringType);
            il.Emit(OpCodes.Ldfld, field);
        }
        else if (typeof(TDeclaring) == typeof(object))
        {
            il.Emit(OpCodes.Castclass, declaringType);
            il.Emit(OpCodes.Ldfld, field);
        }
        else
        {
            il.Emit(OpCodes.Ldfld, field);
        }
        
        // Box if needed
        if (typeof(TField) == typeof(object) && field.FieldType.IsValueType)
        {
            il.Emit(OpCodes.Box, field.FieldType);
        }
        
        il.Emit(OpCodes.Ret);

        return method.CreateDelegate<Func<TDeclaring, TField>>();
    }

    private static Action<TDeclaring, TField> CreateSetter<TDeclaring, TField>(FieldInfo field)
    {
        var declaringType = field.DeclaringType ?? throw new ArgumentException("Field must have a declaring type.", nameof(field));
        
        var method = new DynamicMethod(
            name: $"Set_{field.DeclaringType!.Name}_{field.Name}",
            returnType: typeof(void),
            parameterTypes: [typeof(TDeclaring), typeof(TField)],
            owner: typeof(FieldAccessor),
            skipVisibility: true);

        var il = method.GetILGenerator();
        
        // Load the instance
        il.Emit(OpCodes.Ldarg_0);
        
        // Cast if needed
        if (typeof(TDeclaring) == typeof(object) && declaringType.IsValueType)
        {
            il.Emit(OpCodes.Unbox, declaringType);
        }
        else if (typeof(TDeclaring) == typeof(object))
        {
            il.Emit(OpCodes.Castclass, declaringType);
        }
        
        // Load the value
        il.Emit(OpCodes.Ldarg_1);
        
        // Unbox if needed
        if (typeof(TField) == typeof(object) && field.FieldType.IsValueType)
        {
            il.Emit(OpCodes.Unbox_Any, field.FieldType);
        }
        else if (typeof(TField) == typeof(object) && !field.FieldType.IsValueType)
        {
            il.Emit(OpCodes.Castclass, field.FieldType);
        }
        
        // Store the field
        il.Emit(OpCodes.Stfld, field);
        il.Emit(OpCodes.Ret);

        return method.CreateDelegate<Action<TDeclaring, TField>>();
    }

    private static Func<object, object?> CreateUntypedGetter(FieldInfo field)
    {
        var declaringType = field.DeclaringType ?? throw new ArgumentException("Field must have a declaring type.", nameof(field));
        
        var method = new DynamicMethod(
            name: $"GetUntyped_{field.DeclaringType!.Name}_{field.Name}",
            returnType: typeof(object),
            parameterTypes: [typeof(object)],
            owner: typeof(FieldAccessor),
            skipVisibility: true);

        var il = method.GetILGenerator();
        
        // Load the instance
        il.Emit(OpCodes.Ldarg_0);
        
        // Cast/unbox to declaring type
        if (declaringType.IsValueType)
        {
            il.Emit(OpCodes.Unbox, declaringType);
            il.Emit(OpCodes.Ldfld, field);
        }
        else
        {
            il.Emit(OpCodes.Castclass, declaringType);
            il.Emit(OpCodes.Ldfld, field);
        }
        
        // Box if value type
        if (field.FieldType.IsValueType)
        {
            il.Emit(OpCodes.Box, field.FieldType);
        }
        
        il.Emit(OpCodes.Ret);

        return method.CreateDelegate<Func<object, object?>>();
    }

    private static Action<object, object?> CreateUntypedSetter(FieldInfo field)
    {
        var declaringType = field.DeclaringType ?? throw new ArgumentException("Field must have a declaring type.", nameof(field));
        
        var method = new DynamicMethod(
            name: $"SetUntyped_{field.DeclaringType!.Name}_{field.Name}",
            returnType: typeof(void),
            parameterTypes: [typeof(object), typeof(object)],
            owner: typeof(FieldAccessor),
            skipVisibility: true);

        var il = method.GetILGenerator();
        
        // Load the instance
        il.Emit(OpCodes.Ldarg_0);
        
        // Cast/unbox to declaring type
        if (declaringType.IsValueType)
        {
            il.Emit(OpCodes.Unbox, declaringType);
        }
        else
        {
            il.Emit(OpCodes.Castclass, declaringType);
        }
        
        // Load the value
        il.Emit(OpCodes.Ldarg_1);
        
        // Unbox/cast to field type
        if (field.FieldType.IsValueType)
        {
            il.Emit(OpCodes.Unbox_Any, field.FieldType);
        }
        else
        {
            il.Emit(OpCodes.Castclass, field.FieldType);
        }
        
        // Store the field
        il.Emit(OpCodes.Stfld, field);
        il.Emit(OpCodes.Ret);

        return method.CreateDelegate<Action<object, object?>>();
    }
}
