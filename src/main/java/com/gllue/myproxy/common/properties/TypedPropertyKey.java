package com.gllue.myproxy.common.properties;

import com.gllue.myproxy.common.properties.TypedPropertyValue.Type;

/**
 * Typed property key.
 */
public interface TypedPropertyKey {
    String CONNECTOR = ".";

    /**
     * Get prefix of property key.
     *
     * The complete property key is "prefix + CONNECTOR + property key".
     *
     * @return prefix
     */
    String getPrefix();
    
    /**
     * Get property key.
     *
     * The complete property key is "prefix + CONNECTOR + property key".
     * 
     * @return property key
     */
    String getKey();
    
    /**
     * Get default property value.
     * 
     * @return default property value
     */
    String getDefaultValue();
    
    /**
     * Get property type.
     * 
     * @return property type
     */
    Type getType();

    /**
     * Get complete key.
     *
     * @return complete key
     */
    default String getCompleteKey() {
        return getPrefix() + CONNECTOR + getKey();
    }
}
