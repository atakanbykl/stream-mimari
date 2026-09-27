package com.stream.base.config;

/**
 * Yapılandırma dosyası eksik olduğunda veya parametreler doğrulanamadığında fırlatılan istisna.
 */
public class ConfigValidationException extends RuntimeException {

    public ConfigValidationException(String message) {
        super(message);
    }

    public ConfigValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
