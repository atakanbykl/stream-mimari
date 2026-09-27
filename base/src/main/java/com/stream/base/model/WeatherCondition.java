package com.stream.base.model;

/**
 * Dinlenen Konu 3 (JetStream): Canlı hava ve yol durumu akışı.
 * Topic: stream.weather.current
 */
public class WeatherCondition {
    public String condition;       // CLEAR, RAINY, SNOWY, ICY
    public double ambientTempCelsius;
    public boolean slipperyRoad;

    public WeatherCondition() {}

    public WeatherCondition(String condition, double ambientTempCelsius, boolean slipperyRoad) {
        this.condition = condition;
        this.ambientTempCelsius = ambientTempCelsius;
        this.slipperyRoad = slipperyRoad;
    }

    @Override
    public String toString() {
        return String.format("Weather[Durum=%s, DışSıcaklık=%.1f°C, KayganYol=%b]",
                condition, ambientTempCelsius, slipperyRoad);
    }
}
