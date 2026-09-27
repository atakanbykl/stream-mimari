package com.stream.base.model;

/**
 * Dinlenen Konu 4 (JetStream): Şoför tercih ve sürüş modu profili.
 * Topic: stream.driver.profile
 */
public class DriverProfile {
    public String mode;             // ECO, COMFORT, SPORT
    public double speedLimitKmh;
    public boolean aggressiveBrakingAllowed;

    public DriverProfile() {}

    public DriverProfile(String mode, double speedLimitKmh, boolean aggressiveBrakingAllowed) {
        this.mode = mode;
        this.speedLimitKmh = speedLimitKmh;
        this.aggressiveBrakingAllowed = aggressiveBrakingAllowed;
    }

    @Override
    public String toString() {
        return String.format("DriverProfile[Mod=%s, HızSınırı=%.0f km/h]", mode, speedLimitKmh);
    }
}
