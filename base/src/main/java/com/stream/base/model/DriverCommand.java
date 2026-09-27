package com.stream.base.model;

/**
 * Dinlenen Konu 1 (NATS Core): Şoför veya otonom sistemden gelen anlık sürüş komutları.
 * Topic: core.car.commands
 */
public class DriverCommand {
    public String action;    // ACCELERATE, BRAKE, CRUISE_CONTROL, GEAR_SHIFT
    public double targetValue;
    public String source;

    public DriverCommand() {}

    public DriverCommand(String action, double targetValue, String source) {
        this.action = action;
        this.targetValue = targetValue;
        this.source = source;
    }

    @Override
    public String toString() {
        return String.format("DriverCommand[action=%s, target=%.1f, source=%s]", action, targetValue, source);
    }
}
