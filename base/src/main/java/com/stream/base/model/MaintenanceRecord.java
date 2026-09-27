package com.stream.base.model;

/**
 * Geçmişten Çekilen Konu (JetStream Geçmiş Hafızası / State Store):
 * Aracın geçmişteki son yetkili servis ve mekanik sağlık durumu.
 * Topic: stream.car.maintenance
 */
public class MaintenanceRecord {
    public double lastServiceOdometerKm;
    public double brakePadHealthPercent;
    public double oilLifePercent;
    public boolean inspectionPassed;
    public String lastServiceNotes;

    public MaintenanceRecord() {}

    public MaintenanceRecord(double lastServiceOdometerKm, double brakePadHealthPercent, double oilLifePercent, boolean inspectionPassed, String lastServiceNotes) {
        this.lastServiceOdometerKm = lastServiceOdometerKm;
        this.brakePadHealthPercent = brakePadHealthPercent;
        this.oilLifePercent = oilLifePercent;
        this.inspectionPassed = inspectionPassed;
        this.lastServiceNotes = lastServiceNotes;
    }

    @Override
    public String toString() {
        return String.format("MaintenanceRecord[SonServis=%.0f km, BalataSağlığı=%%%.0f, YağÖmrü=%%%.0f, MuayeneGeçti=%b, Notlar='%s']",
                lastServiceOdometerKm, brakePadHealthPercent, oilLifePercent, inspectionPassed, lastServiceNotes);
    }
}
