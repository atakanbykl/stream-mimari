package com.stream.node1.gui;

import com.stream.base.model.AlertData;
import com.stream.base.model.DriverCommand;
import com.stream.base.model.RadarSensorData;
import com.stream.node1.App;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.border.TitledBorder;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * node-1 Otomobil ECU Kontrol ve Mesajlaşma Gösterge Paneli (GUI)
 * 
 * Bu arayüz, node-1'in o anda yaptığı işlemleri:
 * 1. 📥 Aldığı mesajları (NATS Core & JetStream abonelikleri)
 * 2. 📤 Attığı mesajları (NATS Core & JetStream yayınları)
 * 3. 📜 Geçmişten çektiği mesajları (JetStream Direct Get)
 * anlık ve canlı olarak görselleştirir.
 */
public class Node1Dashboard extends JFrame implements NodeActivityListener {

    private final App app;
    private final DateTimeFormatter timeFmt = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    // Anlık Telemetri Gösterge Etiketleri
    private JLabel lblSpeed;
    private JLabel lblRpm;
    private JLabel lblGear;
    private JLabel lblFuel;
    private JLabel lblBattery;
    private JLabel lblMode;
    private JLabel lblConnection;
    private JLabel lblMaintenanceAlert;

    // Tablolar ve Veri Modelleri
    private DefaultTableModel modelReceived;
    private DefaultTableModel modelSent;
    private DefaultTableModel modelHistorical;
    private JTable tblReceived;
    private JTable tblSent;
    private JTable tblHistorical;

    // Konsol Akış Alanı
    private JTextArea txtLiveLog;

    public Node1Dashboard(App app) {
        super("node-1 | Otomobil ECU Mesaj Akış ve Kontrol Paneli");
        this.app = app;

        // Sistem Look and Feel uygula
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception ignored) {}

        initUi();
        app.addActivityListener(this);
        updateDashboardCards();
    }

    private void initUi() {
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(1000, 720);
        setMinimumSize(new Dimension(850, 600));
        setLocationRelativeTo(null);

        // Ana Konteyner
        JPanel mainPanel = new JPanel(new BorderLayout(8, 8));
        mainPanel.setBorder(new EmptyBorder(10, 10, 10, 10));

        // 1. ÜST PANEL: ARAÇ ANLIK DURUM VE TELEMETRİ KARTLARI
        mainPanel.add(createHeaderPanel(), BorderLayout.NORTH);

        // 2. ORTA PANEL: SEKMELİ MESAJ AKIŞ GÖRÜNTÜLEYİCİ
        mainPanel.add(createTabsPanel(), BorderLayout.CENTER);

        // 3. ALT PANEL: TEST VE SİMÜLASYON BUTONLARI
        mainPanel.add(createControlBar(), BorderLayout.SOUTH);

        setContentPane(mainPanel);
    }

    // =========================================================================
    // 1. ÜST PANEL (TELEMETRİ VE BAĞLANTI KARTLARI)
    // =========================================================================

    private JPanel createHeaderPanel() {
        JPanel header = new JPanel(new BorderLayout(6, 6));
        header.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createEtchedBorder(), " Araç Canlı Durumu ve NATS İletişim Hattı ",
                TitledBorder.LEFT, TitledBorder.TOP, new Font("Segoe UI", Font.BOLD, 13)));

        JPanel cardsPanel = new JPanel(new GridLayout(1, 7, 8, 8));
        cardsPanel.setBorder(new EmptyBorder(6, 6, 6, 6));

        lblSpeed = createMetricCard("Hız", "50.0 km/h", new Color(0, 102, 204));
        lblRpm = createMetricCard("Devir", "2000 RPM", new Color(51, 102, 153));
        lblGear = createMetricCard("Vites", "3", new Color(70, 80, 95));
        lblFuel = createMetricCard("Yakıt", "%82.5", new Color(34, 139, 34));
        lblBattery = createMetricCard("Batarya", "%95.0", new Color(46, 139, 87));
        lblMode = createMetricCard("Sürüş Modu", "COMFORT", new Color(138, 43, 226));
        lblConnection = createMetricCard("NATS Hattı", "KONTROL EDİLİYOR", Color.DARK_GRAY);

        cardsPanel.add(lblSpeed);
        cardsPanel.add(lblRpm);
        cardsPanel.add(lblGear);
        cardsPanel.add(lblFuel);
        cardsPanel.add(lblBattery);
        cardsPanel.add(lblMode);
        cardsPanel.add(lblConnection);

        lblMaintenanceAlert = new JLabel(" Servis Durumu: Kontrol ediliyor...", JLabel.LEFT);
        lblMaintenanceAlert.setFont(new Font("Segoe UI", Font.BOLD, 12));
        lblMaintenanceAlert.setForeground(new Color(100, 100, 100));

        header.add(cardsPanel, BorderLayout.CENTER);
        header.add(lblMaintenanceAlert, BorderLayout.SOUTH);
        return header;
    }

    private JLabel createMetricCard(String title, String initialValue, Color valueColor) {
        JLabel label = new JLabel(String.format("<html><center><font size='2' color='#666666'>%s</font><br/><b><font size='4' color='%s'>%s</font></b></center></html>",
                title, toHex(valueColor), initialValue), JLabel.CENTER);
        label.setOpaque(true);
        label.setBackground(new Color(245, 247, 250));
        label.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(new Color(220, 224, 230), 1),
                new EmptyBorder(4, 4, 4, 4)));
        return label;
    }

    // =========================================================================
    // 2. ORTA PANEL (SEKMELİ TABLO VE CANLI LOG AKIŞI)
    // =========================================================================

    private JTabbedPane createTabsPanel() {
        JTabbedPane tabs = new JTabbedPane();
        tabs.setFont(new Font("Segoe UI", Font.BOLD, 12));

        // SEKME 1: ALINAN MESAJLAR (SUBSCRIBED)
        String[] colsRecv = {"Zaman", "Katman", "Konu (Topic)", "Mesaj İçeriği (Payload)"};
        modelReceived = new DefaultTableModel(colsRecv, 0);
        tblReceived = createStyledTable(modelReceived);
        tabs.addTab("📥 Alınan Mesajlar (Dinlenen 4 Topic)", new JScrollPane(tblReceived));

        // SEKME 2: ATILAN MESAJLAR (PUBLISHED)
        String[] colsSent = {"Zaman", "Katman", "Konu (Topic)", "Mesaj İçeriği (Payload)"};
        modelSent = new DefaultTableModel(colsSent, 0);
        tblSent = createStyledTable(modelSent);
        tabs.addTab("📤 Atılan Mesajlar (Yayınlanan 3 Topic)", new JScrollPane(tblSent));

        // SEKME 3: GEÇMİŞTEN ÇEKİLEN MESAJLAR (DIRECT GET)
        String[] colsHist = {"Sorgu Zamanı", "Konu (Topic)", "JetStream Sıra No", "Kayıt Zamanı", "Kalıcı Durum Detayı"};
        modelHistorical = new DefaultTableModel(colsHist, 0);
        tblHistorical = createStyledTable(modelHistorical);
        tabs.addTab("📜 Geçmişten Çekilen Mesajlar (Direct Get)", new JScrollPane(tblHistorical));

        // SEKME 4: TÜM CANLI AKIŞ KONSOLU
        txtLiveLog = new JTextArea();
        txtLiveLog.setEditable(false);
        txtLiveLog.setFont(new Font("Consolas", Font.PLAIN, 12));
        txtLiveLog.setBackground(new Color(30, 30, 30));
        txtLiveLog.setForeground(new Color(220, 220, 220));
        txtLiveLog.setCaretColor(Color.WHITE);
        tabs.addTab("📋 Tüm Canlı Akış Günlüğü", new JScrollPane(txtLiveLog));

        return tabs;
    }

    private JTable createStyledTable(DefaultTableModel model) {
        JTable table = new JTable(model) {
            @Override
            public boolean isCellEditable(int row, int column) {
                return false;
            }
        };
        table.setRowHeight(24);
        table.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        table.getTableHeader().setFont(new Font("Segoe UI", Font.BOLD, 12));
        table.setShowGrid(true);
        table.setGridColor(new Color(230, 230, 230));

        // Saat ve Katman kolonlarını daraltıp ortala
        DefaultTableCellRenderer centerRenderer = new DefaultTableCellRenderer();
        centerRenderer.setHorizontalAlignment(JLabel.CENTER);

        if (table.getColumnCount() >= 2) {
            table.getColumnModel().getColumn(0).setPreferredWidth(100);
            table.getColumnModel().getColumn(0).setMaxWidth(110);
            table.getColumnModel().getColumn(0).setCellRenderer(centerRenderer);

            table.getColumnModel().getColumn(1).setPreferredWidth(100);
            table.getColumnModel().getColumn(1).setMaxWidth(110);
            table.getColumnModel().getColumn(1).setCellRenderer(centerRenderer);
        }
        return table;
    }

    // =========================================================================
    // 3. ALT PANEL (KONTROL VE TEST BUTONLARI)
    // =========================================================================

    private JPanel createControlBar() {
        JPanel bar = new JPanel(new FlowLayout(FlowLayout.CENTER, 8, 6));
        bar.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createEtchedBorder(), " Hızlı Test ve Simülasyon Kontrolleri ",
                TitledBorder.LEFT, TitledBorder.TOP, new Font("Segoe UI", Font.BOLD, 12)));

        JButton btnAccelerate = new JButton("🚗 Gaz Ver (ACCELERATE)");
        btnAccelerate.addActionListener(e -> {
            DriverCommand cmd = new DriverCommand("ACCELERATE", 40.0, "GUI-Pedal");
            if (app.isConnected()) {
                app.publishCoreJson("core.car.commands", cmd);
            } else {
                app.handleDriverCommand(cmd);
            }
        });

        JButton btnBrake = new JButton("🛑 Fren Yap (BRAKE)");
        btnBrake.addActionListener(e -> {
            DriverCommand cmd = new DriverCommand("BRAKE", 50.0, "GUI-Fren");
            if (app.isConnected()) {
                app.publishCoreJson("core.car.commands", cmd);
            } else {
                app.handleDriverCommand(cmd);
            }
        });

        JButton btnRadar = new JButton("⚠️ Radar Engel Testi (15m)");
        btnRadar.addActionListener(e -> {
            RadarSensorData radar = new RadarSensorData(15.0, -25.0, true);
            if (app.isConnected()) {
                app.publishCoreJson("core.car.sensors.radar", radar);
            } else {
                app.handleRadarSensor(radar);
            }
        });

        JButton btnFetchHistory = new JButton("🔍 Geçmiş Bakımı Çek (Direct Get)");
        btnFetchHistory.addActionListener(e -> new Thread(app::checkHistoricalMaintenance).start());

        JButton btnPublishAll = new JButton("📤 Manuel Yayınla (Tüm Konular)");
        btnPublishAll.addActionListener(e -> new Thread(app::publishAllMetrics).start());

        JButton btnClear = new JButton("🧹 Tabloları Temizle");
        btnClear.addActionListener(e -> {
            modelReceived.setRowCount(0);
            modelSent.setRowCount(0);
            modelHistorical.setRowCount(0);
            txtLiveLog.setText("");
        });

        bar.add(btnAccelerate);
        bar.add(btnBrake);
        bar.add(btnRadar);
        bar.add(btnFetchHistory);
        bar.add(btnPublishAll);
        bar.add(btnClear);
        return bar;
    }

    // =========================================================================
    // LISTENER GERİ ÇAĞIRIMLARI (EVENT CALLBACKS - THREAD SAFE)
    // =========================================================================

    @Override
    public void onMessageReceived(String topic, String layer, String content) {
        SwingUtilities.invokeLater(() -> {
            String time = LocalTime.now().format(timeFmt);
            modelReceived.addRow(new Object[]{time, layer, topic, content});
            scrollTableToBottom(tblReceived);

            appendLiveLog(String.format("📥 [ALINDI - %s] %s -> %s\n", layer, topic, content));
            updateDashboardCards();
        });
    }

    @Override
    public void onMessageSent(String topic, String layer, String content) {
        SwingUtilities.invokeLater(() -> {
            String time = LocalTime.now().format(timeFmt);
            modelSent.addRow(new Object[]{time, layer, topic, content});
            scrollTableToBottom(tblSent);

            appendLiveLog(String.format("📤 [ATILDI - %s] %s -> %s\n", layer, topic, content));
            updateDashboardCards();
        });
    }

    @Override
    public void onHistoricalRetrieved(String topic, long sequence, String timestamp, String content) {
        SwingUtilities.invokeLater(() -> {
            String time = LocalTime.now().format(timeFmt);
            modelHistorical.addRow(new Object[]{time, topic, sequence, timestamp, content});
            scrollTableToBottom(tblHistorical);

            appendLiveLog(String.format("📜 [GEÇMİŞTEN ÇEKİLDİ] %s (Sıra: %d, Zaman: %s) -> %s\n",
                    topic, sequence, timestamp, content));
            updateDashboardCards();
        });
    }

    @Override
    public void onStateChanged() {
        SwingUtilities.invokeLater(this::updateDashboardCards);
    }

    private void updateDashboardCards() {
        lblSpeed.setText(buildCardHtml("Hız", String.format("%.1f km/h", app.getCurrentSpeedKmh()), "#0066CC"));
        lblRpm.setText(buildCardHtml("Devir", String.format("%d RPM", app.getCurrentRpm()), "#336699"));
        lblGear.setText(buildCardHtml("Vites", String.valueOf(app.getCurrentGear()), "#46505F"));
        lblFuel.setText(buildCardHtml("Yakıt", String.format("%%%.1f", app.getFuelPercent()), "#228B22"));
        lblBattery.setText(buildCardHtml("Batarya", String.format("%%%.1f", app.getBatteryPercent()), "#2E8B57"));
        lblMode.setText(buildCardHtml("Sürüş Modu", app.getActiveProfile().mode, "#8A2BE2"));

        if (app.isConnected()) {
            lblConnection.setText(buildCardHtml("NATS Hattı", "🟢 BAĞLI", "#008800"));
        } else {
            lblConnection.setText(buildCardHtml("NATS Hattı", "🔴 ÇEVRİMDİŞİ", "#CC0000"));
        }

        if (app.isMaintenanceWarningActive()) {
            lblMaintenanceAlert.setText(" ⚠️ [DİKKAT] Geçmiş bakım kayıtlarına göre balata veya motor yağı kritik seviyede!");
            lblMaintenanceAlert.setForeground(new Color(200, 30, 30));
        } else {
            lblMaintenanceAlert.setText(" ✅ Araç sağlığı ve servis periyodu uygun durumda.");
            lblMaintenanceAlert.setForeground(new Color(34, 139, 34));
        }
    }

    private String buildCardHtml(String title, String val, String hexColor) {
        return String.format("<html><center><font size='2' color='#666666'>%s</font><br/><b><font size='4' color='%s'>%s</font></b></center></html>",
                title, hexColor, val);
    }

    private void appendLiveLog(String line) {
        txtLiveLog.append(line);
        // Çok uzamaması için sınırlama (son 2000 satır)
        if (txtLiveLog.getLineCount() > 2000) {
            try {
                int end = txtLiveLog.getLineEndOffset(100);
                txtLiveLog.replaceRange("", 0, end);
            } catch (Exception ignored) {}
        }
        txtLiveLog.setCaretPosition(txtLiveLog.getDocument().getLength());
    }

    private void scrollTableToBottom(JTable table) {
        if (table.getRowCount() > 0) {
            table.scrollRectToVisible(table.getCellRect(table.getRowCount() - 1, 0, true));
        }
    }

    private String toHex(Color c) {
        return String.format("#%02x%02x%02x", c.getRed(), c.getGreen(), c.getBlue());
    }
}
