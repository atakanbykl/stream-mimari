package com.stream.base.config;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;

/**
 * Yapılandırma hatalarında kullanıcıyı modal bir uyarı penceresiyle bilgilendiren
 * ve kullanıcı "Tamam" butonuna bastıktan sonra uygulamayı güvenle sonlandıran yardımcı sınıf.
 */
public class ConfigErrorHandler {

    /**
     * Test ortamlarında System.exit çağrısının test yürütücüsünü öldürmesini engellemek için bayrak.
     */
    public static boolean exitOnFatalError = true;

    /**
     * Test ortamlarında modal diyalog veya bloklayıcı okumayı atlamak için bayrak.
     */
    public static boolean suppressDialogForTesting = false;

    /**
     * Kullanıcıya modal uyarı penceresi gösterir ve onaylandıktan sonra uygulamayı kapatır.
     * 
     * @param title Hata penceresi başlığı
     * @param errorMessage Kullanıcıya gösterilecek detaylı hata mesajı
     */
    public static void showFatalErrorAndExit(String title, String errorMessage) {
        System.err.println("=================================================================");
        System.err.println(">> KRİTİK YAPILANDIRMA HATASI: " + title + " <<");
        System.err.println("=================================================================");
        System.err.println(errorMessage);
        System.err.println("=================================================================");

        // Test modunda veya çıkış iptal edilmişse diyalog açmadan istisna fırlat
        if (!exitOnFatalError || suppressDialogForTesting) {
            throw new ConfigValidationException(title + ": " + errorMessage);
        }

        // Masaüstü ortamında (GUI destekli) modal pop-up göster
        if (!GraphicsEnvironment.isHeadless()) {
            try {
                UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
            } catch (Exception ignored) {}

            JTextArea txt = new JTextArea(errorMessage + "\n\n(Uygulama sonlandırılacaktır. Çıkmak için 'Tamam' butonuna tıklayınız.)");
            txt.setEditable(false);
            txt.setFont(new Font("Segoe UI", Font.PLAIN, 13));
            txt.setBackground(new Color(248, 248, 250));
            txt.setBorder(new EmptyBorder(10, 10, 10, 10));
            txt.setLineWrap(true);
            txt.setWrapStyleWord(true);

            JScrollPane scroll = new JScrollPane(txt);
            scroll.setPreferredSize(new Dimension(560, 240));

            // Bloklayıcı modal diyalog: Kullanıcı 'Tamam' diyene kadar bekler
            JOptionPane.showMessageDialog(
                    null,
                    scroll,
                    title,
                    JOptionPane.ERROR_MESSAGE
            );
        } else {
            System.err.println("Uygulamayı kapatmak için ENTER tuşuna basınız...");
            try {
                System.in.read();
            } catch (Exception ignored) {}
        }

        if (exitOnFatalError) {
            System.exit(1);
        } else {
            throw new ConfigValidationException(title + ": " + errorMessage);
        }
    }
}
