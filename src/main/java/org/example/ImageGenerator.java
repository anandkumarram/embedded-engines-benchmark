package org.example;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.Random;

public class ImageGenerator {
    public static void generateImages(int numImages, int pixelsPerSide, File outputDir) throws IOException {
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new IOException("Failed to create directory: " + outputDir.getAbsolutePath());
        }
        Random random = new Random(42);
        for (int i = 0; i < numImages; i++) {
            BufferedImage image = new BufferedImage(pixelsPerSide, pixelsPerSide, BufferedImage.TYPE_INT_RGB);
            Graphics2D g2d = image.createGraphics();
            // Fill background with random color to avoid compressibility bias
            g2d.setColor(new Color(random.nextInt(256), random.nextInt(256), random.nextInt(256)));
            g2d.fillRect(0, 0, pixelsPerSide, pixelsPerSide);
            // Draw a few random rectangles
            for (int r = 0; r < 10; r++) {
                g2d.setColor(new Color(random.nextInt(256), random.nextInt(256), random.nextInt(256)));
                int w = 10 + random.nextInt(Math.max(1, pixelsPerSide - 10));
                int h = 10 + random.nextInt(Math.max(1, pixelsPerSide - 10));
                int x = random.nextInt(Math.max(1, pixelsPerSide - w));
                int y = random.nextInt(Math.max(1, pixelsPerSide - h));
                g2d.fillRect(x, y, w, h);
            }
            g2d.dispose();
            File out = new File(outputDir, String.format("img_%06d.png", i));
            ImageIO.write(image, "png", out);
        }
    }

    public static void generateImagesBatched(int numImages, int pixelsPerSide, File outputDir, int batchSize) throws IOException {
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new IOException("Failed to create directory: " + outputDir.getAbsolutePath());
        }
        Random random = new Random(42);
        int generated = 0;
        long batchStart = System.currentTimeMillis();
        for (int i = 0; i < numImages; i++) {
            BufferedImage image = new BufferedImage(pixelsPerSide, pixelsPerSide, BufferedImage.TYPE_INT_RGB);
            Graphics2D g2d = image.createGraphics();
            g2d.setColor(new Color(random.nextInt(256), random.nextInt(256), random.nextInt(256)));
            g2d.fillRect(0, 0, pixelsPerSide, pixelsPerSide);
            for (int r = 0; r < 10; r++) {
                g2d.setColor(new Color(random.nextInt(256), random.nextInt(256), random.nextInt(256)));
                int w = 10 + random.nextInt(Math.max(1, pixelsPerSide - 10));
                int h = 10 + random.nextInt(Math.max(1, pixelsPerSide - 10));
                int x = random.nextInt(Math.max(1, pixelsPerSide - w));
                int y = random.nextInt(Math.max(1, pixelsPerSide - h));
                g2d.fillRect(x, y, w, h);
            }
            g2d.dispose();
            File out = new File(outputDir, String.format("img_%06d.png", i));
            ImageIO.write(image, "png", out);

            generated++;
            if (generated % Math.max(1, batchSize) == 0 || generated == numImages) {
                long now = System.currentTimeMillis();
                long ms = now - batchStart;
                double itemsPerSec = ms == 0 ? 0.0 : (generated % batchSize == 0 ? batchSize : generated % batchSize) / (ms / 1000.0);
                System.out.printf("Gen batch: items=%d, time=%d ms, items/s=%.2f, total=%d/%d%n",
                        (generated % batchSize == 0 ? batchSize : generated % batchSize), ms, itemsPerSec, generated, numImages);
                batchStart = now;
            }
        }
    }
}



