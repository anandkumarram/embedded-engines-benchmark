package org.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

public class PostgresImageBenchmark {
    public static class ResultSummary {
        public final long bytesProcessed;
        public final long itemsProcessed;
        public final long millis;

        public ResultSummary(long bytesProcessed, long itemsProcessed, long millis) {
            this.bytesProcessed = bytesProcessed;
            this.itemsProcessed = itemsProcessed;
            this.millis = millis;
        }
    }

    public static void main(String[] args) throws Exception {
        int numImages = getIntArg(args, 0, 100000);
        int pixelsPerSide = getIntArg(args, 1, 128);
        String imagesDirPath = getStringArg(args, 2, "images");
        String jdbcUrl = getStringArg(args, 3, "jdbc:postgresql://127.0.0.1:15432/bench");
        String user = getStringArg(args, 4, "bench");
        String pass = getStringArg(args, 5, "bench");
        int threads = getIntArg(args, 6, Math.max(2, Runtime.getRuntime().availableProcessors() * 2));
        int batchSize = getIntArg(args, 7, 10000);

        File imagesDir = new File(imagesDirPath);
        if (!imagesDir.exists() || Objects.requireNonNull(imagesDir.listFiles()).length < numImages) {
            System.out.println("Generating images to " + imagesDir.getAbsolutePath());
            ImageGenerator.generateImages(numImages, pixelsPerSide, imagesDir);
        }

        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(jdbcUrl);
        cfg.setUsername(user);
        cfg.setPassword(pass);
        cfg.setMaximumPoolSize(Math.max(threads, 8));
        cfg.setAutoCommit(false);
        try (HikariDataSource ds = new HikariDataSource(cfg)) {
            try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE IF NOT EXISTS images (id TEXT PRIMARY KEY, data BYTEA)");
                c.commit();
            }

            List<File> files = listPngFiles(imagesDir, numImages);

            ResultSummary write = runSequentialBatchesWithParallelItems(files, batchSize, threads, f -> {
                byte[] bytes = Files.readAllBytes(f.toPath());
                try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement("INSERT INTO images (id, data) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data")) {
                    ps.setString(1, f.getName());
                    ps.setBytes(2, bytes);
                    ps.executeUpdate();
                    c.commit();
                }
                return (long) bytes.length;
            }, "pg-write");

            System.out.printf("PG Write: items=%d, size=%.2f MB, time=%d ms%n", write.itemsProcessed, write.bytesProcessed / (1024.0 * 1024.0), write.millis);

            ResultSummary read = runSequentialBatchesWithParallelItems(files, batchSize, threads, f -> {
                try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement("SELECT data FROM images WHERE id = ?")) {
                    ps.setString(1, f.getName());
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            byte[] data = rs.getBytes(1);
                            return (long) data.length;
                        }
                    }
                }
                return 0L;
            }, "pg-read");

            System.out.printf("PG Read: items=%d, size=%.2f MB, time=%d ms%n", read.itemsProcessed, read.bytesProcessed / (1024.0 * 1024.0), read.millis);
        }
    }

    private interface FileJob { long apply(File file) throws Exception; }

    private static ResultSummary runSequentialBatchesWithParallelItems(List<File> files, int batchSize, int threads, FileJob perItemJob, String opName) throws InterruptedException, ExecutionException {
        List<List<File>> batches = partition(files, batchSize);
        long totalBytes = 0L;
        Instant globalStart = Instant.now();
        long totalItems = files.size();
        for (int i = 0; i < batches.size(); i++) {
            List<File> batch = batches.get(i);
            long start = System.currentTimeMillis();
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Callable<Long>> tasks = new ArrayList<>();
            for (File f : batch) {
                tasks.add(() -> perItemJob.apply(f));
            }
            List<Future<Long>> futures = pool.invokeAll(tasks);
            pool.shutdown();
            long bytes = 0L;
            for (Future<Long> fut : futures) {
                bytes += fut.get();
            }
            long end = System.currentTimeMillis();
            totalBytes += bytes;
            synchronized (System.out) {
                System.out.printf("%s batch #%d finished by %s: items=%d, start=%d, end=%d, time=%d ms%n",
                        opName, i + 1, Thread.currentThread().getName(), batch.size(), start, end, (end - start));
                System.out.flush();
            }
        }
        long millis = Duration.between(globalStart, Instant.now()).toMillis();
        return new ResultSummary(totalBytes, totalItems, millis);
    }

    private static List<List<File>> partition(List<File> list, int size) {
        if (size <= 0) size = Integer.MAX_VALUE;
        List<List<File>> out = new ArrayList<>();
        int n = list.size();
        for (int i = 0; i < n; i += size) {
            out.add(list.subList(i, Math.min(n, i + size)));
        }
        return out;
    }

    private static List<File> listPngFiles(File dir, int limit) {
        File[] all = dir.listFiles((d, name) -> name.endsWith(".png"));
        if (all == null) return List.of();
        Arrays.sort(all);
        if (limit < all.length) {
            return Arrays.asList(Arrays.copyOf(all, limit));
        }
        return Arrays.asList(all);
    }

    private static int getIntArg(String[] args, int idx, int def) {
        if (args.length > idx) {
            try { return Integer.parseInt(args[idx]); } catch (Exception ignored) {}
        }
        return def;
    }

    private static String getStringArg(String[] args, int idx, String def) {
        if (args.length > idx) {
            return args[idx];
        }
        return def;
    }
}


