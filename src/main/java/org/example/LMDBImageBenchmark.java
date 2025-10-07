package org.example;

import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.Txn;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.lmdbjava.Env.create;

public class LMDBImageBenchmark {
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
        String lmdbDirPath = getStringArg(args, 3, "./lmdbdata");
        int threads = getIntArg(args, 4, Math.max(2, Runtime.getRuntime().availableProcessors()));
        int batchSize = getIntArg(args, 5, 10000);

        File imagesDir = new File(imagesDirPath);
        if (!imagesDir.exists() || Objects.requireNonNull(imagesDir.listFiles()).length < numImages) {
            System.out.println("Generating images to " + imagesDir.getAbsolutePath());
            ImageGenerator.generateImages(numImages, pixelsPerSide, imagesDir);
        }
        System.out.println(" Image Generation completed !!");
        File lmdbDir = new File(lmdbDirPath);
        if (!lmdbDir.exists()) lmdbDir.mkdirs();

        try (Env<ByteBuffer> env = create()
                .setMapSize(10L * 1024 * 1024 * 1024) // 10GB
                .setMaxDbs(1)
                .open(lmdbDir)) {

            Dbi<ByteBuffer> db = env.openDbi("images", DbiFlags.MDB_CREATE);

            List<File> files = listPngFiles(imagesDir, numImages);

            ResultSummary write = runSequentialBatchesWithParallelItems(files, batchSize, threads, f -> {
                byte[] bytes = Files.readAllBytes(f.toPath());
                ByteBuffer key = ByteBuffer.allocateDirect(f.getName().getBytes(UTF_8).length);
                key.put(f.getName().getBytes(UTF_8)).flip();
                ByteBuffer val = ByteBuffer.allocateDirect(bytes.length);
                val.put(bytes).flip();
                try (Txn<ByteBuffer> txn = env.txnWrite()) {
                    db.put(txn, key, val);
                    txn.commit();
                }
                return (long) bytes.length;
            }, "lmdb-write");

            System.out.printf("LMDB Write: items=%d, size=%.2f MB, time=%d ms%n", write.itemsProcessed, write.bytesProcessed / (1024.0 * 1024.0), write.millis);

            ResultSummary read = runSequentialBatchesWithParallelItems(files, batchSize, threads, f -> {
                ByteBuffer key = ByteBuffer.allocateDirect(f.getName().getBytes(UTF_8).length);
                key.put(f.getName().getBytes(UTF_8)).flip();
                try (Txn<ByteBuffer> txn = env.txnRead()) {
                    ByteBuffer found = db.get(txn, key);
                    if (found == null) return 0L;
                    return (long) found.remaining();
                }
            }, "lmdb-read");

            System.out.printf("LMDB Read: items=%d, size=%.2f MB, time=%d ms%n", read.itemsProcessed, read.bytesProcessed / (1024.0 * 1024.0), read.millis);
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


