package org.example;

import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

public class TiKVImageBenchmark {
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
        String pdAddress = getStringArg(args, 3, "127.0.0.1:2379");
        int threads = getIntArg(args, 4, Math.max(2, Runtime.getRuntime().availableProcessors() * 2));
        int batchSize = getIntArg(args, 5, 10000);

        File imagesDir = new File(imagesDirPath);
        if (!imagesDir.exists() || Objects.requireNonNull(imagesDir.listFiles()).length < numImages) {
            System.out.println("Generating images to " + imagesDir.getAbsolutePath());
            ImageGenerator.generateImages(numImages, pixelsPerSide, imagesDir);
        }

        int cpuCores = Runtime.getRuntime().availableProcessors();
        long maxHeapMb = Runtime.getRuntime().maxMemory() / (1024 * 1024);
        System.out.printf(
                "Benchmark config: images=%d, size=%dx%d, threads=%d, batch=%d, cpus=%d, heapMax=%dMB, pd=%s%n",
                numImages, pixelsPerSide, pixelsPerSide, threads, batchSize, cpuCores, maxHeapMb, pdAddress);

        TiConfiguration conf = TiConfiguration.createRawDefault(pdAddress);
        conf.setEnableAtomicForCAS(true);
        try (TiSession session = TiSession.create(conf); RawKVClient client = session.createRawClient()) {
            List<File> files = listPngFiles(imagesDir, numImages);
            long plannedBytes = 0L;
            for (File f : files) {
                try { plannedBytes += Files.size(f.toPath()); } catch (Exception ignored) {}
            }
            double plannedMb = plannedBytes / (1024.0 * 1024.0);
            double avgImgKb = files.isEmpty() ? 0.0 : (plannedBytes / 1024.0) / files.size();
            System.out.printf("Planned workload: files=%d, total=%.2f MB, avg=%.2f KB/image%n",
                    files.size(), plannedMb, avgImgKb);

            ResultSummary write = runSequentialBatchesWithParallelItems(
                    files, batchSize, threads, file -> {
                        byte[] bytes = Files.readAllBytes(file.toPath());
                        ByteString key = ByteString.copyFromUtf8(file.getName());
                        client.put(key, ByteString.copyFrom(bytes));
                        return (long) bytes.length;
                    }, "write");
            double writeMb = write.bytesProcessed / (1024.0 * 1024.0);
            double writeThroughput = writeMb / (write.millis / 1000.0);
            double writeItemsPerSec = write.itemsProcessed / (write.millis / 1000.0);
            double writeAvgMsPerItem = write.itemsProcessed == 0 ? 0.0 : (double) write.millis / write.itemsProcessed;
            System.out.printf(
                    "Write: items=%d, size=%.2f MB, time=%d ms, MB/s=%.2f, items/s=%.2f, avg=%.2f ms/item, threads=%d%n",
                    write.itemsProcessed, writeMb, write.millis, writeThroughput, writeItemsPerSec, writeAvgMsPerItem, threads);

            ResultSummary read = runSequentialBatchesWithParallelItems(
                    files, batchSize, threads, file -> {
                        ByteString key = ByteString.copyFromUtf8(file.getName());
                        var valueOpt = client.get(key);
                        return valueOpt.map(v -> (long) v.size()).orElse(0L);
                    }, "read");
            double readMb = read.bytesProcessed / (1024.0 * 1024.0);
            double readThroughput = readMb / (read.millis / 1000.0);
            double readItemsPerSec = read.itemsProcessed / (read.millis / 1000.0);
            double readAvgMsPerItem = read.itemsProcessed == 0 ? 0.0 : (double) read.millis / read.itemsProcessed;
            System.out.printf(
                    "Read: items=%d, size=%.2f MB, time=%d ms, MB/s=%.2f, items/s=%.2f, avg=%.2f ms/item, threads=%d%n",
                    read.itemsProcessed, readMb, read.millis, readThroughput, readItemsPerSec, readAvgMsPerItem, threads);
        }
    }

    private interface FileJob {
        long apply(File file) throws Exception;
    }

    private interface BatchFileJob {
        long apply(List<File> batch) throws Exception;
    }

    private static ResultSummary runParallel(List<File> files, int threads, FileJob job) throws InterruptedException, ExecutionException {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Callable<Long>> tasks = new ArrayList<>();
        for (File f : files) {
            tasks.add(() -> job.apply(f));
        }
        Instant start = Instant.now();
        List<Future<Long>> futures = pool.invokeAll(tasks);
        pool.shutdown();
        long bytes = 0;
        for (Future<Long> fut : futures) {
            try {
                bytes += fut.get();
            } catch (ExecutionException ee) {
                throw ee;
            }
        }
        long millis = Duration.between(start, Instant.now()).toMillis();
        return new ResultSummary(bytes, files.size(), millis);
    }

    private static ResultSummary runParallelBatches(List<File> files, int batchSize, int threads, BatchFileJob job, String opName) throws InterruptedException, ExecutionException {
        List<List<File>> batches = partition(files, batchSize);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Callable<Long>> tasks = new ArrayList<>();
        for (int i = 0; i < batches.size(); i++) {
            final int batchNo = i + 1;
            final List<File> batch = batches.get(i);
            tasks.add(() -> {
                long start = System.currentTimeMillis();
                String thread = Thread.currentThread().getName();
                long bytes = job.apply(batch);
                long end = System.currentTimeMillis();
                long ms = end - start;
                synchronized (System.out) {
                    System.out.printf(
                            "%s batch #%d finished by %s: items=%d, start=%d, end=%d, time=%d ms%n",
                            opName, batchNo, thread, batch.size(), start, end, ms);
                    System.out.flush();
                }
                return bytes;
            });
        }
        Instant start = Instant.now();
        List<Future<Long>> futures = pool.invokeAll(tasks);
        pool.shutdown();
        long bytes = 0;
        for (Future<Long> fut : futures) {
            bytes += fut.get();
        }
        long millis = Duration.between(start, Instant.now()).toMillis();
        return new ResultSummary(bytes, files.size(), millis);
    }

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


