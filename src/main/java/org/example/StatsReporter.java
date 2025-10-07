package org.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class StatsReporter implements AutoCloseable {
    private final HikariDataSource dataSource;

    public StatsReporter(String jdbcUrl, String user, String pass) throws Exception {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(jdbcUrl);
        cfg.setUsername(user);
        cfg.setPassword(pass);
        cfg.setMaximumPoolSize(4);
        cfg.setAutoCommit(true);
        this.dataSource = new HikariDataSource(cfg);
        ensureTable();
    }

    private void ensureTable() throws Exception {
        String ddl = "CREATE TABLE IF NOT EXISTS bench_stats (" +
                "id BIGSERIAL PRIMARY KEY, " +
                "ts TIMESTAMPTZ DEFAULT now(), " +
                "db TEXT NOT NULL, " +
                "op TEXT NOT NULL, " +
                "batch_no INT, " +
                "items BIGINT, " +
                "bytes BIGINT, " +
                "millis BIGINT, " +
                "threads INT, " +
                "batch_size INT, " +
                "cpu INT, " +
                "heap_mb BIGINT, " +
                "note TEXT)";
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.executeUpdate(ddl);
        }
    }

    public void recordBatch(String db, String op, int batchNo, long items, long bytes, long millis,
                            int threads, int batchSize, int cpuCores, long heapMb, String note) {
        String sql = "INSERT INTO bench_stats (db, op, batch_no, items, bytes, millis, threads, batch_size, cpu, heap_mb, note) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, db);
            ps.setString(2, op);
            ps.setInt(3, batchNo);
            ps.setLong(4, items);
            ps.setLong(5, bytes);
            ps.setLong(6, millis);
            ps.setInt(7, threads);
            ps.setInt(8, batchSize);
            ps.setInt(9, cpuCores);
            ps.setLong(10, heapMb);
            ps.setString(11, note);
            ps.executeUpdate();
        } catch (Exception ignored) {}
    }

    public void recordTotal(String db, String op, long items, long bytes, long millis,
                            int threads, int batchSize, int cpuCores, long heapMb, String note) {
        recordBatch(db, op, 0, items, bytes, millis, threads, batchSize, cpuCores, heapMb, note);
    }

    @Override
    public void close() {
        if (dataSource != null) dataSource.close();
    }
}


