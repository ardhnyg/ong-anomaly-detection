-- 02. Deteksi anomali getaran per aset dengan ML_DETECT_ANOMALIES (ARIMA, built-in Confluent Cloud).
-- Tidak perlu model atau endpoint eksternal. Model dilatih terus dari stream.
-- Di SQL workspace: satu statement per cell.
--
-- Alur: pump_telemetry -> rata-rata 5 detik per aset -> ML_DETECT_ANOMALIES -> pump_vibration_anomalies
--
-- Konfigurasi untuk demo:
--   minTrainingSize 24 = 24 window x 5 detik = sekitar 2 menit sebelum hasil keluar (default 128).
--   updateInterval 5   = model dilatih ulang tiap 5 window.
--   confidencePercentage 99 = batas atas/bawah di level 99%.

-- CELL 1. Baca hanya data baru, supaya record dari connector degradasi/gagal yang sempat
-- keluar sebelum di-pause tidak ikut masuk training. Berlaku untuk query baru di tabel ini.
ALTER TABLE pump_telemetry SET ('scan.startup.mode' = 'latest-offset');

-- CELL 2. Cek dulu dengan EXPLAIN (gratis, tidak memakai CFU). Kalau PARTITION BY ditolak,
-- buat satu statement per aset dengan WHERE asset_id = '...'.
EXPLAIN
SELECT
  asset_id,
  window_time,
  ML_DETECT_ANOMALIES(avg_vibration, window_time,
    JSON_OBJECT('minTrainingSize' VALUE 24, 'maxTrainingSize' VALUE 360,
                'confidencePercentage' VALUE 99.0, 'updateInterval' VALUE 5, 'horizon' VALUE 1)
  ) OVER (PARTITION BY asset_id ORDER BY window_time
          RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS anomaly
FROM (
  SELECT asset_id, window_start, window_end, window_time, AVG(vibration_rms_mm_s) AS avg_vibration
  FROM TABLE(TUMBLE(TABLE pump_telemetry, DESCRIPTOR($rowtime), INTERVAL '5' SECOND))
  GROUP BY asset_id, window_start, window_end, window_time
);

-- CELL 3. Statement yang jalan terus. Membuat topic dan tabel pump_vibration_anomalies.
CREATE TABLE pump_vibration_anomalies AS
SELECT
  asset_id,
  window_time                        AS ts,
  ROUND(avg_vibration, 3)            AS avg_vibration,
  ROUND(avg_bearing_temp, 2)         AS avg_bearing_temp,
  ROUND(anomaly.forecast_value, 3)   AS forecast_vibration,
  ROUND(anomaly.lower_bound, 3)      AS lower_bound,
  ROUND(anomaly.upper_bound, 3)      AS upper_bound,
  anomaly.is_anomaly                 AS is_anomaly
FROM (
  SELECT
    asset_id,
    window_time,
    avg_vibration,
    avg_bearing_temp,
    ML_DETECT_ANOMALIES(
      avg_vibration,
      window_time,
      JSON_OBJECT(
        'minTrainingSize'      VALUE 24,
        'maxTrainingSize'      VALUE 360,
        'confidencePercentage' VALUE 99.0,
        'updateInterval'       VALUE 5,
        'horizon'              VALUE 1
      )
    ) OVER (
      PARTITION BY asset_id
      ORDER BY window_time
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS anomaly
  FROM (
    SELECT
      asset_id,
      window_start,
      window_end,
      window_time,
      AVG(vibration_rms_mm_s) AS avg_vibration,
      AVG(bearing_temp_c)     AS avg_bearing_temp
    FROM TABLE(TUMBLE(TABLE pump_telemetry, DESCRIPTOR($rowtime), INTERVAL '5' SECOND))
    GROUP BY asset_id, window_start, window_end, window_time
  )
);

-- CELL 4. Lihat hasil. is_anomaly NULL selama sekitar 2 menit pertama (masa training).
SELECT * FROM pump_vibration_anomalies;
