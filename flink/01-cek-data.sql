-- 01. Cek data dari Datagen sudah masuk.
-- Tabel pump_telemetry otomatis ada begitu topic dan schema Avro terdaftar.
-- $rowtime = timestamp record Kafka, dipakai sebagai event time.

DESCRIBE pump_telemetry;

SELECT asset_id, scenario, vibration_rms_mm_s, bearing_temp_c, motor_current_a, $rowtime
FROM pump_telemetry;

-- Rata-rata per aset per 5 detik. Ini input untuk ML_DETECT_ANOMALIES di file 02.
SELECT
  asset_id,
  window_time,
  COUNT(*)                      AS n,
  ROUND(AVG(vibration_rms_mm_s), 2) AS avg_vibration
FROM TABLE(TUMBLE(TABLE pump_telemetry, DESCRIPTOR($rowtime), INTERVAL '5' SECOND))
GROUP BY asset_id, window_start, window_end, window_time;
