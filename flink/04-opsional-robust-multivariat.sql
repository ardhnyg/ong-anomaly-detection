-- 04. OPSIONAL. Deteksi multivariat (getaran + suhu bearing + arus motor) dengan
-- ML_DETECT_ANOMALIES_ROBUST. Status: Open Preview, hanya untuk evaluasi, bukan produksi.
-- Docs belum mencantumkan field hasilnya, jadi SELECT dulu dan cek bentuk kolom anomaly
-- sebelum membuat CTAS.

SELECT
  asset_id,
  window_time,
  avg_vibration,
  avg_bearing_temp,
  avg_motor_current,
  ML_DETECT_ANOMALIES_ROBUST(
    ROW(avg_vibration, avg_bearing_temp, avg_motor_current),
    window_time,
    JSON_OBJECT(
      'window'    VALUE 60,
      'threshold' VALUE 3.5
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
    AVG(bearing_temp_c)     AS avg_bearing_temp,
    AVG(motor_current_a)    AS avg_motor_current
  FROM TABLE(TUMBLE(TABLE pump_telemetry, DESCRIPTOR($rowtime), INTERVAL '5' SECOND))
  GROUP BY asset_id, window_start, window_end, window_time
);
