-- 03. Topic alert untuk CMMS / SAP PM / notifikasi.
-- Hanya anomali ke atas (getaran di atas upper_bound). Anomali ke bawah biasanya pompa berhenti,
-- itu ditangani event status, tidak masuk alert maintenance.

CREATE TABLE pump_maintenance_alerts AS
SELECT
  asset_id,
  CASE
    WHEN asset_id LIKE 'ESP-WLA-%' THEN 'Wellpad A'
    WHEN asset_id LIKE 'ESP-WLB-%' THEN 'Wellpad B'
    WHEN asset_id LIKE 'CMP-JMB-%' THEN 'Stasiun kompresor (contoh)'
    ELSE 'Tidak diketahui'
  END                                   AS site,
  CASE WHEN asset_id LIKE 'ESP-%' THEN 'ESP' ELSE 'Kompresor' END AS asset_type,
  ts,
  avg_vibration,
  upper_bound,
  ROUND(avg_vibration - upper_bound, 3) AS excess_mm_s,
  CASE
    WHEN avg_vibration >= 7.1 THEN 'HIGH'    -- contoh batas alarm vendor
    WHEN avg_vibration >= 4.5 THEN 'MEDIUM'
    ELSE 'LOW'
  END                                   AS severity,
  'Getaran di atas batas atas model ARIMA (99%)' AS reason
FROM pump_vibration_anomalies
WHERE is_anomaly = TRUE
  AND avg_vibration > upper_bound;

SELECT * FROM pump_maintenance_alerts;
