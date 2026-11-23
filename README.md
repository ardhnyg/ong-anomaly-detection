# Demo Predictive Maintenance di Confluent Cloud: Datagen + Flink ML_DETECT_ANOMALIES

Skenario 03 dari deck Pertamina Digital Hub. Semua langkah lewat Confluent Cloud Console, tanpa CLI.

Datagen Source connector (fully managed) menghasilkan telemetri pompa ESP dan kompresor ke satu topic. Flink di SQL workspace merata-rata tiap 5 detik per aset, lalu `ML_DETECT_ANOMALIES` (ARIMA bawaan Confluent Cloud, tanpa model atau endpoint eksternal) menandai getaran yang keluar dari batas prediksi.

```
Datagen normal (6 aset)     ─┐
Datagen degradasi (1 aset)  ─┼─> pump_telemetry ─> Flink: AVG 5 detik ─> ML_DETECT_ANOMALIES ─> pump_vibration_anomalies ─> pump_maintenance_alerts
Datagen gagal (1 aset)      ─┘
```

## Isi folder

| File | Fungsi |
|---|---|
| `datagen/paste-schema-normal.txt` | Schema satu baris untuk di-paste ke connector normal (6 aset, getaran 2,0–3,2 mm/s) |
| `datagen/paste-schema-degradasi.txt` | ESP-WLA-02, getaran 3,6–4,6 mm/s, suhu bearing 80–86 °C |
| `datagen/paste-schema-gagal.txt` | ESP-WLA-02, getaran 6,5–9,0 mm/s, suhu bearing 90–100 °C |
| `datagen/pump-*.avsc` | Versi rapi (multi-baris) dari schema yang sama, untuk dibaca atau diedit |
| `flink/01-cek-data.sql` | Cek data masuk dan rata-rata per window |
| `flink/02-deteksi-anomali-arima.sql` | `ML_DETECT_ANOMALIES` → tabel `pump_vibration_anomalies` |
| `flink/03-alert-maintenance.sql` | Tabel `pump_maintenance_alerts`, hanya anomali ke atas |
| `flink/04-opsional-robust-multivariat.sql` | `ML_DETECT_ANOMALIES_ROBUST` multivariat (Open Preview) |
| `opsional-cli/build-configs.sh` | Kalau nanti mau pakai Confluent CLI: membuat JSON config connector |

Semua angka dan ID aset adalah contoh.

## Prasyarat di Confluent Cloud

- Environment dengan Stream Governance (Schema Registry) aktif.
- Satu Kafka cluster. Basic sudah cukup untuk demo.
- Flink compute pool di cloud provider dan region yang sama dengan cluster.
- API key Kafka atau service account untuk connector.

## Persiapan (sebelum demo, sekitar 20 menit)

### 1. Buat topic
Cluster → **Topics** → **Add topic**. Nama `pump_telemetry`, 6 partisi.

### 2. Buat connector normal
Cluster → **Connectors** → **Add connector** → **Datagen Source**.

| Isian | Nilai |
|---|---|
| Topic | `pump_telemetry` |
| Kafka credentials | API key atau service account |
| Output record value format | `AVRO` |
| Schema | Pilih opsi schema sendiri (Schema string), paste isi `paste-schema-normal.txt` |
| Max interval between messages (ms) | `500` |
| Schema keyfield | `asset_id` (di advanced configuration) |
| Tasks | `1` |
| Nama connector | `pdm-datagen-normal` |

Label di UI bisa sedikit berbeda. Nama property-nya: `schema.string`, `schema.keyfield`, `max.interval`, `output.data.format`.

Setelah status Running, cek tab **Messages** di topic `pump_telemetry`.

### 3. Siapkan connector degradasi dan gagal, lalu pause
Ulangi langkah 2 dua kali:
- `pdm-datagen-degradasi`: paste `paste-schema-degradasi.txt`, max interval `400`.
- `pdm-datagen-gagal`: paste `paste-schema-gagal.txt`, max interval `400`.

Begitu masing-masing Running, langsung **Pause**. Dengan begitu saat demo cukup klik **Resume**, tanpa menunggu provisioning connector baru. Record yang sempat keluar sebelum pause tidak ikut training karena langkah 4 membaca dari offset terbaru.

### 4. Jalankan Flink
Environment → **Flink** → **Open SQL workspace**. Pilih catalog = environment, database = cluster.

Jalankan per cell, berurutan:
1. `01-cek-data.sql`: pastikan data masuk.
2. `02-deteksi-anomali-arima.sql`: cell 1 (`ALTER TABLE ... latest-offset`), cell 2 (`EXPLAIN`), cell 3 (`CREATE TABLE ... AS`).
3. `03-alert-maintenance.sql`: `CREATE TABLE ... AS`.

Statement `CREATE TABLE ... AS` berjalan terus dan terlihat di **Flink → Statements**.

### 5. Biarkan baseline terbentuk
Hasil mulai keluar sekitar 2 menit setelah statement jalan (`minTrainingSize` 24 window × 5 detik). Biarkan kondisi normal 5–10 menit sebelum demo.

## Alur demo

Buka dua cell di workspace: `SELECT * FROM pump_vibration_anomalies WHERE asset_id = 'ESP-WLA-02';` dan `SELECT * FROM pump_maintenance_alerts;`.

| Menit | Aksi di Console | Yang terlihat |
|---|---|---|
| 0 | Hanya `pdm-datagen-normal` jalan | `is_anomaly = FALSE`, getaran ESP-WLA-02 di sekitar 2,6 mm/s |
| 1 | **Resume** `pdm-datagen-degradasi` | Rata-rata getaran ESP-WLA-02 naik ke ±3,9 mm/s, `is_anomaly = TRUE`, alert severity LOW |
| 4 | **Pause** degradasi, **Resume** `pdm-datagen-gagal` | Getaran ±7–8 mm/s, alert MEDIUM/HIGH |
| 6 | **Pause** gagal | ESP-WLA-02 kembali ke pola normal |

Poin cerita ke customer: model menandai perubahan di menit ke-1, saat getaran masih jauh di bawah batas alarm 7,1 mm/s.

Kenapa angkanya begitu: connector normal mengirim sekitar 4 pesan/detik untuk 6 aset, sedangkan connector degradasi sekitar 5 pesan/detik untuk ESP-WLA-02 saja. Rata-rata 5 detik ESP-WLA-02 jadi didominasi nilai tinggi.

## Setelah demo

Connector Datagen ditagih per jam task, dan statement Flink memakai CFU.
1. **Connectors**: hapus ketiga connector `pdm-datagen-*`.
2. **Flink → Statements**: stop statement `CREATE TABLE ... AS` dari file 02 dan 03.
3. Topic `pump_telemetry`, `pump_vibration_anomalies`, `pump_maintenance_alerts` bisa dihapus dari menu Topics. Hati-hati: `DROP TABLE` di Flink juga menghapus topic beserta datanya.

## Perilaku yang perlu diketahui

- **Datagen tidak bisa membuat tren.** Setiap record dibangkitkan acak dan independen, karena itu kondisi abnormal memakai connector terpisah.
- **ARIMA beradaptasi.** Model dilatih ulang tiap 5 window. Kalau degradasi dibiarkan lama, level baru dianggap normal dan `is_anomaly` kembali FALSE. Untuk kondisi yang menetap, pakai batas statis (kolom `severity` di file 03) atau hitung anomali berturut-turut.
- **Anomali ke bawah** (misalnya pompa berhenti) difilter di file 03.
- **`PARTITION BY asset_id`** belum muncul di contoh docs `ML_DETECT_ANOMALIES`. Cell `EXPLAIN` di file 02 untuk memastikannya.
- **`range` di schema Datagen.** Docs Confluent Cloud menyebut `options` didukung dan tidak membahas `range`. Kalau connector menolak schema, ganti `range` dengan `options` berisi daftar angka, misalnya `[2.1, 2.4, 2.6, 2.9, 3.1]`.
- **`ALTER TABLE pump_telemetry SET ('scan.startup.mode' = 'latest-offset')`** berlaku untuk query baru di tabel ini. Untuk membaca dari awal lagi, set kembali ke `earliest-offset`.
- **`ML_DETECT_ANOMALIES_ROBUST`** masih Open Preview. File 04 hanya SELECT karena field hasilnya belum didokumentasikan.

## Referensi

- Datagen Source Connector for Confluent Cloud: https://docs.confluent.io/cloud/current/connectors/cc-datagen-source.html
- Detect Anomalies with Confluent Cloud for Apache Flink: https://docs.confluent.io/cloud/current/ai/builtin-functions/detect-anomalies.html
- ALTER TABLE (scan.startup.mode): https://docs.confluent.io/cloud/current/flink/reference/statements/alter-table.html
