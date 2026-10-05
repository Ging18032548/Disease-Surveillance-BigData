# Disease Surveillance Big Data

## การวิเคราะห์ข้อมูลขนาดใหญ่เพื่อเฝ้าระวังแนวโน้มโรคติดต่อในกรุงเทพมหานคร

**Big Data Analytics for Communicable Disease Surveillance in Bangkok**

โปรเจกต์นี้จัดทำขึ้นเพื่อออกแบบและพัฒนา Big Data Pipeline สำหรับรวบรวม จัดเก็บ ประมวลผล และวิเคราะห์ข้อมูลผู้ป่วยโรคติดต่อในกรุงเทพมหานคร โดยใช้ข้อมูลจากแหล่งข้อมูลภาครัฐและข้อมูลประชากรประกอบการวิเคราะห์

ระบบออกแบบให้ครอบคลุมกระบวนการตั้งแต่ **Data Ingestion → Data Lake → Workflow Orchestration → Distributed Processing → Data Warehouse → Dashboard**

---

## Project Objectives

วัตถุประสงค์ของโครงการ ได้แก่

1. รวบรวมข้อมูลผู้ป่วยโรคติดต่อจากแหล่งข้อมูลภาครัฐ
2. พัฒนา Data Ingestion สำหรับข้อมูลจาก REST API และไฟล์ Excel
3. จัดเก็บข้อมูลต้นฉบับใน Raw Data Layer / Data Lake
4. ใช้ Apache Airflow ควบคุม Data Pipeline
5. ใช้ Apache Spark สำหรับประมวลผลและตรวจสอบคุณภาพข้อมูล
6. จัดเก็บข้อมูลที่ผ่านการประมวลผลใน Data Warehouse
7. วิเคราะห์แนวโน้มโรคตามเวลา พื้นที่ และลักษณะประชากร
8. นำเสนอผลผ่าน Dashboard

---

## Disease Scope

ข้อมูล Disease Surveillance ปัจจุบันประกอบด้วยโรคสำคัญ เช่น

- ไข้หวัดใหญ่
- ไข้เลือดออก
- อุจจาระร่วง / โรคอุจจาระร่วงเฉียบพลัน
- โรคปอดบวม
- COVID-19

> รายชื่อและค่าของโรคจะอ้างอิงจากข้อมูลต้นทาง โดยการ Standardize ชื่อโรคจะดำเนินการใน Processing Layer

---

# Data Sources

## 1. Disease Surveillance Data

**Source:** Data.go.th Data API  
**Format:** JSON / REST API

ข้อมูลที่ใช้งาน:

| ปี พ.ศ. | ค.ศ. | Available Records |
|---|---:|---:|
| 2568 | 2025 | 234,081 |
| 2569 | 2026 | 193,866 |
| **รวม** | | **427,947** |

ใน Development Phase ใช้ตัวอย่างเพียง:

```text
2568 → 100 records
2569 → 100 records
```

เพื่อทดสอบ Data Pipeline โดยไม่ส่งคำขอไปยัง API มากเกินความจำเป็น

### Disease Fields

ข้อมูลประกอบด้วย 16 fields:

```text
_id
ชื่อกลุ่มโรค
อายุ (เต็ม) ปี
อายุ (เต็ม) เดือน
อาย (เต็ม) วัน
เพศ
สถานภาพสมรส
สัญชาติ
อาชีพ
จังหวัด
อำเภอ/เขต
ตำบล/แขวง
วันที่เริ่มป่วย
สภาพผู้ป่วย
ประเภทผู้ป่วย
สถานที่รักษา
```

---

## 2. Population Reference Data

ข้อมูลประชากรและครัวเรือนของสำนักงานเขตบึงกุ่ม กรุงเทพมหานคร

**Format:** Microsoft Excel (`.xlsx`)

ปีที่มีข้อมูล:

```text
2568
2569
```

พื้นที่ประกอบด้วย 3 แขวง:

```text
คลองกุ่ม
นวมินทร์
นวลจันทร์
```

### Population Fields

```text
แขวง
จำนวนครัวเรือน
ประชากรชาย
ประชากรหญิง
ประชากรรวม
```

> Population Dataset ปัจจุบันใช้เป็น Reference / Validation และสำหรับทดลอง Pipeline เท่านั้น เนื่องจากครอบคลุมเฉพาะเขตบึงกุ่ม ไม่ใช่ทั้งกรุงเทพมหานคร

---

# System Architecture

```text
+---------------------------+
|       Data Sources        |
|                           |
| - Data.go.th API          |
| - Population Excel        |
+-------------+-------------+
              |
              v
+---------------------------+
|     Python Ingestion      |
|                           |
| requests / pandas         |
+-------------+-------------+
              |
              v
+---------------------------+
|        Apache Airflow     |
|     Workflow Scheduling   |
+-------------+-------------+
              |
              v
+---------------------------+
|          Data Lake        |
|       MinIO / HDFS        |
|                           |
|          Raw Zone         |
+-------------+-------------+
              |
              v
+---------------------------+
|        Apache Spark       |
|                           |
| Cleaning                  |
| Standardization           |
| Data Quality              |
| Transformation            |
+-------------+-------------+
              |
              v
+---------------------------+
|       Data Warehouse      |
|         PostgreSQL        |
+-------------+-------------+
              |
              v
+---------------------------+
|          Dashboard        |
|       Power BI / BI       |
+---------------------------+
```

---

# Team Responsibilities

โปรเจกต์แบ่งการทำงานออกเป็น 4 ส่วนหลัก

## PART 1 — Data Sources & Data Ingestion

รับผิดชอบ:

- ค้นหาและประเมิน Data Sources
- เชื่อมต่อ REST API
- API Authentication
- Data Extraction
- Ethical API Usage
- Raw Data Storage
- Raw Data Profiling
- Data Source Documentation
- Data Contract

Output:

```text
Raw JSON
Raw Excel
Data Source Documentation
Data Contract
```

---

## PART 2 — Data Lake & Apache Airflow

รับผิดชอบ:

- Data Lake
- MinIO / HDFS
- Apache Airflow
- DAG
- Scheduling
- Pipeline Logging
- Retry / Failure Handling
- Raw Zone Management

Input:

```text
Raw JSON
Raw Excel
```

Output:

```text
Data Lake Raw Zone
```

---

## PART 3 — Apache Spark & Data Quality

รับผิดชอบ:

- Apache Spark / PySpark
- Schema Validation
- Data Cleaning
- Missing Value Handling
- Duplicate Handling
- Type Conversion
- Date Parsing
- Disease Name Standardization
- Age Processing
- Geographic Standardization
- Population Processing
- Data Quality Validation

Output:

```text
Clean Dataset
Standardized Dataset
Curated Dataset
```

---

## PART 4 — Data Warehouse & Dashboard

รับผิดชอบ:

- PostgreSQL
- Data Warehouse
- Dimensional Modeling
- SQL Analytics
- Dashboard
- Visualization

ตัวอย่างการวิเคราะห์:

- จำนวนผู้ป่วยแยกตามโรค
- แนวโน้มผู้ป่วยตามเวลา
- จำนวนผู้ป่วยแยกตามเขต
- จำนวนผู้ป่วยแยกตามแขวง
- การกระจายตามเพศ
- การกระจายตามช่วงอายุ
- การเปรียบเทียบโรค
- Incidence Rate เมื่อมี Population Coverage ที่เหมาะสม

---

# Project Structure

```text
Disease-Surveillance-BigData/
│
├── README.md
├── requirements.txt
├── .env
├── .gitignore
│
├── data/
│   ├── raw/
│   │   ├── disease/
│   │   │   ├── disease_cases_2568_sample.json
│   │   │   └── disease_cases_2569_sample.json
│   │   │
│   │   ├── population/
│   │   │
│   │   └── reference/
│   │       ├── ประชากรและครัวเรือน_2568.xlsx
│   │       └── ประชากรและครัวเรือน_2569.xlsx
│   │
│   └── processed/
│
├── src/
│   └── extract/
│       ├── test_disease_api.py
│       ├── extract_disease.py
│       ├── profile_disease.py
│       └── profile_population.py
│
├── docs/
│   ├── data_sources.md
│   ├── data_contract.md
│   └── data_dictionary/
│
├── airflow/
│
├── spark/
│
└── dashboard/
```

---

# Development Environment

## Requirements

แนะนำ:

```text
Python 3.11+
pip
Git
```

Python packages หลัก:

```text
requests
pandas
openpyxl
python-dotenv
```

---

# Installation

## 1. Clone Repository

```bash
git clone <repository-url>
cd Disease-Surveillance-BigData
```

---

## 2. Create Virtual Environment

Windows:

```powershell
python -m venv .venv
```

Activate:

```powershell
.\.venv\Scripts\Activate.ps1
```

เมื่อสำเร็จควรเห็น:

```text
(.venv)
```

ด้านหน้า Terminal

---

## 3. Install Dependencies

```powershell
pip install -r requirements.txt
```

ตรวจสอบ:

```powershell
pip list
```

---

# Environment Variables

โปรเจกต์ใช้ Data.go.th API Token สำหรับ Authentication

สร้างไฟล์:

```text
.env
```

ที่ Root ของ Project

แล้วกำหนด:

```env
DATA_GO_TH_TOKEN=YOUR_API_TOKEN
```

> ห้าม Commit `.env` หรือ API Token ขึ้น Git Repository

---

# Git Ignore

ตัวอย่าง `.gitignore`:

```gitignore
# Virtual Environment
.venv/

# Secrets
.env

# Python
__pycache__/
*.pyc

# Raw Data
data/raw/
```

Raw Data สามารถดาวน์โหลดหรือสร้างใหม่จาก Data Source ได้ จึงไม่จำเป็นต้องเก็บไฟล์ขนาดใหญ่ใน Git Repository

---

# Running Data Ingestion

## Test API

ใช้ตรวจสอบ API Authentication และ Schema:

```powershell
python src\extract\test_disease_api.py
```

---

## Extract Disease Sample

```powershell
python src\extract\extract_disease.py
```

Development Configuration ปัจจุบัน:

```text
100 records / year
```

ผลลัพธ์:

```text
data/raw/disease/disease_cases_2568_sample.json
data/raw/disease/disease_cases_2569_sample.json
```

---

# Data Profiling

## Disease Profiling

```powershell
python src\extract\profile_disease.py
```

ตรวจสอบเบื้องต้น เช่น

```text
Rows
Columns
Missing Values
Duplicate Rows
Disease Distribution
Gender Distribution
Districts
Subdistricts
Date Range
```

> Sample Profiling ไม่ใช้เป็นตัวแทนสำหรับการสรุปสถิติของ Full Dataset

---

## Population Profiling

```powershell
python src\extract\profile_population.py
```

ใช้ตรวจสอบ:

```text
Excel Structure
Columns
Missing Values
Duplicate Rows
Subdistrict Records
Households
Male Population
Female Population
Total Population
```

---

# Raw Data Policy

Raw Data ต้องรักษาข้อมูลตาม Source

ห้ามแก้ไข Raw Dataset โดยตรง เช่น

```text
Rename Column
Remove Missing Value
Delete Duplicate
Standardize Disease
Convert Date
Create Age Group
Aggregate Records
```

การดำเนินการดังกล่าวต้องทำใน Processing Layer โดย Apache Spark

---

# Data Quality Notes

## Disease Name

พบความแตกต่างของชื่อโรคระหว่างปี เช่น

```text
2568:
อุจจาระร่วง

2569:
โรคอุจจาระร่วงเฉียบพลัน
```

จึงต้องมี Disease Name Standardization ใน Spark

---

## Sample Bias

ข้อมูล Development Sample ใช้ records จากจุดเริ่มต้นของ Dataset:

```text
offset = 0
limit = 100
```

ดังนั้นข้อมูลดังกล่าว:

- ไม่ใช่ Random Sample
- ไม่ใช่ Stratified Sample
- ไม่ควรใช้สรุป Disease Distribution
- ใช้สำหรับ Pipeline Development และ Testing เท่านั้น

---

## Population Coverage

Population Dataset ปัจจุบันครอบคลุมเฉพาะ:

```text
กรุงเทพมหานคร
└── เขตบึงกุ่ม
    ├── คลองกุ่ม
    ├── นวมินทร์
    └── นวลจันทร์
```

ดังนั้นไม่ควรนำไปคำนวณ Incidence Rate ของทั้งกรุงเทพมหานคร

---

# Ethical Data Ingestion

โปรเจกต์ให้ความสำคัญกับการใช้งาน Open Data และ API อย่างเหมาะสม

Development Phase ใช้แนวทาง:

1. ดึงข้อมูลเฉพาะเท่าที่จำเป็น
2. ใช้ `limit` จำกัดจำนวน records
3. หลีกเลี่ยงการเรียก API ซ้ำโดยไม่จำเป็น
4. ใช้ข้อมูล Raw ที่จัดเก็บไว้แล้วสำหรับการพัฒนา
5. เคารพ Authentication และ Rate Limit ของผู้ให้บริการ
6. ไม่พยายาม Bypass ข้อจำกัดของ API
7. ไม่เปิดเผย API Token
8. ไม่ Hard-code Secret ลง Source Code

Full Dataset Ingestion จะดำเนินการเมื่อมีความจำเป็นต่อการประมวลผลและสอดคล้องกับข้อกำหนดของผู้ให้บริการข้อมูล

---

# Data Contract

รายละเอียด Schema, File Naming, Raw Data Rules, Data Lake Structure และข้อตกลงระหว่างแต่ละส่วนของ Pipeline อยู่ที่:

```text
docs/data_contract.md
```

รายละเอียดแหล่งข้อมูลอยู่ที่:

```text
docs/data_sources.md
```

---

# Data Lake and Airflow (Local Development)

The local infrastructure uses Docker Compose, SeaweedFS as an S3-compatible raw data lake, and Apache Airflow with LocalExecutor. It lands the disease sample JSON files already extracted by `src/extract/extract_disease.py`; the DAG does not call the source API.

1. Keep the existing `.env` (it contains the API token); add local service credentials from `.env.example` only if you want to override the development defaults. Never commit `.env`.
2. Start the services: `docker compose up -d --build`
3. Open Airflow at `http://localhost:8080` (default local login is `airflow` / `airflow-local-only`, unless overridden in `.env`), unpause the DAG if needed, then trigger `disease_raw_to_lake`.
4. Browse SeaweedFS filer at `http://localhost:8888` or connect an S3 client to `http://localhost:8333` and inspect bucket `disease-surveillance`.

Objects are written under `raw/disease/year=2568/` and `raw/disease/year=2569/`, with a checksum and manifest per year. Re-running the DAG is safe when the source bytes are unchanged; changed bytes at an existing key fail instead of silently replacing raw history. See `docs/data_lake_airflow.md` for details and operations.

These Compose credentials are for local development only. Do not use the defaults in a shared or production environment.

---

# Current Project Status

## PART 1 — Development Phase

| Task | Status |
|---|---|
| Disease Data Source | Completed |
| API Authentication | Completed |
| API Schema Validation | Completed |
| Disease 2568 Ingestion | Completed |
| Disease 2569 Ingestion | Completed |
| Ethical Sample Extraction | Completed |
| Disease Raw Profiling | Completed |
| Population Reference 2568 | Completed |
| Population Reference 2569 | Completed |
| Population Profiling | Completed |
| Data Source Documentation | Completed |
| Data Contract | Completed |

---

# Next Phase

ขั้นตอนถัดไปของ Data Pipeline:

```text
PART 1
Data Sources & Ingestion
        |
        | Raw JSON / Excel
        v
PART 2
Apache Airflow + Data Lake
        |
        v
PART 3
Apache Spark + Data Quality
        |
        v
PART 4
Data Warehouse + Dashboard
```

---

## Important

โปรเจกต์นี้มีวัตถุประสงค์เพื่อการศึกษาและการวิเคราะห์ข้อมูลเชิงสถิติ/การเฝ้าระวังโรคจากข้อมูลที่เผยแพร่โดยหน่วยงานภาครัฐ

ผลการวิเคราะห์จาก Development Sample ไม่ควรตีความว่าเป็นสถิติทางระบาดวิทยาของประชากรทั้งหมด
