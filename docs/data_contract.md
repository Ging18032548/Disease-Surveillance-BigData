# Data Contract
## Big Data Analytics for Communicable Disease Surveillance in Bangkok

Version: 1.0

---

# 1. Purpose

เอกสาร Data Contract นี้กำหนดรูปแบบข้อมูลและข้อตกลงในการส่งต่อข้อมูลระหว่าง

- PART 1: Data Sources & Data Ingestion
- PART 2: Data Lake & Apache Airflow
- PART 3: Apache Spark Processing & Data Quality
- PART 4: Data Warehouse & Dashboard

โดยมีวัตถุประสงค์เพื่อให้แต่ละส่วนของ Data Pipeline
สามารถทำงานร่วมกันโดยใช้โครงสร้างข้อมูลที่ชัดเจนและสอดคล้องกัน

---

# 2. Overall Data Flow

Data Sources
    |
    v
PART 1 - Data Ingestion
    |
    | Raw JSON / Raw Excel
    v
PART 2 - Data Lake & Airflow
    |
    | Raw Zone
    v
PART 3 - Apache Spark
    |
    | Clean / Standardized Data
    v
PART 4 - Data Warehouse
    |
    v
Dashboard / Analytics

---

# 3. General Rules

## Raw Data

ข้อมูลที่ได้รับจากแหล่งข้อมูลต้นทางต้องถูกจัดเก็บใน Raw Layer
โดยรักษาค่าจากต้นทางไว้

PART 1 และ PART 2 ไม่ควร:

- เปลี่ยนชื่อคอลัมน์
- Standardize ชื่อโรค
- แก้ไข Missing Value
- ลบ Duplicate
- เปลี่ยนประเภทข้อมูลเพื่อการวิเคราะห์
- สร้าง Age Group
- Aggregate จำนวนผู้ป่วย

การดำเนินการดังกล่าวเป็นหน้าที่ของ PART 3

---

# 4. Dataset: Disease Surveillance

## Dataset ID

disease_cases

## Source

Data.go.th Data API

## Source Type

REST API

## Raw Format

JSON

## Encoding

UTF-8

## Available Years

- 2568 / 2025
- 2569 / 2026

## Development Mode

ในขั้นตอน Development ใช้ข้อมูลตัวอย่าง:

- 100 records สำหรับปี 2568
- 100 records สำหรับปี 2569

ข้อมูลตัวอย่างใช้สำหรับทดสอบ Pipeline เท่านั้น
และไม่ใช้เป็นตัวแทนสำหรับการสรุปสถิติของข้อมูลทั้งหมด

---

# 5. Disease Raw File Naming Convention

รูปแบบชื่อไฟล์:

disease_cases_{year}_sample.json

ตัวอย่าง:

disease_cases_2568_sample.json
disease_cases_2569_sample.json

Development Source Path:

data/raw/disease/

Expected files:

data/raw/disease/disease_cases_2568_sample.json
data/raw/disease/disease_cases_2569_sample.json

---

# 6. Disease Schema

| Field | Source Type | Required | Description |
|---|---|---|---|
| _id | integer | Yes | รหัส record จากแหล่งข้อมูล |
| ชื่อกลุ่มโรค | string | Yes | ชื่อกลุ่มโรค |
| อายุ (เต็ม) ปี | numeric | No | อายุเต็มหน่วยปี |
| อายุ (เต็ม) เดือน | numeric | No | อายุส่วนเดือน |
| อาย (เต็ม) วัน | numeric | No | อายุส่วนวัน |
| เพศ | string | No | เพศ |
| สถานภาพสมรส | string | No | สถานภาพสมรส |
| สัญชาติ | string | No | สัญชาติ |
| อาชีพ | string | No | อาชีพ |
| จังหวัด | string | Yes | จังหวัด |
| อำเภอ/เขต | string | Yes | อำเภอหรือเขต |
| ตำบล/แขวง | string | No | ตำบลหรือแขวง |
| วันที่เริ่มป่วย | timestamp/string | Yes | วันที่เริ่มป่วย |
| สภาพผู้ป่วย | string | No | สถานะ/สภาพผู้ป่วย |
| ประเภทผู้ป่วย | string | No | เช่น ผู้ป่วยนอก |
| สถานที่รักษา | string | No | สถานพยาบาลที่รักษา |

หมายเหตุ:
ชื่อ Field ต้องคงตาม Source ใน Raw Layer

---

# 7. Disease Data Quality Expectations

PART 1 ตรวจสอบเบื้องต้น:

- Schema ของปี 2568 และ 2569 มี 16 fields
- Sample ไม่มี Full-row Duplicate
- Sample ที่ตรวจไม่พบ Missing Value
- วันที่เริ่มป่วยสามารถแปลงเป็น Date/Timestamp ได้
- ข้อมูลมีระดับพื้นที่เขตและแขวง

อย่างไรก็ตาม การตรวจสอบจาก Sample
ไม่ถือเป็นผล Data Quality ของ Full Dataset

---

# 8. Known Disease Data Quality Issues

พบความแตกต่างของชื่อโรคระหว่างปี เช่น:

2568:

อุจจาระร่วง

2569:

โรคอุจจาระร่วงเฉียบพลัน

PART 3 ต้องพิจารณา Standardization ก่อนวิเคราะห์ข้ามปี

ตัวอย่าง:

อุจจาระร่วง
โรคอุจจาระร่วงเฉียบพลัน

        ↓

Acute Diarrhea

ห้ามแก้ไขค่าดังกล่าวใน Raw Layer

---

# 9. Dataset: Population Reference

## Dataset ID

population_buengkum

## Source

ข้อมูลประชากรและครัวเรือน
สำนักงานเขตบึงกุ่ม กรุงเทพมหานคร

## Source Type

Excel File

## Raw Format

XLSX

## Available Years

- 2568
- 2569

## Geographic Coverage

เฉพาะเขตบึงกุ่ม กรุงเทพมหานคร

ประกอบด้วย:

- คลองกุ่ม
- นวมินทร์
- นวลจันทร์

---

# 10. Population Raw File Naming Convention

ประชากรและครัวเรือน_{year}.xlsx

ตัวอย่าง:

ประชากรและครัวเรือน_2568.xlsx
ประชากรและครัวเรือน_2569.xlsx

Development Source Path:

data/raw/reference/

---

# 11. Population Excel Structure

Worksheet:

จำนวนประชากรและครัวเรือน

โครงสร้างไฟล์ต้นฉบับ:

Row 1
ชื่อรายงาน

Row 2
Column Header

Rows 3-5
ข้อมูลรายแขวง

Row 6
ยอดรวม

Row 7
Blank Row

Row 8
วันที่ปรับปรุงข้อมูล

ดังนั้น Raw Excel ไม่ควรถูกอ่านโดยสมมติว่า Row 1
เป็น Column Header

---

# 12. Population Schema

| Field | Expected Type | Required | Description |
|---|---|---|---|
| แขวง | string | Yes | ชื่อแขวง |
| จำนวนครัวเรือน | numeric | Yes | จำนวนครัวเรือน |
| ประชากรชาย | numeric | Yes | จำนวนประชากรชาย |
| ประชากรหญิง | numeric | Yes | จำนวนประชากรหญิง |
| ประชากรรวม | numeric | Yes | จำนวนประชากรรวม |

---

# 13. Population Validation Values

## 2568

คลองกุ่ม:
Population = 66,476

นวมินทร์:
Population = 25,979

นวลจันทร์:
Population = 43,435

Total:
Population = 135,890


## 2569

คลองกุ่ม:
Population = 65,467

นวมินทร์:
Population = 25,677

นวลจันทร์:
Population = 42,938

Total:
Population = 134,082

ค่าดังกล่าวสามารถใช้ตรวจสอบว่า
Data Lake / Spark อ่านข้อมูลจาก Excel ถูกต้องหรือไม่

---

# 14. Known Population Data Quality Issues

Raw Excel มี:

- Title Row
- Summary Row
- Blank Row
- Last Updated Row

ดังนั้น Missing Values ที่เกิดจากแถวเหล่านี้
ไม่ควรถูกตีความทันทีว่าเป็นข้อมูลประชากรสูญหาย

PART 3 ต้องแยก Data Records
ออกจาก Metadata Rows ก่อนการวิเคราะห์

---

# 15. Population Limitation

Population Dataset ปัจจุบันครอบคลุมเฉพาะเขตบึงกุ่ม

ดังนั้น:

ห้ามใช้ Population Dataset นี้
เพื่อคำนวณ Incidence Rate ของทั้งกรุงเทพมหานคร

สามารถใช้สำหรับ:

- Reference Data
- Pipeline Testing
- Data Validation
- การทดลอง Join เฉพาะเขตบึงกุ่ม

หากต้องการวิเคราะห์ Incidence Rate ทั้งกรุงเทพมหานคร
ต้องเพิ่ม Population Dataset ที่ครอบคลุมพื้นที่เดียวกับ Disease Dataset

---

# 16. PART 1 Responsibilities

PART 1 รับผิดชอบ:

1. เชื่อมต่อ Data Source
2. Authentication
3. ดึงข้อมูลตามขอบเขตที่กำหนด
4. จัดเก็บ Raw Data
5. ตรวจสอบ Schema เบื้องต้น
6. ตรวจสอบจำนวน Records
7. บันทึก Source Metadata
8. ปฏิบัติตามข้อกำหนดและ Rate Limit ของ API

PART 1 ไม่รับผิดชอบ Data Transformation

---

# 17. PART 2 Responsibilities

PART 2 รับข้อมูลจาก PART 1 และรับผิดชอบ:

1. นำ Raw Data เข้า Data Lake
2. แยก Storage Zone ตาม Dataset
3. ใช้ Apache Airflow จัดการ Workflow
4. ตรวจสอบว่า File Ingestion สำเร็จ
5. Logging
6. Retry เมื่อ Task ล้มเหลวตามนโยบายที่กำหนด
7. ส่ง Raw Data ให้ PART 3

PART 2 ต้องรักษา Raw Data ต้นฉบับไว้

---

# 18. Recommended Data Lake Structure

raw/
|
+-- disease/
|   |
|   +-- year=2568/
|   |   +-- disease_cases_2568_sample.json
|   |
|   +-- year=2569/
|       +-- disease_cases_2569_sample.json
|
+-- population/
    |
    +-- year=2568/
    |   +-- population_buengkum_2568.xlsx
    |
    +-- year=2569/
        +-- population_buengkum_2569.xlsx