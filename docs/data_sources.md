# Data Sources

## Project
Big Data Analytics for Communicable Disease Surveillance in Bangkok

การวิเคราะห์ข้อมูลขนาดใหญ่เพื่อเฝ้าระวังแนวโน้มโรคติดต่อในกรุงเทพมหานคร

---

## 1. Disease Surveillance Data

### Source
Data.go.th Data API

### Data Format
JSON (REST API)

### Years
- พ.ศ. 2568 (2025)
- พ.ศ. 2569 (2026)

### Resource IDs

2568:
ae882ad6-e057-4419-b3f7-1ffdbcfb6593

2569:
b59f2279-ced4-4c41-8452-e1c2cf60b2f1

### Available Records

- 2568: 234,081 records
- 2569: 193,866 records
- Total available: 427,947 records

### Development Sample

เพื่อหลีกเลี่ยงการร้องขอข้อมูลจาก API มากเกินความจำเป็น
ในขั้นตอนพัฒนาได้กำหนด limit = 100 records ต่อปี

ดังนั้นข้อมูลที่จัดเก็บใน Development Environment คือ:

- 2568: 100 records
- 2569: 100 records

ข้อมูลตัวอย่างดังกล่าวใช้สำหรับทดสอบ Data Ingestion Pipeline
และไม่ใช้เป็นตัวแทนสำหรับการสรุปสถิติของข้อมูลทั้งหมด

### Fields

1. _id
2. ชื่อกลุ่มโรค
3. อายุ (เต็ม) ปี
4. อายุ (เต็ม) เดือน
5. อาย (เต็ม) วัน
6. เพศ
7. สถานภาพสมรส
8. สัญชาติ
9. อาชีพ
10. จังหวัด
11. อำเภอ/เขต
12. ตำบล/แขวง
13. วันที่เริ่มป่วย
14. สภาพผู้ป่วย
15. ประเภทผู้ป่วย
16. สถานที่รักษา

### Raw Storage

data/raw/disease/

- disease_cases_2568_sample.json
- disease_cases_2569_sample.json

### Data Quality Observations

- Schema ของปี 2568 และ 2569 มีโครงสร้างเหมือนกัน
- Sample ไม่พบ Full-row duplicates
- Sample ไม่พบ Missing values
- ชื่อโรคบางรายการอาจแตกต่างกันระหว่างปี
- Sample ที่ดึงจาก offset แรกไม่ใช่ Random Sample
- ข้อมูล Sample ไม่ควรใช้สรุป Disease Distribution ของข้อมูลทั้งหมด

ตัวอย่างความแตกต่างของชื่อโรค:

- 2568: อุจจาระร่วง
- 2569: โรคอุจจาระร่วงเฉียบพลัน

การ Standardize ชื่อโรคจะดำเนินการในขั้นตอน Data Transformation

---

## 2. Population Reference Data

### Source

ข้อมูลประชากรและครัวเรือนในพื้นที่สำนักงานเขตบึงกุ่ม

### Data Format

Microsoft Excel (.xlsx)

### Years

- พ.ศ. 2568
- พ.ศ. 2569

### Geographic Coverage

สำนักงานเขตบึงกุ่ม กรุงเทพมหานคร

ประกอบด้วย 3 แขวง:

- คลองกุ่ม
- นวมินทร์
- นวลจันทร์

### Fields

1. แขวง
2. จำนวนครัวเรือน
3. ประชากรชาย
4. ประชากรหญิง
5. ประชากรรวม

### Population Summary

#### 2568

- Households: 77,477
- Male: 61,608
- Female: 74,282
- Total Population: 135,890

#### 2569

- Households: 80,522
- Male: 60,765
- Female: 73,317
- Total Population: 134,082

### Raw Storage

data/raw/reference/

- ประชากรและครัวเรือน_2568.xlsx
- ประชากรและครัวเรือน_2569.xlsx

### Data Quality Observations

ไฟล์ต้นฉบับประกอบด้วย:

- Title row
- Header row
- Subdistrict records
- Summary row
- Empty row
- Last updated information

Missing values ที่พบในการตรวจสอบ Raw Excel
ส่วนหนึ่งเกิดจากโครงสร้างเอกสาร เช่น แถวว่างและแถววันที่ปรับปรุง
ไม่ได้หมายความว่าข้อมูลประชากรของ 3 แขวงขาดหาย

### Limitation

Population Dataset นี้ครอบคลุมเฉพาะเขตบึงกุ่ม
จึงยังไม่สามารถใช้คำนวณ Population-based Incidence Rate
สำหรับทั้งกรุงเทพมหานครได้

---

## 3. Ethical Data Ingestion

ใน Development Phase มีการจำกัดจำนวนข้อมูลที่ร้องขอจาก API
เพื่อลดภาระต่อระบบต้นทางและหลีกเลี่ยงการร้องขอข้อมูล
มากเกินความจำเป็น

แนวทางที่ใช้:

- ใช้ API Token ตามข้อกำหนดของผู้ให้บริการ
- จำกัดจำนวน records ด้วย API limit
- ไม่เรียก API ซ้ำเมื่อมี Raw Data อยู่แล้ว
- ไม่พยายามหลีกเลี่ยง Rate Limit
- เก็บ API Token ใน .env
- ไม่บันทึก API Token ลง Source Code หรือ Git Repository
- เก็บ Raw Data โดยไม่แก้ไขค่าต้นฉบับ

---

## 4. Current Ingestion Flow

Data.go.th Disease API
        |
        v
Python Requests
        |
        v
Raw JSON
        |
        v
Data Lake / Airflow
        |
        v
Apache Spark

Population Excel
        |
        v
Raw Excel
        |
        v
Data Lake / Airflow
        |
        v
Apache Spark

การ Cleaning, Standardization และ Transformation
จะดำเนินการใน Apache Spark ไม่ใช่ใน Raw Ingestion Layer.