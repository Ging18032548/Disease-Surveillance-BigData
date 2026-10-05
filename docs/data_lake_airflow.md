# Data Lake และ Airflow สำหรับการพัฒนาในเครื่อง

## ขอบเขต

ชุดระบบนี้รับไฟล์ตัวอย่างข้อมูลโรค 2 ไฟล์จาก `data/raw/disease/` และไฟล์ population summary รายปีจาก `data/raw/disease/reference/` ตรวจสอบโครงสร้างแล้วเก็บ byte ต้นฉบับไว้ใน SeaweedFS ผ่าน S3-compatible API โดยให้ Airflow ควบคุมลำดับการทำงาน ทั้งนี้ DAG ปัจจุบันไม่ได้แปลงข้อมูลและไม่ได้เรียก Data.go.th

## บริการที่ใช้

- **SeaweedFS**: จัดเก็บข้อมูลแบบ S3-compatible เปิด S3 API ที่ `localhost:8333` และหน้า filer ที่ `localhost:8888`
- **PostgreSQL**: เก็บ metadata ของ Airflow ไม่ใช่ Data Warehouse
- **Airflow**: ควบคุม workflow ในเครื่องด้วย LocalExecutor เปิดหน้าเว็บที่ `localhost:8080`

ข้อมูลใน SeaweedFS, metadata ของ Airflow และ task logs จะเก็บใน Docker named volumes ส่วน Airflow mount โฟลเดอร์ `data/raw` แบบอ่านอย่างเดียว

## เริ่มและใช้งาน

1. เก็บไฟล์ `.env` เดิมไว้ เพราะมี API token อยู่ หากต้องการเปลี่ยนค่าตั้งต้น ให้เพิ่มค่าจาก `.env.example` ลงใน `.env` เดิม และห้าม commit `.env`
2. เริ่มบริการด้วย `docker compose up -d --build`
3. ตรวจสถานะด้วย `docker compose ps`
4. เข้าสู่ Airflow โดยใช้ `AIRFLOW_ADMIN_USER` และ `AIRFLOW_ADMIN_PASSWORD` จาก `.env` (ค่าตั้งต้นคือ `airflow` / `airflow-local-only`) จากนั้น unpause DAG หากยัง pause อยู่ แล้วสั่งรัน `disease_raw_to_lake`
5. ดูข้อมูลผ่าน SeaweedFS filer ที่ `http://localhost:8888` หรือใช้ S3 client เชื่อมต่อ `http://localhost:8333`
6. หยุดบริการโดยเก็บข้อมูลไว้ด้วย `docker compose down` หากต้องการล้างข้อมูลและฐานข้อมูลของชุดพัฒนา ให้ใช้ `docker compose down -v` ซึ่งจะลบ Docker volumes ด้วย

API token ใน `.env` ใช้เฉพาะตอนเรียกสคริปต์ดึงข้อมูลแยกต่างหาก DAG อ่านไฟล์ที่มีอยู่แล้วจึงไม่ต้องใช้ token นี้ ส่วน S3 service ใช้ `S3_ACCESS_KEY` และ `S3_SECRET_KEY` เป็น credentials ตั้งต้น

## การทำงานของ DAG

DAG `disease_raw_to_lake` ตรวจและสร้าง bucket หากยังไม่มี จากนั้นนำเข้าข้อมูลโรคและ population ของปี 2568 และ 2569 โดย task ของแต่ละปีทำงานขนานกันได้

DAG ตรวจว่ามีไฟล์ทั้งสองปี ตรวจว่าแต่ละไฟล์มี 100 records และมี 16 fields ตาม Data Contract จากนั้นเก็บ byte ต้นฉบับลง bucket ตาม key เหล่านี้:

```text
raw/disease/year=2568/disease_cases_2568_sample.json
raw/disease/year=2569/disease_cases_2569_sample.json
```

ระบบบันทึก SHA-256, จำนวน records, ปีของข้อมูล และ content type เป็น object metadata พร้อมสร้าง manifest ในโฟลเดอร์ `_metadata/` ของแต่ละปี โดยตั้ง retry ไว้ 2 ครั้ง หากรันซ้ำด้วยไฟล์เดิม checksum จะตรงกันและไม่เขียนข้อมูลซ้ำ แต่ถ้าไฟล์เปลี่ยนและใช้ key เดิม task จะล้มเหลวเพื่อป้องกันการเขียนทับ Raw Data เดิม ควรใช้ชื่อไฟล์หรือ key แบบมีเวอร์ชันสำหรับข้อมูลชุดใหม่

Population ต้องใช้ไฟล์ `population_summary_{year}.csv` ที่มีคอลัมน์ `ปี,เขต,ประชากรรวม`
และต้องมีข้อมูลครบ 50 เขตของกรุงเทพมหานครแบบไม่ซ้ำกัน ค่าประชากรต้องเป็นจำนวนเต็มบวก
ก่อน land หากข้อมูลไม่ครบหรือมีเขตเกิน ระบบจะหยุด task พร้อมรายชื่อเขตที่ผิดพลาด
object จะถูกเก็บที่ `raw/population/year={year}/` และมี metadata ระบุ `coverage=Bangkok-50-districts`

## เตรียม Population Reference สำหรับ Spark

ใช้ `spark/prepare_population_reference.py` กับไฟล์หรือ URL จากแหล่งข้อมูลทางการ
แยกตามปี เช่น:

```powershell
python -m spark.prepare_population_reference `
  --project-root . `
  --source-2568 "C:\source\population_2568.xlsx" `
  --source-2569 "C:\source\population_2569.xlsx"
```

สคริปต์รองรับข้อมูลที่มีระดับรายเขตหรือรายแขวง (โดยต้องมีคอลัมน์เขต)
และจะรวม `ประชากรรวม` ตาม `ปี,เขต` ก่อนตรวจสอบว่าครบ 50 เขต
จะไม่สร้างตัวเลขแทนข้อมูลจริง และจะไม่เขียนทับไฟล์เดิมโดยอัตโนมัติ

ตรวจสอบไฟล์ที่เตรียมแล้วด้วย:

```powershell
python -m spark.validate_population_reference --project-root .
```

ผลลัพธ์จะถูกสร้างเป็น:

```text
data/raw/disease/reference/ประชากรและครัวเรือน_2568.csv
data/raw/disease/reference/ประชากรและครัวเรือน_2569.csv
```

ถ้าต้องการแทนที่ไฟล์ผลลัพธ์เดิม ต้องระบุ `--overwrite` อย่างชัดเจน
และควรเก็บไฟล์ต้นทางกับ URL/วันที่ดาวน์โหลดไว้เป็นหลักฐานนอก Raw output

## รัน Spark ผ่าน Airflow

มี DAG แยกชื่อ `spark_processing` สำหรับเรียก
`python -m spark.main --fail-on-dq` โดย Raw DAG จะ trigger DAG นี้
อัตโนมัติหลัง upload disease และ population ทั้งหมดสำเร็จ

ติดตั้ง runtime ใหม่และเริ่มบริการ:

```powershell
docker compose up -d --build
```

รัน Raw DAG ก่อน:

```powershell
docker compose exec airflow airflow dags trigger disease_raw_to_lake
```

หากต้องการรัน Spark ซ้ำโดยไม่ land Raw ใหม่:

```powershell
docker compose exec airflow airflow dags trigger spark_processing
```

ผลลัพธ์จะถูกเขียนใน `data/processed/` ของโปรเจกต์
และตรวจคุณภาพด้วย `--fail-on-dq`; หาก Data Quality ไม่ผ่าน task จะล้มเหลว

ไฟล์ `spark/convert_excel_to_csv.py` ไม่ใช่ source ingestion
เพราะมีค่าประชากรที่เขียนตายตัวในโค้ด จึงห้ามใช้สร้างข้อมูลประชากรจริง
สำหรับ pipeline ต้องใช้ CSV ที่มาจากแหล่งทางการและผ่าน validation เท่านั้น

## ความปลอดภัยสำหรับการพัฒนาในเครื่อง

ค่าตั้งต้นใน Compose มีไว้สำหรับทดลองในเครื่องเท่านั้น หากแชร์เครื่องหรือเปิดให้คนอื่นเข้าถึง ให้เปลี่ยน credentials และจำกัดพอร์ตที่เปิดใช้งาน ก่อนนำไปใช้ในสภาพแวดล้อมอื่น ควรจัดการ secrets ผ่าน secret manager และตั้งค่า TLS กับ access policies ให้เหมาะสม พอร์ต Airflow และ SeaweedFS เปิดรับจากทุก interface ของเครื่องตามค่าเริ่มต้น ควรจำกัดพอร์ตหากเครื่องเชื่อมต่อเครือข่ายร่วมกับผู้อื่น
