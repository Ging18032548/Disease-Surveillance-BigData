from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ---------------------------------------------------------
# Canonical Schema
# ---------------------------------------------------------

DISEASE_CANONICAL_SCHEMA = StructType(
    [
        StructField("case_id", StringType(), True),
        StructField("report_date", DateType(), True),
        StructField("year_be", IntegerType(), True),
        StructField("disease_name_raw", StringType(), True),
        StructField("district_name_raw", StringType(), True),
        StructField("case_count", DoubleType(), True),
        StructField("sex", StringType(), True),
        StructField("age", DoubleType(), True),
        StructField("source_file", StringType(), True),
    ]
)

GOLD_SCHEMA = StructType(
    [
        StructField("year_be", IntegerType(), False),
        StructField("district_name", StringType(), False),
        StructField("disease_name", StringType(), False),
        StructField("total_cases", LongType(), False),
        StructField("population", LongType(), True),
        StructField(
            "incidence_rate_per_100k",
            DoubleType(),
            True,
        ),
        StructField("source_records", LongType(), False),
    ]
)

GOLD_PRIMARY_KEY = [
    "year_be",
    "district_name",
    "disease_name",
]


# ---------------------------------------------------------
# Column Aliases
# ---------------------------------------------------------

DISEASE_COLUMN_ALIASES = {
    "case_id": [
        "case_id", "id", "patient_id", "record_id",
        "_id", "รหัสผู้ป่วย", "เลขที่ผู้ป่วย",
    ],
    "report_date": [
        "report_date", "date", "reported_date", "onset_date",
        "วันที่รายงาน", "วันที่", "วันที่ป่วย", "วันเริ่มป่วย",
        "วันที่เริ่มป่วย",
    ],
    "year": [
        "year", "report_year", "ปี", "ปีข้อมูล",
        "ปีพ.ศ.", "พ.ศ.",
    ],
    "disease_name": [
        "disease_name", "disease", "disease_th", "diagnosis",
        "โรค", "ชื่อโรค", "ชื่อกลุ่มโรค", "การวินิจฉัย",
    ],
    "district_name": [
        "district_name", "district", "district_th", "area",
        "เขต", "ชื่อเขต", "อำเภอ/เขต", "พื้นที่",
    ],
    "case_count": [
        "case_count", "cases", "count", "total_cases",
        "จำนวน", "จำนวนผู้ป่วย", "จำนวนราย",
    ],
    "sex": [
        "sex", "gender", "เพศ",
    ],
    "age": [
        "age", "patient_age", "อายุ",
        "อายุ (เต็ม) ปี",
    ],
}

POPULATION_COLUMN_ALIASES = {
    "year": [
        "year", "ปี", "ปีข้อมูล", "ปีพ.ศ.", "พ.ศ.",
        "_source_year",
    ],
    "district_name": [
        "district_name", "district", "district_th",
        "เขต", "ชื่อเขต", "อำเภอ/เขต", "แขวง/เขต",
    ],
    "population": [
        "population", "total_population", "ประชากร",
        "จำนวนประชากร", "ประชากรรวม", "รวมประชากร",
        "ประชากรทั้งหมด", "ประชากรรวมทั้งสิ้น",
    ],
}


# ---------------------------------------------------------
# Standard Reference Values
# ---------------------------------------------------------

_BANGKOK_DISTRICT_POMPRAP = "ป้อมปราบศัตรูพ่าย"

BANGKOK_DISTRICTS = [
    "พระนคร", "ดุสิต", "หนองจอก", "บางรัก", "บางเขน",
    "บางกะปิ", "ปทุมวัน", _BANGKOK_DISTRICT_POMPRAP, "พระโขนง",
    "มีนบุรี", "ลาดกระบัง", "ยานนาวา", "สัมพันธวงศ์",
    "พญาไท", "ธนบุรี", "บางกอกใหญ่", "ห้วยขวาง",
    "คลองสาน", "ตลิ่งชัน", "บางกอกน้อย", "บางขุนเทียน",
    "ภาษีเจริญ", "หนองแขม", "ราษฎร์บูรณะ", "บางพลัด",
    "ดินแดง", "บึงกุ่ม", "สาทร", "บางซื่อ", "จตุจักร",
    "บางคอแหลม", "ประเวศ", "คลองเตย", "สวนหลวง",
    "จอมทอง", "ดอนเมือง", "ราชเทวี", "ลาดพร้าว", "วัฒนา",
    "บางแค", "หลักสี่", "สายไหม", "คันนายาว", "สะพานสูง",
    "วังทองหลาง", "คลองสามวา", "บางนา", "ทวีวัฒนา",
    "ทุ่งครุ", "บางบอน",
]

DISTRICT_ALIASES = {
    "ป้อมปราบฯ": _BANGKOK_DISTRICT_POMPRAP,
    "ป้อมปราบ": _BANGKOK_DISTRICT_POMPRAP,
    _BANGKOK_DISTRICT_POMPRAP: _BANGKOK_DISTRICT_POMPRAP,
    "สัมพันธวงษ์": "สัมพันธวงศ์",
    "บางกอกไหญ่": "บางกอกใหญ่",
    "ราษฏร์บูรณะ": "ราษฎร์บูรณะ",
    "พระโขนg": "พระโขนง",
}

_DISEASE_DENGUE = "ไข้เลือดออก"
_DISEASE_INFLUENZA = "ไข้หวัดใหญ่"
_DISEASE_HFMD = "โรคมือเท้าปาก"
_DISEASE_COVID = "โควิด-19"
_DISEASE_DIARRHEA = "อุจจาระร่วงเฉียบพลัน"
_DISEASE_TUBERCULOSIS = "วัณโรค"

DISEASE_ALIASES = {
    _DISEASE_DENGUE: _DISEASE_DENGUE,
    "โรคไข้เลือดออก": _DISEASE_DENGUE,
    "dengue": _DISEASE_DENGUE,
    "denguefever": _DISEASE_DENGUE,
    _DISEASE_INFLUENZA: _DISEASE_INFLUENZA,
    "โรคไข้หวัดใหญ่": _DISEASE_INFLUENZA,
    "influenza": _DISEASE_INFLUENZA,
    "flu": _DISEASE_INFLUENZA,
    "มือเท้าปาก": _DISEASE_HFMD,
    _DISEASE_HFMD: _DISEASE_HFMD,
    "hfmd": _DISEASE_HFMD,
    "โควิด19": _DISEASE_COVID,
    _DISEASE_COVID: _DISEASE_COVID,
    "covid19": _DISEASE_COVID,
    "covid-19": _DISEASE_COVID,
    "อุจจาระร่วง": _DISEASE_DIARRHEA,
    "โรคอุจจาระร่วง": _DISEASE_DIARRHEA,
    "diarrhea": _DISEASE_DIARRHEA,
    _DISEASE_TUBERCULOSIS: _DISEASE_TUBERCULOSIS,
    "tuberculosis": _DISEASE_TUBERCULOSIS,
    "tb": _DISEASE_TUBERCULOSIS,
}

SEX_ALIASES = {
    "m": "M", "male": "M", "ชาย": "M", "1": "M",
    "f": "F", "female": "F", "หญิง": "F", "2": "F",
}