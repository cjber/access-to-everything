from pathlib import Path


class NHSScot:
    HOSPITAL_ID = "c698f450-eeed-41a0-88f7-c1e40a568acc"
    GP_ID = "b3b126d3-3b0c-4856-b348-0b37f8286367"
    DENTISTS_ID = "3e848c81-758d-4d64-87ee-0e2f147a7a81"
    PHARMACIES_CSV = "https://www.opendata.nhs.scot/dataset/a30fde16-1226-49b3-b13d-eb90e39c2058/resource/bfbc492d-7318-4b3d-9f01-087491aafb38/download/dispenser_contactdetails_may_24.csv"


class NHSEng:
    HOSPITALS_URL = (
        "https://files.digital.nhs.uk/assets/ods/current/ets.zip",
        "ets.csv",
    )
    GP_URL = (
        "https://files.digital.nhs.uk/assets/ods/current/epraccur.zip",
        "epraccur.csv",
    )
    DENTISTS_URL = (
        "https://files.digital.nhs.uk/assets/ods/current/egdpprac.zip",
        "egdpprac.csv",
    )
    PHARMACIES_URL = (
        "https://files.digital.nhs.uk/assets/ods/current/edispensary.zip",
        "edispensary.csv",
    )


class NHSWales:
    PHARMACIES_EXCEL = "https://nwssp.nhs.wales/ourservices/primary-care-services/primary-care-services-documents/pharmacy-practice-dispensing-data-docs/dispensing-data-report-november-2023"


class Paths:
    DATA = Path("data")
    RAW = DATA / "raw"
    OUT = DATA / "out"
    POSTCODES = RAW / "ONSPD_FEB_2024.csv"

    GREENSPACE = RAW / "opgrsp_gb.gpkg"
    ENG_EDUCATION = RAW / "schools.csv"
    SCOT_EDUCATION = RAW / "SG_SchoolRoll_2023" / "SG_SchoolRoll_2023.shp"
    OVERTURE = RAW / "overture_poi_alternate.parquet"
    TRANSPORT = RAW / "Stops.csv"
