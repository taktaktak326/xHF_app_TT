# apps/api/schemas.py
from typing import Any, List, Optional
from pydantic import BaseModel

class LoginReq(BaseModel):
    email: str
    password: str

class FourValues(BaseModel):
    login_token: str
    gigya_uuid: str
    gigya_uuid_signature: str
    gigya_signature_timestamp: str

class FarmsReq(BaseModel):
    login_token: str
    api_token: str
    includeTokens: bool = False

class FieldsReq(BaseModel):
    login_token: str
    api_token: str
    farm_uuid: str
    includeTokens: bool = False

# ---- /field-notes ----
class FieldNotesReq(BaseModel):
    login_token: str
    api_token: str
    farm_uuids: list[str]
    includeTokens: bool = False

# ---- /weather-by-field ----
class WeatherByFieldReq(BaseModel):
    login_token: str
    api_token: str
    field_uuid: str
    from_date: Optional[str] = None
    till_date: Optional[str] = None
    includeTokens: bool = False

# ---- /crop-protection-products ----
class CropProtectionProductsReq(BaseModel):
    login_token: str
    api_token: str
    farm_uuids: List[str]
    country_uuid: str
    crop_uuid: str
    task_type_code: Optional[str] = "FIELDTREATMENT"
    includeTokens: bool = False

class CropProtectionProductsBulkReq(BaseModel):
    login_token: str
    api_token: str
    farm_uuids: List[str]
    country_uuid: str
    crop_uuids: List[str]
    task_type_code: Optional[str] = "FIELDTREATMENT"
    includeTokens: bool = False

class FieldDataLayersReq(BaseModel):
    login_token: str
    api_token: str
    field_uuid: str
    types: Optional[List[str]] = None
    includeTokens: bool = False

# ---- /field-data-layer/image ----
class FieldDataLayerImageReq(BaseModel):
    login_token: str
    api_token: str
    image_url: str
    includeTokens: bool = False
    
class CombinedFieldsReq(BaseModel):
    login_token: str
    api_token: str
    farm_uuids: List[str]
    languageCode: str = "ja"
    countryCode: str = "JP"
    cropSeasonLifeCycleStates: List[str] = ["ACTIVE", "PLANNED"]
    withBoundarySvg: bool = True
    stream: bool = False
    includeTasks: bool = True
    # Task flags
    withHarvests: bool = True
    withCropEstablishments: bool = True
    withLandPreparations: bool = True
    withDroneFlights: bool = False
    withSeedTreatments: bool = True
    withSeedBoxTreatments: bool = True
    withSmartSprayingTasks: bool = False
    withWaterManagementTasks: bool = True
    withScoutingTasks: bool = True
    withObservations: bool = False
    withSprayingsV2: bool = True
    withSoilSamplingTasks: bool = False
    includeTokens: bool = False

class BiomassNdviReq(BaseModel):
    login_token: str
    api_token: str
    crop_season_uuids: List[str]
    from_date: str
    includeTokens: bool = False

class BiomassLaiReq(BaseModel):
    login_token: str
    api_token: str
    crop_season_uuids: List[str]
    from_date: str
    till_date: str
    includeTokens: bool = False

class CombinedFieldDataTasksReq(BaseModel):
    login_token: str
    api_token: str
    farm_uuids: List[str]
    languageCode: str = "ja"
    cropSeasonLifeCycleStates: List[str] = ["ACTIVE", "PLANNED"]
    withBoundary: bool = True
    withCropSeasonsV2: bool = True
    withHarvests: bool = True
    withCropEstablishments: bool = True
    withLandPreparations: bool = True
    withDroneFlights: bool = False
    withSeedTreatments: bool = True
    withSeedBoxTreatments: bool = True
    withSmartSprayingTasks: bool = False
    withWaterManagementTasks: bool = True
    withScoutingTasks: bool = True
    withObservations: bool = False
    withSprayingsV2: bool = True
    withSoilSamplingTasks: bool = False
    includeTokens: bool = False


class SprayingTaskUpdateReq(BaseModel):
    login_token: str
    api_token: str
    plannedDate: Optional[str] = None
    executionDate: Optional[str] = None
    ifMatch: Optional[str] = None
    includeTokens: bool = False


class MasterdataCropsReq(BaseModel):
    login_token: str
    api_token: str
    locale: str = "JA-JP"
    includeTokens: bool = False


class MasterdataVarietiesReq(BaseModel):
    login_token: str
    api_token: str
    cropUuid: str
    locale: str = "JA-JP"
    countryCode: str = "JP"
    includeTokens: bool = False


class MasterdataPartnerTillagesReq(BaseModel):
    login_token: str
    api_token: str
    locale: str = "JA-JP"
    includeTokens: bool = False


class MasterdataTillageSystemsReq(BaseModel):
    login_token: str
    api_token: str
    locale: str = "JA-JP"
    includeTokens: bool = False


class CropSeasonCreateItem(BaseModel):
    fieldUuid: str
    cropUuid: str
    varietyUuid: str
    startDate: str
    yieldExpectation: float
    lifecycleState: Optional[str] = None
    cropEstablishmentMethodCode: Optional[str] = None
    cropEstablishmentGrowthStageIndex: Optional[str] = None
    cropMix: Optional[dict[str, Any]] = None
    seedingTillageSystemUuid: Optional[str] = None
    tillageUuid: Optional[str] = None
    preCropUuid: Optional[str] = None

    class Config:
        extra = "allow"


class CropSeasonCreateReq(BaseModel):
    login_token: str
    api_token: str
    payloads: List[CropSeasonCreateItem]
    includeTokens: bool = False


# ---- /cross-farm-dashboard/_search ----
class CrossFarmDashboardSearchReq(BaseModel):
    login_token: str
    api_token: str
    body: dict = {}
    includeClosedCropSeasons: bool = False
    includeTokens: bool = False

    class Config:
        extra = "allow"
