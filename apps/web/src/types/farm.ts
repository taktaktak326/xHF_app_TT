export type LoginAndTokenResp = {
  ok: boolean;
  login: {
    login_token: string;
    gigya_uuid: string;
    gigya_uuid_signature: string;
    gigya_signature_timestamp: string;
  };
  api_token: string;
};

export type Crop = {
  uuid?: string;
  name: string;
};

export type Variety = {
  name: string;
};

export type ActiveGrowthStage = {
  index: string;
  gsOrder: number;
  scale: string;
};

export type Recommendation = {
  startDate: string;
  endDate: string;
  status: string;
  actionType: string;
  description?: string;
  confidenceLevel?: string;
};

export type SubstanceApplicationRate = {
  uuid?: string;
  name?: string | null;
  type?: string | null;
  averageApplicationRate?: number | null;
  totalApplication?: number | null;
  formulation?: string | null;
  unit?: string | null;
  localUnitUuid?: string | null;
  volumeBasisLocalUnitUuid?: string | null;
  volumeBasisRate?: number | null;
  dilutionFactor?: number | null;
  ratesPerZone?: number[] | null;
  ratesByZone?: { id?: string | null; rate?: number | null }[] | null;
};

export type FieldNote = {
  uuid: string;
  note: string;
  categories?: string[] | null;
  creationDate: string;
  creator?: {
    firstName: string;
    lastName: string;
  } | null;
  attachments?: {
    uuid: string;
    url: string;
    fileName?: string | null;
    mimeType?: string | null;
  }[] | null;
  audioAttachments?: {
    url: string;
    contentType?: string | null;
    fileName?: string | null;
  }[] | null;
  locationType?: string | null;
  location?: {
    type?: string;
    coordinates?: [number, number];
  } | null;
  region?: string | null;
  cropSeason?: {
    uuid: string;
  } | null;
};

export type Risk = {
  startDate: string;
  endDate: string;
  status: string;
  __typename: string;
  stressV2: {
    uuid: string;
    __typename: string;
  };
};

export type TimingStressInfo = {
  stressV2: {
    uuid: string;
    stressTypeCode: string;
    name: string;
    __typename: string;
  };
};

export type ActionWindow = {
  startDate: string;
  endDate: string;
  actionType: string;
  status: string;
};

export type CropSeasonStatus = {
  startDate: string;
  endDate: string;
  status: string;
  type: string;
};

export type CountryCropGrowthStagePrediction = {
  index: string;
  startDate: string;
  endDate: string;
  scale: string;
  gsOrder: number;
  cropGrowthStageV2: { name: string };
};

// TasksPage.tsx で使用されるタスクの型定義
export type Assignee = {
  firstName: string;
  lastName: string;
};

export type DosedMapInfo = {
  creationFlowHint?: string | null;
  applicationType?: string | null;
  recipeV2?: SubstanceApplicationRate[] | null;
};

export type BaseTask = {
  uuid: string;
  plannedDate: string | null;
  executionDate: string | null;
  state: string;
  note: string | null;
  assignee: Assignee | null;
  creationFlowHint?: string | null;
  applicationType?: string | null;
  dosedMap?: DosedMapInfo | null;
};

export type CropSeason = {
  uuid: string;
  startDate: string;
  yield?: number | null;
  yieldExpectation?: number | null;
  crop: Crop;
  variety: Variety;
  activeGrowthStage: ActiveGrowthStage | null;
  actionRecommendations: Recommendation[] | null;
  nutritionRecommendations: Recommendation[] | null;
  weedManagementRecommendations: Recommendation[] | null;
  waterRecommendations: Recommendation[] | null;
  actionWindows: ActionWindow[] | null;
  risks: Risk[] | null;
  timingStressesInfo: TimingStressInfo[] | null;
  cropSeasonStatus: CropSeasonStatus[] | null;
  nutritionStatus: CropSeasonStatus[] | null;
  waterStatus: CropSeasonStatus[] | null;
  cropEstablishmentGrowthStageIndex: string | null;
  cropEstablishmentMethodCode: string | null;
  countryCropGrowthStagePredictions: CountryCropGrowthStagePrediction[] | null;
  // タスク関連のプロパティを追加
  harvests?: BaseTask[] | null;
  sprayingsV2?: BaseTask[] | null;
  waterManagementTasks?: BaseTask[] | null;
  scoutingTasks?: BaseTask[] | null;
  landPreparations?: BaseTask[] | null;
  seedTreatmentTasks?: BaseTask[] | null;
  seedBoxTreatments?: BaseTask[] | null;
};

export type Field = {
  uuid: string;
  name: string;
  area: number;
  farm?: { uuid?: string | null; name?: string | null } | null;
  farmV2?: { uuid?: string | null; name?: string | null } | null;
  cropSeasonsV2: CropSeason[] | null;
  // 作付タスク
  cropEstablishments?: BaseTask[] | null;
  fieldNotes?: FieldNote[] | null;
  location?: FieldLocation | null;
};

export type CombinedOut = {
  ok: boolean;
  status: number;
  request: { url: string; headers: Record<string, string>; payload: unknown };
  response?: any;
  source?: 'api' | 'cache';
  response_text?: string;
  warmup?: {
    state: 'idle' | 'running' | 'success' | 'failed';
    loaded: boolean;
    entryCount: number;
    error?: string | null;
  };
  locationEnrichmentPending?: boolean;
  _sub_responses?: any;
};

export type FieldSeasonPair = {
  field: Field;
  season: CropSeason | null;
  nextStage?: CountryCropGrowthStagePrediction | null;
};

// 各タスクにユニークな 'type' プロパティを付与
export type HarvestTask = BaseTask & { type: 'Harvest' };
export type SprayingTask = BaseTask & { type: 'Spraying' };
export type WaterManagementTask = BaseTask & { type: 'WaterManagement' };
export type ScoutingTask = BaseTask & { type: 'Scouting' };
export type CropEstablishmentTask = BaseTask & { type: 'CropEstablishment' };
export type LandPreparationTask = BaseTask & { type: 'LandPreparation' };
export type SeedTreatmentTask = BaseTask & { type: 'SeedTreatment' };
export type SeedBoxTreatmentTask = BaseTask & { type: 'SeedBoxTreatment' };

export type AggregatedTask = (
  | HarvestTask
  | SprayingTask
  | WaterManagementTask
  | ScoutingTask
  | CropEstablishmentTask
  | LandPreparationTask
  | SeedTreatmentTask
  | SeedBoxTreatmentTask
) & {
  fieldName: string;
  fieldUuid: string;
  cropName: string;
  cropUuid?: string | null;
  seasonStartDate: string;
  seasonUuid?: string | null;
  fieldArea: number;
  farmUuid?: string | null;
  farmName?: string | null;
  creationFlowHint?: string | null;
  dosedMap?: DosedMapInfo | null;
};

export type FieldLocation = {
  center: {
    latitude: number;
    longitude: number;
  };
  centerSource?: string | null;
  prefecture?: string | null;
  prefectureOffice?: string | null;
  municipality?: string | null;
  subMunicipality?: string | null;
  cityCode?: string | null;
  label?: string | null;
  isApproximate?: boolean;
};
