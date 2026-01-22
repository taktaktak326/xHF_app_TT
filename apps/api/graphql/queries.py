#GraphQLクエリ文字列の置き場（FarmsOverviewなどを定義）

FARMS_OVERVIEW = """
query FarmsOverview {
  farms: farmsV2(uuids: []) {
    uuid
    name
    latitude
    longitude
    owner { firstName lastName email }
    currentUserPermission { access }
  }
}
"""

# 参考：変数付きクエリ（スキーマに合わせて調整してください）
FIELDS_BY_FARM = """
query FieldsByFarm($farmUuid: UUID!) {
  fields(farmUuid: $farmUuid) {
    uuid
    name
    # ... 追加のフィールド
  }
}
"""

# 既存の FARMS_OVERVIEW / FIELDS_BY_FARM の下あたりに追記

COMBINED_DATA_BASE = """
query CombinedDataBase(
    $farmUuids: [UUID!]!,
    $languageCode: String!,
    $cropSeasonLifeCycleStates: [LifecycleState]!,
    $withBoundary: Boolean!
) {
  fieldsV2(farmUuids: $farmUuids) {
    uuid
    name
    area
    farmV2 { uuid name latitude longitude owner { firstName lastName email } }
    boundary: boundary @include(if: $withBoundary)
    cropSeasonsV2(lifecycleState: $cropSeasonLifeCycleStates) {
      uuid
      startDate
      yield
      yieldExpectation
      lifecycleState
      crop(languageCode: $languageCode) { uuid name }
      variety(languageCode: $languageCode) { name }
      activeGrowthStage { index gsOrder scale }
      cropEstablishmentDetails { seedBoxPerArea seedWeightPerSeedBox }
      cropEstablishmentGrowthStageIndex
      cropEstablishmentMethodCode
      cropSeasonNutritionProgram {
        uuid
        nutritionTargetRates { nutrientUuid rate }
      }
      cropSeasonNutritionProgramExecutionStatus {
        sprayingNutrients { sprayingUuid nutrientUuid rate amount fertilizerTypeUuid }
        totalNutrients {
          nutrientUuid
          amount
          rate
          targetAmountDelta
          targetRateDelta
        }
      }
      timingStressesInfo {
        stressV2(languageCode: $languageCode) {
          uuid
          stressTypeCode
          name
          __typename
        }
      }
    }
  }
}
"""

WEATHER_HISTORIC_FORECAST_DAILY = """
query WeatherHistoricForecastDaily($fieldUuid: UUID!, $fromDate: Date!, $tillDate: Date!) {
  fieldV2(uuid: $fieldUuid) {
    weatherHistoricForecastDaily: weatherV2(
      fromDate: $fromDate
      tillDate: $tillDate
      format: DAILY
      type: [HISTORIC, FORECAST]
    ) {
      datetime
      date
      airTempCAvg
      airTempCMax
      airTempCMin
      sunshineDurationH
      precipitationBestMm
      precipitationProbabilityPct
      windSpeedMSAvg
      windDirectionDeg
      relativeHumidityPctAvg
      relativeHumidityPctMax
      relativeHumidityPctMin
      leafWetnessDurationH
    }
  }
}
"""

WEATHER_CLIMATOLOGY_DAILY = """
query WeatherClimatologyDaily($fieldUuid: UUID!, $fromDate: Date!, $tillDate: Date!) {
  fieldV2(uuid: $fieldUuid) {
    weatherClimatologyDaily: weatherV2(
      fromDate: $fromDate
      tillDate: $tillDate
      format: DAILY
      type: [CLIMATOLOGY10Y]
    ) {
      datetime
      date
      airTempCMax
      airTempCAvg
      airTempCMin
    }
  }
}
"""

SPRAY_WEATHER = """
query SprayWeather($fieldUuid: UUID!, $fromDate: Date!, $tillDate: Date!) {
  fieldV2(uuid: $fieldUuid) {
    sprayWeather(fromDate: $fromDate, toDate: $tillDate) {
      fromDate
      toDate
      result
      factors { factor result }
    }
  }
}
"""

WEATHER_HISTORIC_FORECAST_HOURLY = """
query WeatherHistoricForecastHourly($fieldUuid: UUID!, $fromDate: Date!, $tillDate: Date!) {
  fieldV2(uuid: $fieldUuid) {
    weatherHistoricForecastHourly: weatherV2(
      fromDate: $fromDate
      tillDate: $tillDate
      format: HOURLY
      type: [HISTORIC, FORECAST]
    ) {
      datetime
      startDatetime
      endDatetime
      airTempCAvg
      precipitationBestMm
      windSpeedMSAvg
      windDirectionDeg
      relativeHumidityPctAvg
      leafWetnessBool
    }
  }
}
"""

CROP_PROTECTION_TASK_CREATION_PRODUCTS = """
query CropProtectionTaskCreationProducts(
  $farmUuids: [UUID]!,
  $cropUuid: UUID!,
  $countryUuid: UUID!,
  $taskTypeCode: String
) {
  productsV2(
    cropUuid: $cropUuid
    countryUuid: $countryUuid
    farmUuids: $farmUuids
    taskTypeCode: $taskTypeCode
  ) {
    uuid
    code
    name
    registrationNumber
    formulationTypeGroupCode
    organizations { uuid code name }
    taskMethods { uuid taskTypeCode }
    categories { uuid code name }
    features { features vraMinRateSi }
    productFeatures
    minRateSi
    maxRateSi
    minWaterSi
    maxWaterSi
    isCustom
  }
}
"""

FIELD_DATA_LAYER_IMAGES = """
query FieldDataLayerImages($fieldUuid: UUID!, $types: [FieldDataLayerType!]) {
  fieldV2(uuid: $fieldUuid) {
    uuid
    name
    fieldDataLayers(types: $types) {
      uuid
      type
      date
      sourceType
      sourceUuid
      name
      magnitudes {
        type
        imageUrl
        vectorTilesUrl
        vectorTilesStyleUrl
      }
    }
  }
}
"""

COMBINED_FIELD_DATA_TASKS = """
query CombinedFieldData(
    $farmUuids: [UUID!]!,
    $languageCode: String!,
    $cropSeasonLifeCycleStates: [LifecycleState]!,
    $fromDate: Date,
    $tillDate: Date,
    $withrisk: Boolean = false,
    $withCropSeasonsV2: Boolean!,
    $withHarvests: Boolean!,
    $withCropEstablishments: Boolean!,
    $withLandPreparations: Boolean!,
    $withDroneFlights: Boolean!,
    $withSeedTreatments: Boolean!,
    $withSeedBoxTreatments: Boolean!,
    $withSmartSprayingTasks: Boolean!,
    $withWaterManagementTasks: Boolean!,
    $withScoutingTasks: Boolean!,
    $withObservations: Boolean!,
    $withSprayingsV2: Boolean!,
    $withSoilSamplingTasks: Boolean!,
    $withBoundary: Boolean!
) {
  fieldsV2(farmUuids: $farmUuids) {
    uuid
    name
    area
    boundary @include(if: $withBoundary)

    cropSeasonsV2(lifecycleState: $cropSeasonLifeCycleStates) @include(if: $withCropSeasonsV2) {
      uuid
      startDate
      yield
      yieldExpectation
      lifecycleState
      crop(languageCode: $languageCode) { uuid name }
      variety(languageCode: $languageCode) { name }
      activeGrowthStage { index gsOrder scale }
      cropEstablishmentDetails { seedBoxPerArea seedWeightPerSeedBox }

      cropSeasonNutritionProgram {
        uuid
        nutritionTargetRates { nutrientUuid rate }
      }
      cropSeasonNutritionProgramExecutionStatus {
        sprayingNutrients {
          sprayingUuid
          nutrientUuid
          rate
          amount
          fertilizerTypeUuid
        }
        totalNutrients {
          nutrientUuid
          amount
          rate
          targetAmountDelta
          targetRateDelta
        }
      }

      actionRecommendations {
        startDate
        endDate
        status
        actionType: type
        confidenceLevel
      }

      risks(fromDate: $fromDate, tillDate: $tillDate, status: [HIGH, MEDIUM_HIGH, MEDIUM, MEDIUM_LOW, PROTECTED]) @include(if: $withrisk) {
        startDate
        endDate
        status
        __typename
        stressV2 { uuid __typename }
      }

      harvests @include(if: $withHarvests) {
        uuid
        plannedDate
        assignmentState
        assignee { firstName lastName }
        note
        state
        executionDate
        yieldProperties
        yield
        harvestMethodCode
      }

      observations @include(if: $withObservations) {
        uuid
        executionDate
        stressAnswers(language: $languageCode) {
          uuid
          label
          stressQuestion(language: $languageCode) {
            stressV2(languageCode: $languageCode) { name code }
          }
        }
      }

      smartSprayingTasksV2 @include(if: $withSmartSprayingTasks) {
        uuid
        plannedDate
        assignee { firstName lastName }
        note
        executionDate
        state
        dosedMaps {
          tankNumber
          dosedMap { uuid applicationType }
        }
      }

      sprayingsV2 @include(if: $withSprayingsV2) {
        uuid
        autoExecutedOn
        plannedDate
        executionDate
        isAutoExecutable
        note
        assignmentState
        assignee { firstName lastName }
        state
        dosedMap {
          applicationType
          creationFlowHint
          recipeV2 {
            uuid
            name
            type
            totalApplication
            formulation
            unit
            organization
          }
        }
      }

      seedTreatmentTasks @include(if: $withSeedTreatments) {
        uuid
        autoExecutedOn
        plannedDate
        executionDate
        isAutoExecutable
        note
        assignmentState
        assignee { firstName lastName }
        totalLiquidRate
        state
        products { uuid unit applicationRate localUnitUuid dilutionFactor }
        recipe {
          uuid
          name
          type
          totalApplication
          averageApplicationRate
          organization
        }
      }

      seedBoxTreatments @include(if: $withSeedBoxTreatments) {
        uuid
        plannedDate
        executionDate
        autoExecutedOn
        isAutoExecutable
        note
        assignmentState
        assignee { firstName lastName }
        state
        products { uuid unit applicationRate localUnitUuid dilutionFactor }
        recipe {
          uuid
          name
          type
          totalApplication
          averageApplicationRate
          organization
        }
      }

      cropEstablishments @include(if: $withCropEstablishments) {
        uuid
        assignmentState
        assignee { firstName lastName }
        note
        dosedMap {
          applicationMode
          applicationType
          sourceMap { sourceDate }
        }
      }

      landPreparations @include(if: $withLandPreparations) {
        uuid
        plannedDate
        executionDate
        autoExecutedOn
        isAutoExecutable
        note
        assignmentState
        state
        tillageDepth
        processedArea
        assignee { firstName lastName }
      }

      waterManagementTasks @include(if: $withWaterManagementTasks) {
        uuid
        plannedDate
        executionDate
        autoExecutedOn
        assignee { firstName lastName }
        isAutoExecutable
        note
        state
        type
        waterHeightDifference
        waterHeight
        executionStartDate
        fieldCoveragePercentage
      }

      scoutingTasks @include(if: $withScoutingTasks) {
        uuid
        plannedDate
        assignee { firstName lastName }
        note
        executionDate
        state
      }

      droneFlights @include(if: $withDroneFlights) {
        uuid
        status
        executedDate
        assignee { firstName lastName }
        note
        plannedDate
        warningCode
      }

      soilSamplingTasks @include(if: $withSoilSamplingTasks) {
        uuid
        plannedDate
        executionDate
        assignee { firstName lastName }
        note
        state
      }
    }
  }
}
"""

BIOMASS_NDVI = """
query Biomass($uuids: [UUID!]!, $from: Date!, $till: Date!) {
  biomassAnalysisNdvi(
    cropSeasonUuids: $uuids,
    fromDate: $from,
    tillDate: $till
  ) {
    uuid
    average
    cropSeasonUuid
    acquisitionDate
  }
}
"""

BIOMASS_LAI = """
query BiomassLai($uuids: [UUID!]!, $from: Date!, $till: Date!) {
  biomassAnalysis(
    cropSeasonUuids: $uuids,
    fromDate: $from,
    tillDate: $till
  ) {
    uuid
    average
    cropSeasonUuid
    acquisitionDate
  }
}
"""

COMBINED_DATA_INSIGHTS = """
query CombinedDataInsights(
    $farmUuids: [UUID!]!,
    $fromDate: Date!,
    $tillDate: Date!,
    $cropSeasonLifeCycleStates: [LifecycleState]!,
    $withrisk: Boolean = false
) {
  fieldsV2(farmUuids: $farmUuids) {
    uuid
    cropSeasonsV2(lifecycleState: $cropSeasonLifeCycleStates) {
      uuid
      actionRecommendations {
        startDate
        endDate
        status
        actionType: type
        confidenceLevel
      }
      nutritionRecommendations(fromDate: $fromDate, tillDate: $tillDate) {
        startDate
        endDate
        status
        actionType
      }
      waterRecommendations {
        startDate
        endDate
        description
        actionType
      }
      actionWindows(fromDate: $fromDate, tillDate: $tillDate) {
        startDate
        endDate
        actionType
        status
        cropSeasonUuid
      }
      cropSeasonStatus {
        startDate
        endDate
        status
        type
      }
      weedManagementRecommendations {
        startDate
        endDate
        status
        type
        confidenceLevel
      }
      nutritionStatus {
        startDate
        endDate
        status
        __typename
      }
      waterStatus(fromDate: $fromDate, tillDate: $tillDate) {
        startDate
        endDate
        status
        __typename
      }
      risks(fromDate: $fromDate, tillDate: $tillDate, status: [HIGH, MEDIUM_HIGH, MEDIUM, MEDIUM_LOW, PROTECTED]) @include(if: $withrisk) {
        startDate
        endDate
        status
        __typename
        stressV2 { uuid __typename }
      }
    }
  }
}
"""

COMBINED_DATA_PREDICTIONS = """
query CombinedDataPredictions(
    $farmUuids: [UUID!]!,
    $languageCode: String!,
    $countryCode: String!,
    $cropSeasonLifeCycleStates: [LifecycleState]!
) {
  fieldsV2(farmUuids: $farmUuids) {
    uuid
    cropSeasonsV2(lifecycleState: $cropSeasonLifeCycleStates) {
      uuid
      countryCropGrowthStagePredictions {
        index
        startDate
        endDate
        scale
        gsOrder
        cropGrowthStageV2(languageCode: $languageCode, countryCode: $countryCode) { uuid name code }
      }
    }
  }
}
"""

FIELD_NOTES_BY_FARMS = """
query FieldNotesByFarms($farmUuids: [UUID!]!) {
  fieldsV2(farmUuids: $farmUuids) {
    uuid
    name
    fieldNotes {
      uuid
      note
      categories
      creationDate
      locationType
      location
      region
      cropSeason {
        uuid
      }
      attachments {
        uuid
        url
        thumbnailUrl
        contentType
        fileName
      }
      audioAttachments {
        url
        contentType
      }
      creator {
        uuid
        firstName
        lastName
      }
    }
  }
}
"""
