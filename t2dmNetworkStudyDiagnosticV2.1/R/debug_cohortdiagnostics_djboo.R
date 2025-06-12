execute <- function(connectionDetails,
                    cdmDatabaseSchema,
                    vocabularyDatabaseSchema = cdmDatabaseSchema,
                    cohortDatabaseSchema = cdmDatabaseSchema,
                    cohortTable = "cohort",
                    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                    verifyDependencies = TRUE,
                    outputFolder,
                    incrementalFolder = file.path(outputFolder, "incrementalFolder"),
                    databaseId = "Unknown",
                    databaseName = databaseId,
                    databaseDescription = databaseId) {
  if (!file.exists(outputFolder)) {
    dir.create(outputFolder, recursive = TRUE)
  }
  
  ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
  ParallelLogger::addDefaultErrorReportLogger(file.path(outputFolder, "errorReportR.txt"))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
  on.exit(
    ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE),
    add = TRUE
  )
  
  if (verifyDependencies) {
    ParallelLogger::logInfo("Checking whether correct package versions are installed")
    verifyDependencies()
  }
  
  ParallelLogger::logInfo("Creating cohorts")
  
  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)
  
  # Next create the tables on the database
  CohortGenerator::createCohortTables(
    connectionDetails = connectionDetails,
    cohortTableNames = cohortTableNames,
    cohortDatabaseSchema = cohortDatabaseSchema,
    incremental = TRUE
  )
  
  # get cohort definitions from study package
  cohortDefinitionSet <-
    dplyr::tibble(
      CohortGenerator::getCohortDefinitionSet(
        settingsFileName = "settings/CohortsToCreate.csv",
        jsonFolder = "cohorts",
        sqlFolder = "sql/sql_server",
        packageName = "t2dmNetworkStudy",
        cohortFileNameValue = "cohortId"
      )
    )
  
  # Generate the cohort set
  CohortGenerator::generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    incrementalFolder = incrementalFolder,
    incremental = TRUE
  )
  
  # export stats table to local
  CohortGenerator::exportCohortStatsTables(
    connectionDetails = connectionDetails,
    connection = NULL,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortStatisticsFolder = outputFolder,
    incremental = TRUE
  )
  
  # run cohort diagnostics
  CohortDiagnostics::executeDiagnostics(
    cohortDefinitionSet = cohortDefinitionSet,
    exportFolder = outputFolder,
    databaseId = databaseId,
    connectionDetails = connectionDetails,
    connection = NULL,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortTableNames = cohortTableNames,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    cohortIds = NULL,
    inclusionStatisticsFolder = outputFolder,
    databaseName = databaseName,
    databaseDescription = databaseDescription,
    cdmVersion = 5,
    runInclusionStatistics = TRUE,
    runIncludedSourceConcepts = FALSE,
    runOrphanConcepts = FALSE,
    runTimeDistributions = TRUE,
    runVisitContext = TRUE,
    runBreakdownIndexEvents = TRUE,
    runIncidenceRate = TRUE,
    runTimeSeries = FALSE,
    runCohortOverlap = TRUE,
    runCohortCharacterization = TRUE,
    covariateSettings = FeatureExtraction::createDefaultCovariateSettings(),
    runTemporalCohortCharacterization = TRUE,
    temporalCovariateSettings = FeatureExtraction::createTemporalCovariateSettings(
      useConditionOccurrence =
        TRUE,
      useDrugEraStart = TRUE,
      useProcedureOccurrence = TRUE,
      useMeasurement = TRUE,
      temporalStartDays = c(-365, -30, 0, 1, 31),
      temporalEndDays = c(-31, -1, 0, 30, 365)
    ),
    minCellCount = 5,
    incremental = TRUE,
    incrementalFolder = incrementalFolder
  )
  
  # drop cohort stats table
  CohortGenerator::dropCohortStatsTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    connection = NULL
  )
}
