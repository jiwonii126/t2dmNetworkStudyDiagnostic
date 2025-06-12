myRunConceptSetDiagnostics <- function(
    connection,
    tempEmulationSchema,
    cdmDatabaseSchema,
    vocabularyDatabaseSchema = cdmDatabaseSchema,
    databaseId,
    cohorts,
    runIncludedSourceConcepts,
    runOrphanConcepts,
    runBreakdownIndexEvents,
    exportFolder,
    minCellCount,
    conceptCountsDatabaseSchema = cdmDatabaseSchema,
    conceptCountsTable = "concept_counts",
    conceptCountsTableIsTemp = FALSE,
    cohortDatabaseSchema,
    cohortTable,
    useExternalConceptCountsTable = FALSE,
    incremental = FALSE,
    conceptIdTable = NULL,
    recordKeepingFile
) {

  ParallelLogger::logInfo("Starting concept set diagnostics")
  startConceptSetDiagnostics <- Sys.time()
  subset <- dplyr::tibble()
  if (runIncludedSourceConcepts) {
    subsetIncluded <- CohortDiagnostics:::subsetToRequiredCohorts(cohorts = cohorts, 
                                              task = "runIncludedSourceConcepts", incremental = incremental, 
                                              recordKeepingFile = recordKeepingFile)
    subset <- dplyr::bind_rows(subset, subsetIncluded)
  }
  if (runBreakdownIndexEvents) {
    subsetBreakdown <- CohortDiagnostics:::subsetToRequiredCohorts(cohorts = cohorts, 
                                               task = "runBreakdownIndexEvents", incremental = incremental, 
                                               recordKeepingFile = recordKeepingFile)
    subset <- dplyr::bind_rows(subset, subsetBreakdown)
  }
  if (runOrphanConcepts) {
    subsetOrphans <- CohortDiagnostics:::subsetToRequiredCohorts(cohorts = cohorts, 
                                             task = "runOrphanConcepts", incremental = incremental, 
                                             recordKeepingFile = recordKeepingFile)
    subset <- dplyr::bind_rows(subset, subsetOrphans)
  }
  subset <- dplyr::distinct(subset)
  if (nrow(subset) == 0) {
    return()
  }
  conceptSets <- CohortDiagnostics:::combineConceptSetsFromCohorts(subset)
  
  print("conceptSets")
  print(str(conceptSets))   
  print(head(conceptSets))  
  
  
  if (is.null(conceptSets)) {
    ParallelLogger::logInfo("Cohorts being diagnosed does not have concept ids. Skipping concept set diagnostics.")
    return(NULL)
  }
  

  
  CohortDiagnostics:::writeToCsv(data = conceptSets %>% dplyr::select(-.data$uniqueConceptSetId), 
             fileName = file.path(exportFolder, "concept_sets.csv"), 
             incremental = incremental, cohortId = conceptSets$cohortId)
  uniqueConceptSets <- conceptSets[!duplicated(conceptSets$uniqueConceptSetId), 
  ] %>% dplyr::select(-.data$cohortId, -.data$conceptSetId)
  instantiateUniqueConceptSets(uniqueConceptSets = uniqueConceptSets, 
                               connection = connection, cdmDatabaseSchema = cdmDatabaseSchema, 
                               vocabularyDatabaseSchema = vocabularyDatabaseSchema, 
                               tempEmulationSchema = tempEmulationSchema, conceptSetsTable = "#inst_concept_sets")
  if ((runIncludedSourceConcepts && nrow(subsetIncluded) > 
       0) || (runOrphanConcepts && nrow(subsetOrphans) > 0)) {
    createConceptCountsTable(connection = connection, cdmDatabaseSchema = cdmDatabaseSchema, 
                             tempEmulationSchema = tempEmulationSchema, conceptCountsDatabaseSchema = conceptCountsDatabaseSchema, 
                             conceptCountsTable = conceptCountsTable, conceptCountsTableIsTemp = conceptCountsTableIsTemp)
  }
  if (runIncludedSourceConcepts) {
    ParallelLogger::logInfo("Fetching included source concepts")
    if (incremental && (nrow(cohorts) - nrow(subsetIncluded)) > 
        0) {
      ParallelLogger::logInfo(sprintf("Skipping %s cohorts in incremental mode.", 
                                      nrow(cohorts) - nrow(subsetIncluded)))
    }
    if (nrow(subsetIncluded) > 0) {
      start <- Sys.time()
      if (useExternalConceptCountsTable) {
        stop("Use of external concept count table is not supported")
      }
      else {
        sql <- SqlRender::loadRenderTranslateSql("CohortSourceCodes.sql", 
                                                 packageName = utils::packageName(), dbms = connection@dbms, 
                                                 tempEmulationSchema = tempEmulationSchema, 
                                                 cdm_database_schema = cdmDatabaseSchema, instantiated_concept_sets = "#inst_concept_sets", 
                                                 include_source_concept_table = "#inc_src_concepts", 
                                                 by_month = FALSE)
        DatabaseConnector::executeSql(connection = connection, 
                                      sql = sql)
        counts <- DatabaseConnector::renderTranslateQuerySql(connection = connection, 
                                                             sql = "SELECT * FROM @include_source_concept_table;", 
                                                             include_source_concept_table = "#inc_src_concepts", 
                                                             tempEmulationSchema = tempEmulationSchema, 
                                                             snakeCaseToCamelCase = TRUE) %>% tidyr::tibble()
        counts <- counts %>% dplyr::rename(uniqueConceptSetId = .data$conceptSetId) %>% 
          dplyr::inner_join(conceptSets %>% dplyr::select(.data$uniqueConceptSetId, 
                                                          .data$cohortId, .data$conceptSetId), by = "uniqueConceptSetId") %>% 
          dplyr::select(-.data$uniqueConceptSetId) %>% 
          dplyr::mutate(databaseId = !!databaseId) %>% 
          dplyr::relocate(.data$databaseId, .data$cohortId, 
                          .data$conceptSetId, .data$conceptId) %>% 
          dplyr::distinct()
        if (nrow(counts) > 0) {
          counts$databaseId <- databaseId
          counts <- enforceMinCellValue(counts, "conceptSubjects", 
                                        minCellCount)
          counts <- enforceMinCellValue(counts, "conceptCount", 
                                        minCellCount)
        }
        writeToCsv(counts, file.path(exportFolder, "included_source_concept.csv"), 
                   incremental = incremental, cohortId = subsetIncluded$cohortId)
        recordTasksDone(cohortId = subsetIncluded$cohortId, 
                        task = "runIncludedSourceConcepts", checksum = subsetIncluded$checksum, 
                        recordKeepingFile = recordKeepingFile, incremental = incremental)
        if (!is.null(conceptIdTable)) {
          sql <- "INSERT INTO @concept_id_table (concept_id)\n                  SELECT DISTINCT concept_id\n                  FROM @include_source_concept_table;\n\n                  INSERT INTO @concept_id_table (concept_id)\n                  SELECT DISTINCT source_concept_id\n                  FROM @include_source_concept_table;"
          DatabaseConnector::renderTranslateExecuteSql(connection = connection, 
                                                       sql = sql, tempEmulationSchema = tempEmulationSchema, 
                                                       concept_id_table = conceptIdTable, include_source_concept_table = "#inc_src_concepts", 
                                                       progressBar = FALSE, reportOverallTime = FALSE)
        }
        sql <- "TRUNCATE TABLE @include_source_concept_table;\nDROP TABLE @include_source_concept_table;"
        DatabaseConnector::renderTranslateExecuteSql(connection = connection, 
                                                     sql = sql, tempEmulationSchema = tempEmulationSchema, 
                                                     include_source_concept_table = "#inc_src_concepts", 
                                                     progressBar = FALSE, reportOverallTime = FALSE)
        delta <- Sys.time() - start
        ParallelLogger::logInfo(paste("Finding source codes took", 
                                      signif(delta, 3), attr(delta, "units")))
      }
    }
  }
  if (runBreakdownIndexEvents) {
    ParallelLogger::logInfo("Breaking down index events")
    if (incremental && (nrow(cohorts) - nrow(subsetBreakdown)) > 
        0) {
      ParallelLogger::logInfo(sprintf("Skipping %s cohorts in incremental mode.", 
                                      nrow(cohorts) - nrow(subsetBreakdown)))
    }
    if (nrow(subsetBreakdown) > 0) {
      start <- Sys.time()
      domains <- readr::read_csv(system.file("csv", 
                                             "domains.csv", package = utils::packageName()), 
                                 col_types = readr::cols(), guess_max = min(1e+07))
      runBreakdownIndexEvents <- function(cohort) {
        ParallelLogger::logInfo("- Breaking down index events for cohort '", 
                                cohort$cohortName, "'")
        cohortDefinition <- RJSONIO::fromJSON(cohort$json, 
                                              digits = 23)
        primaryCodesetIds <- lapply(cohortDefinition$PrimaryCriteria$CriteriaList, 
                                    getCodeSetIds) %>% dplyr::bind_rows()
        if (nrow(primaryCodesetIds) == 0) {
          warning("No primary event criteria concept sets found for cohort id: ", 
                  cohort$cohortId)
          return(tidyr::tibble())
        }
        primaryCodesetIds <- primaryCodesetIds %>% dplyr::filter(.data$domain %in% 
                                                                   c(domains$domain %>% unique()))
        if (nrow(primaryCodesetIds) == 0) {
          warning("Primary event criteria concept sets found for cohort id: ", 
                  cohort$cohortId, " but,", "\nnone of the concept sets belong to the supported domains.", 
                  "\nThe supported domains are:\n", paste(domains$domain, 
                                                          collapse = ", "))
          return(tidyr::tibble())
        }
        primaryCodesetIds <- conceptSets %>% dplyr::filter(.data$cohortId %in% 
                                                             cohort$cohortId) %>% dplyr::select(codeSetIds = .data$conceptSetId, 
                                                                                                .data$uniqueConceptSetId) %>% dplyr::inner_join(primaryCodesetIds, 
                                                                                                                                                by = "codeSetIds")
        pasteIds <- function(row) {
          return(dplyr::tibble(domain = row$domain[1], 
                               uniqueConceptSetId = paste(row$uniqueConceptSetId, 
                                                          collapse = ", ")))
        }
        primaryCodesetIds <- lapply(split(primaryCodesetIds, 
                                          primaryCodesetIds$domain), pasteIds)
        primaryCodesetIds <- dplyr::bind_rows(primaryCodesetIds)
        getCounts <- function(row) {
          domain <- domains[domains$domain == row$domain, 
          ]
          sql <- SqlRender::loadRenderTranslateSql("CohortEntryBreakdown.sql", 
                                                   packageName = utils::packageName(), dbms = connection@dbms, 
                                                   tempEmulationSchema = tempEmulationSchema, 
                                                   cdm_database_schema = cdmDatabaseSchema, 
                                                   vocabulary_database_schema = vocabularyDatabaseSchema, 
                                                   cohort_database_schema = cohortDatabaseSchema, 
                                                   cohort_table = cohortTable, cohort_id = cohort$cohortId, 
                                                   domain_table = domain$domainTable, domain_start_date = domain$domainStartDate, 
                                                   domain_concept_id = domain$domainConceptId, 
                                                   domain_source_concept_id = domain$domainSourceConceptId, 
                                                   use_source_concept_id = !(is.na(domain$domainSourceConceptId) | 
                                                                               is.null(domain$domainSourceConceptId)), 
                                                   primary_codeset_ids = row$uniqueConceptSetId, 
                                                   concept_set_table = "#inst_concept_sets", 
                                                   store = TRUE, store_table = "#breakdown")
          DatabaseConnector::executeSql(connection = connection, 
                                        sql = sql, progressBar = FALSE, reportOverallTime = FALSE)
          sql <- "SELECT * FROM @store_table;"
          counts <- DatabaseConnector::renderTranslateQuerySql(connection = connection, 
                                                               sql = sql, tempEmulationSchema = tempEmulationSchema, 
                                                               store_table = "#breakdown", snakeCaseToCamelCase = TRUE) %>% 
            tidyr::tibble()
          if (!is.null(conceptIdTable)) {
            sql <- "INSERT INTO @concept_id_table (concept_id)\n                  SELECT DISTINCT concept_id\n                  FROM @store_table;"
            DatabaseConnector::renderTranslateExecuteSql(connection = connection, 
                                                         sql = sql, tempEmulationSchema = tempEmulationSchema, 
                                                         concept_id_table = conceptIdTable, store_table = "#breakdown", 
                                                         progressBar = FALSE, reportOverallTime = FALSE)
          }
          sql <- "TRUNCATE TABLE @store_table;\nDROP TABLE @store_table;"
          DatabaseConnector::renderTranslateExecuteSql(connection = connection, 
                                                       sql = sql, tempEmulationSchema = tempEmulationSchema, 
                                                       store_table = "#breakdown", progressBar = FALSE, 
                                                       reportOverallTime = FALSE)
          return(counts)
        }
        counts <- lapply(split(primaryCodesetIds, 1:nrow(primaryCodesetIds)), 
                         getCounts) %>% dplyr::bind_rows() %>% dplyr::arrange(.data$conceptCount)
        if (nrow(counts) > 0) {
          counts$cohortId <- cohort$cohortId
        }
        else {
          ParallelLogger::logInfo("Index event breakdown results were not returned for: ", 
                                  cohort$cohortId)
          return(dplyr::tibble())
        }
        return(counts)
      }
      data <- lapply(split(subsetBreakdown, subsetBreakdown$cohortId), 
                     runBreakdownIndexEvents)
      data <- dplyr::bind_rows(data)
      if (nrow(data) > 0) {
        data <- data %>% dplyr::mutate(databaseId = !!databaseId)
        data <- enforceMinCellValue(data, "conceptCount", 
                                    minCellCount)
        if ("subjectCount" %in% colnames(data)) {
          data <- enforceMinCellValue(data, "subjectCount", 
                                      minCellCount)
        }
      }
      writeToCsv(data = data, fileName = file.path(exportFolder, 
                                                   "index_event_breakdown.csv"), incremental = incremental, 
                 cohortId = subset$cohortId)
      recordTasksDone(cohortId = subset$cohortId, task = "runBreakdownIndexEvents", 
                      checksum = subset$checksum, recordKeepingFile = recordKeepingFile, 
                      incremental = incremental)
      delta <- Sys.time() - start
      ParallelLogger::logInfo(paste("Breaking down index event took", 
                                    signif(delta, 3), attr(delta, "units")))
    }
  }
  if (runOrphanConcepts) {
    ParallelLogger::logInfo("Finding orphan concepts")
    if (incremental && (nrow(cohorts) - nrow(subsetOrphans)) > 
        0) {
      ParallelLogger::logInfo(sprintf("Skipping %s cohorts in incremental mode.", 
                                      nrow(cohorts) - nrow(subsetOrphans)))
    }
    if (nrow(subsetOrphans > 0)) {
      start <- Sys.time()
      if (!useExternalConceptCountsTable) {
        ParallelLogger::logTrace("Using internal concept count table.")
      }
      else {
        stop("Use of external concept count table is not supported")
      }
      data <- list()
      for (i in (1:nrow(uniqueConceptSets))) {
        conceptSet <- uniqueConceptSets[i, ]
        ParallelLogger::logInfo("- Finding orphan concepts for concept set '", 
                                conceptSet$conceptSetName, "'")
        data[[i]] <- .findOrphanConcepts(connection = connection, 
                                         cdmDatabaseSchema = cdmDatabaseSchema, tempEmulationSchema = tempEmulationSchema, 
                                         useCodesetTable = TRUE, codesetId = conceptSet$uniqueConceptSetId, 
                                         conceptCountsDatabaseSchema = conceptCountsDatabaseSchema, 
                                         conceptCountsTable = conceptCountsTable, conceptCountsTableIsTemp = conceptCountsTableIsTemp, 
                                         instantiatedCodeSets = "#inst_concept_sets", 
                                         orphanConceptTable = "#orphan_concepts")
        if (!is.null(conceptIdTable)) {
          sql <- "INSERT INTO @concept_id_table (concept_id)\n                  SELECT DISTINCT concept_id\n                  FROM @orphan_concept_table;"
          DatabaseConnector::renderTranslateExecuteSql(connection = connection, 
                                                       sql = sql, tempEmulationSchema = tempEmulationSchema, 
                                                       concept_id_table = conceptIdTable, orphan_concept_table = "#orphan_concepts", 
                                                       progressBar = FALSE, reportOverallTime = FALSE)
        }
        sql <- "TRUNCATE TABLE @orphan_concept_table;\nDROP TABLE @orphan_concept_table;"
        DatabaseConnector::renderTranslateExecuteSql(connection = connection, 
                                                     sql = sql, tempEmulationSchema = tempEmulationSchema, 
                                                     orphan_concept_table = "#orphan_concepts", 
                                                     progressBar = FALSE, reportOverallTime = FALSE)
      }
      data <- dplyr::bind_rows(data) %>% dplyr::distinct() %>% 
        dplyr::rename(uniqueConceptSetId = .data$codesetId) %>% 
        dplyr::inner_join(conceptSets %>% dplyr::select(.data$uniqueConceptSetId, 
                                                        .data$cohortId, .data$conceptSetId), by = "uniqueConceptSetId") %>% 
        dplyr::select(-.data$uniqueConceptSetId) %>% 
        dplyr::mutate(databaseId = !!databaseId) %>% 
        dplyr::relocate(.data$cohortId, .data$conceptSetId, 
                        .data$databaseId)
      if (nrow(data) > 0) {
        data <- enforceMinCellValue(data, "conceptCount", 
                                    minCellCount)
        data <- enforceMinCellValue(data, "conceptSubjects", 
                                    minCellCount)
      }
      writeToCsv(data, file.path(exportFolder, "orphan_concept.csv"), 
                 incremental = incremental, cohortId = subsetOrphans$cohortId)
      recordTasksDone(cohortId = subsetOrphans$cohortId, 
                      task = "runOrphanConcepts", checksum = subsetOrphans$checksum, 
                      recordKeepingFile = recordKeepingFile, incremental = incremental)
      delta <- Sys.time() - start
      ParallelLogger::logInfo("Finding orphan concepts took ", 
                              signif(delta, 3), " ", attr(delta, "units"))
    }
  }
  DatabaseConnector::renderTranslateExecuteSql(connection = connection, 
                                               sql = "INSERT INTO #concept_ids (concept_id)\n            SELECT DISTINCT concept_id\n            FROM #inst_concept_sets;", 
                                               tempEmulationSchema = tempEmulationSchema, progressBar = FALSE, 
                                               reportOverallTime = FALSE)
  resolvedConceptIds <- DatabaseConnector::renderTranslateQuerySql(connection = connection, 
                                                                   sql = "SELECT *\n                                                      FROM #inst_concept_sets;", 
                                                                   tempEmulationSchema = tempEmulationSchema, snakeCaseToCamelCase = TRUE) %>% 
    dplyr::tibble() %>% dplyr::rename(uniqueConceptSetId = .data$codesetId) %>% 
    dplyr::inner_join(conceptSets, by = "uniqueConceptSetId") %>% 
    dplyr::select(.data$cohortId, .data$conceptSetId, .data$conceptId) %>% 
    dplyr::mutate(databaseId = !!databaseId) %>% dplyr::distinct()
  writeToCsv(resolvedConceptIds, file.path(exportFolder, "resolved_concepts.csv"), 
             incremental = TRUE)
  ParallelLogger::logTrace("Dropping temp concept set table")
  sql <- "TRUNCATE TABLE #inst_concept_sets; DROP TABLE #inst_concept_sets;"
  DatabaseConnector::renderTranslateExecuteSql(connection, 
                                               sql, tempEmulationSchema = tempEmulationSchema, progressBar = FALSE, 
                                               reportOverallTime = FALSE)
  if ((runIncludedSourceConcepts && nrow(subsetIncluded) > 
       0) || (runOrphanConcepts && nrow(subsetOrphans) > 0)) {
    ParallelLogger::logTrace("Dropping temp concept count table")
    if (conceptCountsTableIsTemp) {
      countTable <- conceptCountsTable
    }
    else {
      countTable <- paste(conceptCountsDatabaseSchema, 
                          conceptCountsTable, sep = ".")
    }
    sql <- "TRUNCATE TABLE @count_table; DROP TABLE @count_table;"
    DatabaseConnector::renderTranslateExecuteSql(connection, 
                                                 sql, tempEmulationSchema = tempEmulationSchema, count_table = countTable, 
                                                 progressBar = FALSE, reportOverallTime = FALSE)
  }
  delta <- Sys.time() - startConceptSetDiagnostics
  ParallelLogger::logInfo(
    paste0("Running concept set diagnostics took ", signif(delta, 3), " ", attr(delta, "units"))
)
  
}