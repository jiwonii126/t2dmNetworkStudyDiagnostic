# Setting -----------------------------------------------------------------

# Set your R library path to include the packages needed for Cohort Diagnostics.
.libPaths("C:/your/R/Path")  
library(CohortDiagnostics)

# connection details -------------------------------------------------------
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "sql server",  # fill your own
  server = "your_server_ip_or_name", # fill your own
  user = "your_nickname", # fill your own
  password = "your_password", # fill your own
  port = yourportnumber,  # fill your own
  pathToDriver = "C:/path_to_jdbc_drivers"  # fill your own
)

connection <- DatabaseConnector::connect(connectionDetails)

currentDb <- DatabaseConnector::querySql(connection, "SELECT DB_NAME() as database_name")
print(currentDb)

# bring the package -------------------------------------------------------
library(t2dmNetworkStudy)

# Optional: specify where the temporary files (used by the Andromeda package) will be created:
options(andromedaTempFolder = "C:/your_own_path_to_andromeda/andromedaTemp")

# Maximum number of cores to be used:
maxCores <- parallel::detectCores()

# The folder where the study intermediate and result files will be written:
outputFolder <- "C:/your_output_folder"  # fill your own


# The name of the database schema where the CDM data can be found:
cdmDatabaseSchema <-"your_cdm_database_schema.cdm"  # fill your own (.cdm)

# The name of the database schema and table where the study-specific cohorts will be instantiated:
cohortDatabaseSchema <- "your_cohort_db_schmea" # fill your own (.dbo)
cohortTable <- "create_your_table"  # fill your own 


# Some meta-information that will be used by the export function:
databaseId <- "your_db_id"  # fill your own 
databaseName <-
  "your_db_name"  # fill your own 
databaseDescription <-
  "OHDSI global network study type 2 diabetes mellitus cohort characterization"   # fill your own 

# For some database platforms (e.g. Oracle): define a schema that can be used to emulate temp tables:
options(sqlRenderTempEmulationSchema = NULL)


# execution ---------------------------------------------------------------
t2dmNetworkStudy::execute(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  verifyDependencies = FALSE,
  outputFolder = outputFolder,
  databaseId = databaseId,
  databaseName = databaseName,
  databaseDescription = databaseDescription
)

# export the results and connection to Shiny ------------------------------
CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = outputFolder)

CohortDiagnostics::launchDiagnosticsExplorer(dataFolder = outputFolder)


# Upload the results to the OHDSI SFTP server:
privateKeyFileName <- ""
userName <- ""
t2dmNetworkStudy::uploadResults(outputFolder, privateKeyFileName, userName)


