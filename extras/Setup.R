library(magrittr)

overwrite <- TRUE
atlasCohorts <- data.frame(atlasName = c("[EPI_851] New users of beta-blockers",
                  "[EPI_851] New users of ACE-inhibitors",
                  "[EPI_851] New users of thiazides or thiazide-like diuretics",
                  "[EPI_851] Angioedema"),
                  entityType = c("C", "T", "C", "O"))

tryCatch({
    ROhdsiWebApi:::getWebApiVersion(baseUrl = Sys.getenv("WEBAPI_BASE_URL"))
  },
  error = function(e) {
    print(sprintf("Could not interface with WebAPI:\n\t%s", e))
    stop(1)
  })


replaceExpr <- function(x, extras = NULL, extrasReplace = NULL) {
  x <- gsub("^\\[.*\\]\\s+", "", x)
  if(!is.null(extras) && !is.null(extrasReplace) && length(extras) == length(extrasReplace)) {
    for(i in 1:length(extras)) {
      x <- gsub(extras[i], extrasReplace[i], x)
    }
  }
  return(x)
}


cohortsToCreate <- NULL
for (id in 1:nrow(atlasCohorts)) {
  atlasCohort <- atlasCohorts[id, ]
  print(sprintf('checking if cohort %s exists', atlasCohort$atlasName))
  t <- ROhdsiWebApi::existsCohortName(baseUrl = Sys.getenv("WEBAPI_BASE_URL"), cohortName = atlasCohort$atlasName)

  if (tibble::is_tibble(t) && overwrite) {
    cohortDef <- ROhdsiWebApi::getCohortDefinition(baseUrl = Sys.getenv("WEBAPI_BASE_URL"),
                                      cohortId = t$id)
    json <- RJSONIO::toJSON(cohortDef$expression, pretty = TRUE)
    normalizedName <- stringr::str_to_title(gsub("-", " ", t$name))
    normalizedName <- gsub("^\\[.*\\]\\s+|\\s+", "", normalizedName)
    if(grepl("Or", normalizedName)) {
      normalizedName <- strsplit(normalizedName, "Or")[[1]][1]
    }
    #normalizedName <- replaceExpr(stringr::str_to_title(replaceExpr(t$name)), extras = c("-", " "), extrasReplace = c(" ", ""))
    SqlRender::writeSql(json, file.path("inst", "cohorts", paste0(normalizedName, ".json")))
    if(is.null(cohortsToCreate)) {
      cohortsToCreate <- data.frame(atlasId = t$id,
                                    atlasName = t$name,
                                    cohortId = t$id,
                                    name = normalizedName,
                                    entityType = atlasCohort$entityType)
      next
    }
    cohortsToCreate <- rbind(cohortsToCreate, c(t$id, t$name, t$id, normalizedName, atlasCohort$entityType))

  } else {
    print(sprintf('cohort %s does not exist??', atlasCohort$atlasName))
    next
  }
}


readr::write_csv(cohortsToCreate, file.path("inst", "settings", "CohortsToCreate.csv"))

# something up with generating sql?
# ROhdsiWebApi::insertCohortDefinitionSetInPackage(baseUrl = Sys.getenv("WEBAPI_BASE_URL"), insertTableSql = FALSE, insertCohortCreationR = FALSE)


jsonFiles <- list.files(file.path("inst", "cohorts"), pattern = "\\.json$", full.names=TRUE)
options <- CirceR::createGenerateOptions()
for(file in jsonFiles) {
  print(file)
  fileData <- readChar(file, file.info(file)$size)
  j <- CirceR::cohortExpressionFromJson(fileData)
  sql <- CirceR::buildCohortQuery(j, options)
  print(sprintf("writing %s to disc", basename(file)))
  SqlRender::writeSql(sql, file.path("inst", "sql", "sql_server", paste0(gsub("json", "sql", basename(file)))))

}


targetIds <- cohortsToCreate %>%
  dplyr::filter(entityType == "T") %>%
  dplyr::pull(cohortId)
comparatorIds <- cohortsToCreate %>%
  dplyr::filter(entityType == "C") %>%
  dplyr::pull(cohortId)

outcomeIds <- cohortsToCreate %>%
  dplyr::filter(entityType == "O") %>%
  dplyr::pull(cohortId)



print("Creating TCOs of interest")
tcosOfInterest <- expand.grid(targetIds, comparatorIds)
colnames(tcosOfInterest) <- c("targetId", "comparatorId")
tcosOfInterest[, "outcomeIds"] = paste0(outcomeIds, collapse = ";")

readr::write_csv(tcosOfInterest, file.path("inst", "settings", "TcosOfInterest.csv"))


print("Harmonizing negative controls")
negativeControlInit <- readr::read_csv(file.path("inst", "settings", "NegativeControlsInit.csv"), col_types = readr::cols())
negativeControlConcepts <- unique(negativeControlInit$outcomeId)
# ncs <- expand.grid(combn(cohortsToCreate$cohortId, 2, simplify=FALSE), negativeControlConcepts)
# ncs <- cbind(do.call(rbind, ncs$Var1), ncs$Var2)
ncs <- expand.grid(targetIds, comparatorIds, negativeControlConcepts)
colnames(ncs) <- c("targetId", "comparatorId", "outcomeId")
ncs <- merge(ncs, unique(negativeControlInit[, c("outcomeId", "outcomeName", "type")]))
ncs <- ncs[, c(2, 3, 1, 4, 5)]
ncs <- ncs[order(ncs[, 1], ncs[, 2], ncs[, 3]),]



readr::write_csv(ncs, file.path("inst", "settings", "NegativeControls.csv"))



source(file.path("extras", "CreateAnalyses.R"))
