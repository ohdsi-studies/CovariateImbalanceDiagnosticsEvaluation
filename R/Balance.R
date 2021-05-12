# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of CovarBalDiagEval
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.



computeMeansPerGroup <- function(cohorts, cohortMethodData) {

  hasStrata <- "stratumId" %in% colnames(cohorts)

  if (hasStrata) {
    stratumSize <- cohorts %>%
      group_by(.data$stratumId, .data$treatment) %>%
      count() %>%
      ungroup()
  }

  if (hasStrata && any(stratumSize %>% pull(.data$n) > 1)) {
    # Variable strata sizes detected: weigh by size of strata set
    w <- stratumSize %>%
      mutate(weight = 1/.data$n) %>%
      inner_join(cohorts, by = c("stratumId", "treatment")) %>%
      select(.data$rowId, .data$treatment, .data$weight)

    # Normalize so sum(weight) == 1 per treatment arm:
    wSum <- w %>%
      group_by(.data$treatment) %>%
      summarize(wSum = sum(.data$weight, na.rm = TRUE)) %>%
      ungroup()

    cohortMethodData$w <- w %>%
      inner_join(wSum, by = "treatment") %>%
      mutate(weight = .data$weight / .data$wSum) %>%
      select(.data$rowId, .data$treatment, .data$weight)

    # By definition:
    sumW <- 1

    # Note: using abs() because due to rounding to machine precision number can become slightly negative:
    result <- cohortMethodData$covariates %>%
      inner_join(cohortMethodData$w, by = c("rowId")) %>%
      group_by(.data$covariateId, .data$treatment) %>%
      summarise(sum = sum(as.numeric(.data$covariateValue), na.rm = TRUE),
                mean = sum(.data$weight * as.numeric(.data$covariateValue), na.rm = TRUE),
                sumSqr = sum(.data$weight * as.numeric(.data$covariateValue)^2, na.rm = TRUE),
                sumWSqr = sum(.data$weight^2, na.rm = TRUE)) %>%
      mutate(sd = sqrt(abs(.data$sumSqr - .data$mean^2) * sumW/(sumW^2 - .data$sumWSqr))) %>%
      ungroup() %>%
      select(.data$covariateId, .data$treatment, .data$sum, .data$mean, .data$sd) %>%
      collect()

    cohortMethodData$w <- NULL
  } else {
    cohortCounts <- cohorts %>%
      group_by(.data$treatment) %>%
      count()

    result <- cohortMethodData$covariates %>%
      inner_join(select(cohorts, .data$rowId, .data$treatment), by = "rowId") %>%
      group_by(.data$covariateId, .data$treatment) %>%
      summarise(sum = sum(as.numeric(.data$covariateValue), na.rm = TRUE),
                sumSqr = sum(as.numeric(.data$covariateValue)^2, na.rm = TRUE)) %>%
      inner_join(cohortCounts, by = "treatment") %>%
      mutate(sd = sqrt((.data$sumSqr - (.data$sum^2/.data$n))/.data$n),
             mean = .data$sum/.data$n) %>%
      ungroup() %>%
      select(.data$covariateId, .data$treatment, .data$sum, .data$mean, .data$sd) %>%
      collect()
  }
  target <- result %>%
    filter(.data$treatment == 1) %>%
    select(.data$covariateId, sumTarget = .data$sum, meanTarget = .data$mean, sdTarget = .data$sd)

  comparator <- result %>%
    filter(.data$treatment == 0) %>%
    select(.data$covariateId, sumComparator = .data$sum, meanComparator = .data$mean, sdComparator = .data$sd)

  result <- target %>%
    full_join(comparator, by = "covariateId") %>%
    mutate(sd = sqrt((.data$sdTarget^2 + .data$sdComparator^2)/2)) %>%
    select(!c(.data$sdTarget, .data$sdComparator))

  return(result)
}


#' Compute covariate balance before and after matching and trimming
#'
#' @description
#' For every covariate, prevalence in treatment and comparator groups before and after
#' matching/trimming are computed. When variable ratio matching was used the balance score will be
#' corrected according the method described in Austin et al (2008).
#'
#'
#' @param population         A data frame containing the people that are remaining after matching
#'                           and/or trimming.
#' @param subgroupCovariateId  Optional: a covariate ID of a binary covariate that indicates a subgroup of
#'                             interest. Both the before and after populations will be restricted to this
#'                             subgroup before computing covariate balance.
#' @details
#' The population data frame should have the following three columns:
#'
#' - rowId (numeric): A unique identifier for each row (e.g. the person ID).
#' - treatment (integer): Column indicating whether the person is in the target (1) or comparator (0) group.
#' - propensityScore (numeric): Propensity score.
#'
#' @return
#' Returns a tibble describing the covariate balance before and after matching/trimming.
#'
#' @references
#' Austin, P.C. (2008) Assessing balance in measured baseline covariates when using many-to-one
#' matching on the propensity-score. Pharmacoepidemiology and Drug Safety, 17: 1218-1225.
#'
#' @export
computeCovariateBalance <- function(population, cohortMethodData, subgroupCovariateId = NULL, includeBefore = FALSE) {
  ParallelLogger::logTrace("Computing covariate balance")
  start <- Sys.time()

  if (!is.null(subgroupCovariateId)) {
    subGroupCovariate <- cohortMethodData$covariates %>%
      filter(.data$covariateId == subgroupCovariateId) %>%
      collect()

    if (nrow(subGroupCovariate) == 0) {
      stop("Cannot find covariate with ID ", subgroupCovariateId)
    }

    tempCohorts <- cohortMethodData$cohorts %>%
      collect() %>%
      filter(.data$rowId %in% subGroupCovariate$rowId)

    if (nrow(tempCohorts) == 0) {
      stop("Cannot find covariate with ID ", subgroupCovariateId, " in population before matching/trimming")
    }

    sumTreatment <- sum(tempCohorts$treatment)
    if (sumTreatment == 0 || sumTreatment == nrow(tempCohorts)) {
      stop("Subgroup population before matching/trimming doesn't have both target and comparator")
    }

    tempCohortsAfterMatching <- population %>%
      filter(.data$rowId %in% subGroupCovariate$rowId)

    if (nrow(tempCohortsAfterMatching) == 0) {
      stop("Cannot find covariate with ID ", subgroupCovariateId, " in population after matching/trimming")
    }
    sumTreatment <- sum(tempCohortsAfterMatching$treatment)
    if (sumTreatment == 0 || sumTreatment == nrow(tempCohortsAfterMatching)) {
      stop("Subgroup population before matching/trimming doesn't have both target and comparator")
    }

    cohortMethodData$tempCohorts <- tempCohorts %>%
      select(.data$rowId, .data$treatment)

    cohortMethodData$tempCohortsAfterMatching <- tempCohortsAfterMatching %>%
      select(.data$rowId, .data$treatment, .data$stratumId)
  } else {

    if(includeBefore) {
      cohortMethodData$tempCohorts <- cohortMethodData$cohorts %>%
        select(.data$rowId, .data$treatment)
    }

    if(!("stratumId" %in% colnames(population)))
      population[, "stratumId"] <- 1
    cohortMethodData$tempCohortsAfterMatching <- population %>%
      select(.data$rowId, .data$treatment, .data$stratumId)
  }
  if(includeBefore)
    on.exit(cohortMethodData$tempCohorts <- NULL)
  on.exit(cohortMethodData$tempCohortsAfterMatching <- NULL, add = TRUE)

  if(includeBefore)
    beforeMatching <- computeMeansPerGroup(cohortMethodData$tempCohorts, cohortMethodData)
  afterMatching <- computeMeansPerGroup(cohortMethodData$tempCohortsAfterMatching, cohortMethodData)

  if(includeBefore) {
    colnames(beforeMatching)[colnames(beforeMatching) == "meanTarget"] <- "beforeMatchingMeanTarget"
    colnames(beforeMatching)[colnames(beforeMatching) == "meanComparator"] <- "beforeMatchingMeanComparator"
    colnames(beforeMatching)[colnames(beforeMatching) == "sumTarget"] <- "beforeMatchingSumTarget"
    colnames(beforeMatching)[colnames(beforeMatching) == "sumComparator"] <- "beforeMatchingSumComparator"
    colnames(beforeMatching)[colnames(beforeMatching) == "sd"] <- "beforeMatchingSd"
  }
  colnames(afterMatching)[colnames(afterMatching) == "meanTarget"] <- "afterMatchingMeanTarget"
  colnames(afterMatching)[colnames(afterMatching) == "meanComparator"] <- "afterMatchingMeanComparator"
  colnames(afterMatching)[colnames(afterMatching) == "sumTarget"] <- "afterMatchingSumTarget"
  colnames(afterMatching)[colnames(afterMatching) == "sumComparator"] <- "afterMatchingSumComparator"
  colnames(afterMatching)[colnames(afterMatching) == "sd"] <- "afterMatchingSd"

  if(includeBefore) {
    balance <- beforeMatching %>%
      full_join(afterMatching, by = "covariateId") %>%
      inner_join(collect(cohortMethodData$covariateRef), by = "covariateId") %>%
      mutate(beforeMatchingStdDiff = (.data$beforeMatchingMeanTarget - .data$beforeMatchingMeanComparator)/.data$beforeMatchingSd,
             afterMatchingStdDiff = (.data$afterMatchingMeanTarget - .data$afterMatchingMeanComparator)/.data$afterMatchingSd)
  } else {
    balance <- afterMatching %>%
      inner_join(collect(cohortMethodData$covariateRef), by = "covariateId") %>%
      mutate(afterMatchingStdDiff = (.data$afterMatchingMeanTarget - .data$afterMatchingMeanComparator)/.data$afterMatchingSd)
  }

  if(includeBefore)
    balance$beforeMatchingStdDiff[balance$beforeMatchingSd == 0] <- 0
  balance$afterMatchingStdDiff[balance$beforeMatchingSd == 0] <- 0
  if(includeBefore)
    balance <- balance[order(-abs(balance$beforeMatchingStdDiff)), ]
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Computing covariate balance took", signif(delta, 3), attr(delta, "units")))
  return(balance)
}


computeCovariateBalanceFeatureExtraction <- function(population, cohortMethodData) {


  cohortMethodData$pop <- population
  cohortMethodData$popCov <- cohortMethodData$covariates %>%
    inner_join(select(cohortMethodData$pop, .data$rowId, .data$treatment), by = "rowId")
  cohortMethodData$pop <- NULL

  # get covariate data for target
  targetPop <- population %>%
    filter(.data$treatment == 1)
  targetPopCov <- Andromeda::andromeda(covariateRef = cohortMethodData$covariateRef,
                                       analysisRef = cohortMethodData$analysisRef)
  attr(targetPopCov, "metaData") <- attr(cohortMethodData, "metaData")
  class(targetPopCov) <- "CohortMethodData"

  # cohortMethodData$pop <- targetPop %>%
  #   select(.data$rowId)
  # targetPopCov$covariates <- cohortMethodData$pop %>%
  #   inner_join(cohortMethodData$covariates, by = "rowId")
  # cohortMethodData$pop <- NULL
  targetPopCov$covariates <- cohortMethodData$popCov %>%
    filter(.data$treatment == 1)


  attr(targetPopCov, "metaData")$populationSize <- nrow(targetPop)

  # get covariate data for popoulation treatment 1
  comparatorPop <- population %>%
    filter(.data$treatment == 0)
  comparatorPopCov <- Andromeda::andromeda(covariateRef = cohortMethodData$covariateRef,
                                       analysisRef = cohortMethodData$analysisRef)
  attr(comparatorPopCov, "metaData") <- attr(cohortMethodData, "metaData")
  class(comparatorPopCov) <- "CohortMethodData"

  # cohortMethodData$pop <- comparatorPop
  # comparatorPopCov$covariates <- cohortMethodData$covariates %>%
  #   inner_join(cohortMethodData$pop, by = "rowId")
  # cohortMethodData$pop <- NULL
  comparatorPopCov$covariates <- cohortMethodData$popCov %>%
    filter(.data$treatment == 0)

  attr(comparatorPopCov, "metaData")$populationSize <- nrow(comparatorPop)

  # aggregate both
  targetPopulationCovariateAgg <- FeatureExtraction::aggregateCovariates(targetPopCov)
  comparatorPopulationCovariateAgg <- FeatureExtraction::aggregateCovariates(comparatorPopCov)


  # compute balance
  balance <- FeatureExtraction::computeStandardizedDifference(targetPopulationCovariateAgg,
                                                              comparatorPopulationCovariateAgg)

  balance <- cohortMethodData$analysisRef %>%
    select("analysisId", "isBinary", "missingMeansZero") %>%
    inner_join(select(cohortMethodData$covariateRef, "analysisId", "covariateId")) %>%
    collect() %>%
    inner_join(balance)

  return(balance)
}



computeCovariateBalanceFeatureExtractionOther <- function(population, allCov, cohortMethodData) {


  # cohortMethodData$pop <- population
  cohortMethodData$popCov <- allCov %>%
    inner_join(select(population, .data$rowId, .data$treatment), by = "rowId")
  # cohortMethodData$pop <- NULL

  # get covariate data for target
  targetPop <- population %>%
    filter(.data$treatment == 1)
  targetPopCov <- Andromeda::andromeda(covariateRef = cohortMethodData$covariateRef,
                                       analysisRef = cohortMethodData$analysisRef)
  attr(targetPopCov, "metaData") <- attr(cohortMethodData, "metaData")
  class(targetPopCov) <- "CohortMethodData"

  # cohortMethodData$pop <- targetPop %>%
  #   select(.data$rowId)
  # targetPopCov$covariates <- cohortMethodData$pop %>%
  #   inner_join(cohortMethodData$covariates, by = "rowId")
  # cohortMethodData$pop <- NULL
  targetPopCov$covariates <- cohortMethodData$popCov %>%
    filter(.data$treatment == 1)


  attr(targetPopCov, "metaData")$populationSize <- nrow(targetPop)

  # get covariate data for popoulation treatment 1
  comparatorPop <- population %>%
    filter(.data$treatment == 0)
  comparatorPopCov <- Andromeda::andromeda(covariateRef = cohortMethodData$covariateRef,
                                           analysisRef = cohortMethodData$analysisRef)
  attr(comparatorPopCov, "metaData") <- attr(cohortMethodData, "metaData")
  class(comparatorPopCov) <- "CohortMethodData"

  # cohortMethodData$pop <- comparatorPop
  # comparatorPopCov$covariates <- cohortMethodData$covariates %>%
  #   inner_join(cohortMethodData$pop, by = "rowId")
  # cohortMethodData$pop <- NULL
  comparatorPopCov$covariates <- cohortMethodData$popCov %>%
    filter(.data$treatment == 0)

  attr(comparatorPopCov, "metaData")$populationSize <- nrow(comparatorPop)

  # aggregate both
  targetPopulationCovariateAgg <- FeatureExtraction::aggregateCovariates(targetPopCov)
  comparatorPopulationCovariateAgg <- FeatureExtraction::aggregateCovariates(comparatorPopCov)


  # compute balance
  balance <- FeatureExtraction::computeStandardizedDifference(targetPopulationCovariateAgg,
                                                              comparatorPopulationCovariateAgg)

  balance <- cohortMethodData$analysisRef %>%
    select("analysisId", "isBinary", "missingMeansZero") %>%
    inner_join(select(cohortMethodData$covariateRef, "analysisId", "covariateId")) %>%
    collect() %>%
    inner_join(balance)

  return(balance)
}
