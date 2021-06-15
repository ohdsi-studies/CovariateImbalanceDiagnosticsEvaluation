

getCombinedResults <- function(balanceResults, eseResults)  {
  tmp <- balanceResults %>%
    left_join(unique(select(eseResults, c("databaseId", "analysisId", "targetId", "comparatorId", "ease", "easeCiLb", "easeCiUb"))))

  return(tmp)
}


getBalanceFeasibleStatistics <- function(results) {
  colsToOmit <- c("analysisId", "samplePerc", "psAdjustment", "balanceApproach",
                  "partitionId", "targetId", "comparatorId", "databaseId", "targetName",
                  "compartorName", "numCovContinuous", "numCovDichotomous")

  return(setdiff(colnames(results),
         colsToOmit))
}


getInputObject <- function() {
  return(
    list(
      targetName = "NewUsersOfAceInhibitors",
      comparatorName = "NewUsersOfBetaBlockers",
      databaseId = "OPTUM_DOD",
      psAdjustment = c("matchOnPs", "crude", "random"),
      balanceAggregateStatistic = "smdMaxDichotomous",
      partitionAggregation = "mean"
    )
  )
}
