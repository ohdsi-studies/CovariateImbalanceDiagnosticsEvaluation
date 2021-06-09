library(magrittr)

# populationSampleSizes <- c(1, 2, 5, 10, 20, 30, 40, 60, 70, 80, 90, 100)
populationSamplePercs <- c(1, 2, 5, 10, 20)
psAdjustments <- c("matchOnPs", "crude", "random")
balanceApproach <- "partition" #TODO: change as necessary


analyses <- expand.grid(populationSamplePercs, psAdjustments)
colnames(analyses) <- c("sample_perc", "ps_adjustment")

analyses[, "analysis_id"] <- 1:nrow(analyses)
analyses <- analyses[, c(3, 1, 2)]

analyses[, "balance_approach"] <- balanceApproach
readr::write_csv(analyses, file = file.path("inst", "settings", "analysisList.csv"))
