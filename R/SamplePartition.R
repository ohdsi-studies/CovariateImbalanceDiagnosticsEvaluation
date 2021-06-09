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

partitionPopulation <- function(cmOutputFolder,
                             sampleFolder = NULL,
                             maxCores,
                             dedicatedDirs = FALSE,
                             allPsFiles = FALSE,
                             samplePercentage = 1,
                             seed = 123) {
  set.seed(seed)


  if(is.null(sampleFolder)) {
    sampleFolder <- cmOutputFolder
  }
  if(!file.exists(sampleFolder)) {
    dir.create(sampleFolder, recursive = TRUE, showWarnings = FALSE)
  }


  psFiles <- getPsFiles(cmOutputFolder, sharedOnly = !allPsFiles)

  createSamplingTask <- function(i) {
    args <- list(psFile = psFiles[[i]],
                 dedicatedDirs = dedicatedDirs,
                 cmOutputFolder = cmOutputFolder,
                 sampleFolder = sampleFolder,
                 samplePercentage = samplePercentage)
    return(args)
  }

  samplingTasks <- lapply(1:length(psFiles), createSamplingTask)

  if (length(samplingTasks) != 0) {
    cluster <- ParallelLogger::makeCluster(min(10, maxCores))
    ParallelLogger::clusterRequire(cluster, packageName)
    dummy <- ParallelLogger::clusterApply(cluster, samplingTasks, createPartition)
    ParallelLogger::stopCluster(cluster)
  }

}

createPartition <- function(args) {
  psSampleFolder <- args$sampleFolder
  ps <- readRDS(file.path(args$cmOutputFolder, args$psFile))
  if("partition" %in% colnames(ps))
    return(NULL)
  numberOfSamples <- floor(nrow(ps) / ((args$samplePercentage / 100) * nrow(ps)))
  if(args$dedicatedDirs) {
    psSampleFolder <- sub(pattern = "(.*)\\..*$", replacement = "\\1", basename(args$psFile))
    psSampleFolder <- file.path(psSampleFolder, psSampleFolder)
    if(!file.exists(psSampleFolder)) {
      dir.create(psSampleFolder, showWarnings = FALSE, recursive = TRUE)
    }
  }
  randUnif <- runif(nrow(ps))
  breaks <- quantile(randUnif, (1:(numberOfSamples - 1))/numberOfSamples)
  breaks <- unique(c(0, breaks, 1))
  partitionIds <- as.integer(as.character(cut(randUnif,
                                              breaks = breaks,
                                              labels = 1:(length(breaks) - 1))))
  ps[, "partition"] <- partitionIds
  saveRDS(ps, file = file.path(psSampleFolder, args$psFile))

  return(NULL)
}



partitionPropensityPop <- function(ps, numPartitions, randomSeed) {
  if(numPartitions %% 1 != 0)
    stop("numPartitions must be an integer")
  set.seed(randomSeed)
  popSize <- nrow(ps)
  partitionIds <- sample(1:numPartitions, size = popSize, replace = TRUE)
  ps[, "partition"] <- partitionIds
  set.seed(NULL)
  return(ps)
}
