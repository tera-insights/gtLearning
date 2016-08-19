## This query loads the rule data from a CSV for each type of rule.
library(gtBase)

## The location and prefix of the files containing the rules
prefix <- "/data/probkb/mln"

## Each rule type is processed separately.
for (i in 1:6) {
  ## The file for each rule should be {prefix}{type number}.csv
  path <- paste0(prefix, i, ".csv")

  ## The relation where the rules of this type are stored.
  relation <- paste0("Rules_", i)

  ## The data is read in from the file. The information is only numeric IDs, so
  ## the simple flag is used to increase efficiency.
  data <- ReadCSV(path, relation, simple = TRUE)

  ## The data is stored into the appropriate relation.
  eval(substitute(Store(data, relation, .overwrite = TRUE),
                  list(relation = as.symbol(relation))))
}
