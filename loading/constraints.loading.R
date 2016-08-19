## This query loads the functional constraints data from a CSV.
library(gtBase)

## The location of the CSV file.
path <- "/data/probkb/functionals.csv"

## The data is read in from the file. The information is only integers, so the
## simple flag is used to increase efficiency.
data <- ReadCSV(path, "Constraints", simple = TRUE)

## The data is stored into the Constraints relation.
Store(data, Constraints, .overwrite = TRUE)
