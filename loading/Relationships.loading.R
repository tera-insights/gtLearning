## This query loads the relationship data from a CSV.
library(gtBase)

## The location of the CSV file.
path <- "/data/probkb/relationships.csv"

## The data is read in from the file. The information is only numeric IDs, so
## the simple flag is used to increase efficiency.
data <- ReadCSV(path, "Relationships", simple = TRUE)

## The data is stored into the Relationships relation.
Store(data, Relationships, .overwrite = TRUE)
