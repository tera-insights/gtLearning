## This query loads the entity information. It does this by reading in the name
## and class information, then joinining on ID.
library(gtBase)

## The location of the two CSV files.
names <- "/data/probkb/entities.csv"
class <- "/data/probkb/entClasses.csv"

## The two files are read. The names do not containg quotes, so the simple flag
## is used to increase efficiency.
names <- ReadCSV(names, c(ID = Int, Name = String), simple = TRUE)
class <- ReadCSV(class, c(ID = Int, Class = Int), simple = TRUE)

## The matching is performed.
data <- Join(names, ID, class, ID)

## The data is stored into the Functionals relation.
Store(data, Entities, .overwrite = TRUE)
