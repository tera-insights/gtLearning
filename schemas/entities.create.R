## This table stores the ID, name, and class of each entity, which refers to an
## argument used in a relationship. In the given data, there exists entities
## with an ID and name that are missing class information. These are removed.
library(gtBase)

Create(Entities,
       ID    = Int,
       Name  = Int,
       Class = Int)
