library(gtBase)

Create(fb_facts,
       subjectID = base::Int,
       predicate = base::Factor(dict = "fb_predicate", bytes = 4),
       objectID  = base::Int)
