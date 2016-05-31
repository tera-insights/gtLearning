library(gtBase)

Create(groupjoin_intermediates,
       Predicate = base::Factor(dict = "fb_predicate", bytes = 4),
       Subject   = base::Int,
       Object    = base::Int,
       Rule      = base::Int)

Create(distinct_intermediates,
       Predicate = base::Factor(dict = "fb_predicate", bytes = 4),
       Subject   = base::Int,
       Object    = base::Int,
       Rule      = base::Int)
