library(gtBase)

Create(fb_schema,
       predicate = base::Factor(bytes = 4, dict = "fb_predicate"),
       domain    = base::Factor(bytes = 4, dict = "fb_domain"),
       range     = base::Factor(bytes = 4, dict = "fb_domain"))
