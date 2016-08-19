## This table stores functional constraints, including the relevant predicate, a
## code denoting whether the constraing applies to the subject or object, and
## the maximum number of distinct arguments of that type for that predicate. A
## code of 1 is used for subject; 2, for object.

## For example, the tuple (IsCapitalOf, 2, 1) means that a given object can only
## appear in a single IsCapitalOf relationship. This because each country only
## has one capital. Note that IsCapitalOf would normally be represented by an
## integer ID.
library(gtBase)

Create(Constraints,
       Predicate = Int,
       Code      = Int,
       Maximum   = Int)
