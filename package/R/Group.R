## If no actual keys are given, a dummy key is 1 is used instead. The algorithm
## could be specifically optimizied for the absence of keys, but the benefit is
## most likely not worth the effort.
Group <- function(data, keys = c(group = 1), values, use.array = FALSE,
                  fragment.size = 2E6, debug = 0, delete.contents = FALSE) {
  keys <- convert.exprs(substitute(keys))
  vals <- convert.exprs(substitute(values))

  split <- length(keys)
  if (!is.logical(use.array) && length(use.array) == 1)
    stop("Group: use.array should be a single boolean.")
  gla <- GLA(learning::Group, split, use.array, fragment.size, debug, delete.contents)

  key.names <- convert.names(keys)
  missing <- which(key.names == "")
  exprs <- grokit$expressions[keys[missing]]
  if (all(good <- is.symbols(exprs)))
    key.names[missing] <- as.character(exprs)
  else
    stop("no name given for complex inputs:",
         paste("\n\t", lapply(exprs[!good], deparse), collapse = ""))

  val.names <- convert.names(vals)
  missing <- which(val.names == "")
  exprs <- grokit$expressions[vals[missing]]
  if (all(good <- is.symbols(exprs)))
    val.names[missing] <- as.character(exprs)
  else
    stop("no name given for complex inputs:",
         paste("\n\t", lapply(exprs[!good], deparse), collapse = ""))

  Aggregate(data, gla, c(keys, vals), c(key.names, val.names))
}
