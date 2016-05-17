Group <- function(data, keys, values) {
  keys <- convert.exprs(substitute(keys))
  vals <- convert.exprs(substitute(values))

  split <- length(keys)
  gla <- GLA(learning::Group, split)

  key.names <- convert.names(keys)
  missing <- which(key.names == "")
  exprs <- grokit$expressions[keys[missing]]
  if (all(is.symbols(exprs)))
    key.names[missing] <- as.character(exprs)
  else
    stop("no name given for complex inputs:",
         paste("\n\t", lapply(keys[missing], deparse), collapse = ""))

  val.names <- convert.names(vals)
  missing <- which(val.names == "")
  exprs <- grokit$expressions[vals[missing]]
  if (all(is.symbols(exprs)))
    val.names[missing] <- as.character(exprs)
  else
    stop("no name given for complex inputs:",
         paste("\n\t", lapply(vals[missing], deparse), collapse = ""))

  Aggregate(data, gla, c(keys, vals), c(key.names, val.names))
}
