#' Grouping GLA
#'
#' \code{Group} is used to group data based on a natural ordering of its keys
#' and then output it in that order.
#'
#' This GLA takes in two types of expressions - keys and values. For each
#' distinct combination of keys, it stores all the values associated with those
#' keys in the order that they are seen. It then splits the keyset into chunks
#' and outputs their values separately. Furthermore, the output for the keys in
#' the same chunk is ordered by the natural ordering of the keys, with the first
#' key given the most precedence and so on. The ordering of the values for a
#' single set of keys is usually arbitrary due to the execution of the GLA and
#' therefore it should not be relied upon.
#'
#' The GLA can update the stored values for the same set of keys simultaneously,
#' meaning that the runtime is not significantly impacted by a keyset with few
#' unique values.
#'
#' @param data A \code{\link{waypoint}}.
#' @param keys A named list of expressions, whose names are used for the output
#'   column associated with that key. If no name is given and the corresponding
#'   key is simply an attribute of \code{data}, then the name of that attribute
#'   is also used for the output. Any key more complex than a single attribute
#'   must be given a name.
#'
#'   If no keys are given, a dummy expression always equal to 1 is used instead.
#'   In this case, the GLA simply accumulates all the data in a mostly arbitrary
#'   order.
#' @param values A named list of expressions that specify the values to be
#'   stored alongside the keys. The specification format follows the same rules
#'   as \code{keys}.
#' @param fragment.size The number of distinct combination of keys used in each
#'   chunk of output.
#' @param debug An integer code used to specify the level of debugging messages.
#' @param key.array A boolean specifying whether the keys are stored as an array
#'   or a tuple. This is only important when using this GLA as an input state.
#' @param val.array The same as \code{key.array} but for the storage of values.
#' @param delete.contents Whether the PostFinalize method is used to delete the
#'   stored information. This is only relevant when using this GLA as a state.
#'   If false, the stored data is kept for the entire query, which can be a
#'   wasteful use of memory.
#' @return A \code{\link{waypoint}} with the designated columns and rows.
#' @author Jon Claus, <jonterainsights@@gmail.com>, Tera Insights, LLC.
Group <- function(data, keys = c(group = 1), values, fragment.size = 2E6, debug = 0,
                  key.array = FALSE, val.array = FALSE, delete.contents = FALSE) {
  keys <- convert.exprs(substitute(keys))
  vals <- convert.exprs(substitute(values))

  split <- length(keys)
  if (!is.logical(key.array) && length(key.array) == 1)
    stop("Group: key.array should be a single boolean.")
  if (!is.logical(val.array) && length(val.array) == 1)
    stop("Group: val.array should be a single boolean.")
  gla <- GLA(learning::Group, split, fragment.size, debug, key.array, val.array, delete.contents)

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
