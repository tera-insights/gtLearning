#' GroupJoin GIST
#'
#' \code{GroupJoin} is used to perform the GroupJoin operation, using a GIST
#' waypoint.
#'
#' This GIST performs the GroupJoin operation using two states. The first state
#' should be a Multiplexer placed on top of one or two Segmenter GLAs, each
#' with the inner GLA being Group. Each of these Groups should represent a
#' mapping of subject -> predicate -> object (SPO) or vice versa (OPS). In the
#' case that both types of mapping are present, SPO should come first. In each
#' case, the expression which the Segmenter acts upon should be the one used for
#' the grouping, e.g. subject for SPO and object for OPS. These states represent
#' the knowledge base, which preprocessing done in the form of grouping.
#'
#' The second and final state is also a Segmenter on top of a Group GLA. This
#' should represent a mapping of (r.ID, r.head, r.body1, r.body2) -> arguments,
#' where each argument for a rule is a value z such that that rule infers at
#' least one new fact when z is the shared variable of the two body predicates.
#'
#' For example, the type 4 rule (ID, p, q, r) should be mapped to (a, b) if the
#' knowledge base contains: \code{q(A, a) r(a, B) q(C, b) r(b, D)}. This is
#' because the rule can infer \code{r(A, B)} and \code{r(C, D)}, when the common
#' variable of \code{q} and \code{r} is \code{a} and \code{b}, respectively.
#'
#' Please see the examples for a demonstration of usage.
#'
#' @param states A list of two \code{\link{waypoint}}s, the facts and rules
#'   states. See \code{Details} for more information.
#' @param outputs An attribute list specifying the names of the columns for the
#'   subject, predicate, object, and rule ID of the output, in that order.
#' @param rule.type Which type of rule is being used for inference.
#' @param fragment.size The output is separated into chunks, with this many
#'   rules per chunk.
#' @return A \code{\link{waypoint}} with the designated columns and rows.
#' @author Jon Claus, <jonterainsights@@gmail.com>, Tera Insights, LLC.
#' @references \url{http://www.cise.ufl.edu/~yang/doc/sigmod16.pdf}
GroupJoin <- function(states, outputs, rule.type, fragment.size = 2E6) {
  outputs <- substitute(outputs)
  check.atts(outputs)
  outputs <- convert.atts(outputs)

  gist <- GIST(learning::GroupJoin, fragment.size, rule.type)
  Transition(gist, outputs, states)
}
