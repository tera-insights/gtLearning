GroupJoin <- function(states, outputs) {
  outputs <- substitute(outputs)
  check.atts(outputs)
  outputs <- convert.atts(outputs)

  gist <- GIST(learning::GroupJoin)
  Transition(gist, outputs, states)
}
