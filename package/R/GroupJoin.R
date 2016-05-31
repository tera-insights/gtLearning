GroupJoin <- function(states, outputs, fragment.size = 2E6) {
  outputs <- substitute(outputs)
  check.atts(outputs)
  outputs <- convert.atts(outputs)

  gist <- GIST(learning::GroupJoin, fragment.size)
  Transition(gist, outputs, states)
}
