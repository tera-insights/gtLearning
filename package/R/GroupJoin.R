GroupJoin <- function(states, outputs) {
  outputs <- substitute(outputs)
  check.atts(outputs)
  outputs <- convert.atts(outputs)

  gist <- GIST(statistics)
  Transition(gist, outputs, states)
}
