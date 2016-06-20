GroupJoin <- function(states, outputs, rule.type, fragment.size = 2E6) {
  outputs <- substitute(outputs)
  check.atts(outputs)
  outputs <- convert.atts(outputs)

  gist <- GIST(learning::GroupJoin, fragment.size, rule.type)
  Transition(gist, outputs, states)
}
