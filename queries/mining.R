library(gtLearning)

facts <- GroupBy(Load(fb_facts), objectID, Hash(predicate, subjectID))

rules <- Gather(Load(fb_rules3))

groupjoin <- GroupJoin(list(rules, facts), c(pred, sub, obj, rule))

agg <- GroupBy(groupjoin, c(pred, sub, obj), correct = Sum(rule == 0), Gather(rule))

result <- GroupBy(rule, Sum(correct), total = Count())

as.data.frame(result)
