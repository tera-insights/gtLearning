library(gtLearning)
library(gtSampling)

## State 1: A mapping of object -> predidate -> subject. Steps 1 & 2.
## facts <- Group(Load(fb_facts)[objectID == 313700165], c(object, pred), subject)
facts <- GroupBy(Gather(Load(fb_facts)),
                 objectID, GroupBy(predicate, Gather(subjectID, init.size = 1000)))

## State 2: A mapping of object -> rules relevant to that object.
counts1 <- Segmenter(GroupBy(Load(fb_facts), c(object1 = objectID, predicate), count1 = Count()))
counts2 <- Segmenter(GroupBy(Load(fb_facts), c(object2 = objectID, predicate), count2 = Count()))

rules <- ReadCSV("/pcluster/freebase/rules/rules6.csv", sep = " ",
                 c(head  = base::Factor(dict = "fb_predicate", bytes = 4),
                   body1 = base::Factor(dict = "fb_predicate", bytes = 4),
                   body2 = base::Factor(dict = "fb_predicate", bytes = 4)))

data <- Join(counts1, predicate, rules, body1)
data <- Join(data, c(object1, body2), counts2, c(object2, predicate))
data <- data[count1 <= 100 || count2 <= 100]
rules <- GroupBy(data, object1, Gather(c(body1, body2, head), use.array = TRUE))

## Step 3 / Algorithm 4. The Group-Join GIST.
groupjoin <- GroupJoin(list(facts, rules), c(Predicate, Subject, Object, Rule))

## This is used to stored the intermediate result.
Store(groupjoin, groupjoin_intermediates .overwrite = TRUE)

## Intermediate statistics:
## 2782226755 distinct facts (Subject, Object, Predicate triples).
## 2901904102 distinct rows (fact + Rule).
## 2991940894 total rows.

## Step 4. Remove duplicate rule IDs per fact.
agg <- Segmenter(Distinct(Load(groupjoin_intermediates)), segment = Object, passes = 3)

## Step 5 / Algorithm 5. We don't need to deduplicate rule IDs for this next GroupBy because
## only non-zero rule IDs are repeated. The result of the Sum is still always 0 or 1.
correct <- Segmenter(GroupBy(Load(groupjoin_intermediates),
                             c(Object, Predicate, Subject), correct = Sum(Rule == 0)))
agg <- Join(agg, c(Object, Predicate, Subject), correct, c(Object, Predicate, Subject))
agg <- agg[Rule != 0]  ## Rule 0 just served as a placeholder.

## Step 6.
agg <- GroupBy(agg, Rule, Sum(correct), total = Count())

result <- as.data.frame(agg)
