library(gtLearning)
library(gtStats)

## State 1: A mapping of object -> predidate -> subject. Steps 1 & 2.
facts <- Segmenter(Group(Load(fb_facts), c(objectID, predicate = predicate$GetID()), subjectID))

## State 2: A mapping of object -> rules relevant to that object.
## First, the rule pruning is done. For each rule, every relevant object is joined to it.
## The result is a table containing (rule, object) pairs, along with the number of subjects
## for body1 and body2 for that pair.
## A rule is filtered out if, for any object, both subject counts are greater than t.
counts1 <- Segmenter(GroupBy(Load(fb_facts), c(object1 = objectID, predicate), count1 = Count()))
counts2 <- Segmenter(GroupBy(Load(fb_facts), c(object2 = objectID, predicate), count2 = Count()))

rules <- ReadCSV("/pcluster/freebase/rules/rules6.csv", sep = " ", line.number = ID,
                 c(head  = base::Factor(dict = "fb_predicate", bytes = 4),
                   body1 = base::Factor(dict = "fb_predicate", bytes = 4),
                   body2 = base::Factor(dict = "fb_predicate", bytes = 4)))

data <- Join(counts1, predicate, rules, body1)
data <- Cache(Join(data, c(object1, body2), counts2, c(object2, predicate)))
## This performs the rule pruning, checking the NF for each rule.
t <- 100  ## The constraint parameter, as described in Definition 4.
data <- Cache(data[count1 <= .(t) || count2 <= .(t)])

## This creates an object -> rule mapping describing the relevant rules per object.
rules <- Segmenter(Group(data, object1, use.array = TRUE,
                         c(body1 = body1$GetID(), body2 = body2$GetID(), head = head$GetID(), ID)))

## Step 3 / Algorithm 4. The Group-Join GIST.
groupjoin <- GroupJoin(list(facts, rules), c(Subject, Predicate, Object, Rule), frag = 1E5)

## This is used to stored the intermediate result.
Store(groupjoin, groupjoin_intermediates, .overwrite = TRUE)

## Intermediate statistics:
## 2846162672 distinct facts (Subject, Object, Predicate triples).
## 2911518515 distinct rows (fact + Rule).
## 2991940894 total rows.

## Step 4. Remove duplicate rule IDs per fact.
data <- Load(groupjoin_intermediates)
group <- Segmenter(Group(data, Object, c(Predicate, Subject, Rule), frag = 1E5), inner = TRUE)
distinct <- StreamingGroupBy(group, c(group = 1), Distinct(c(Object, Subject, Predicate, Rule)))

Store(distinct, distinct_intermediates, .overwrite = TRUE)

## Step 5 / Algorithm 5. We don't need to deduplicate rule IDs for this next GroupBy because
## only non-zero rule IDs are repeated. The result of the Sum is still always 0 or 1.
data <- Load(distinct_intermediates)
group <- Segmenter(Group(data, Object, c(Predicate, Subject, Rule), frag = 1E5, delete = TRUE), inner = TRUE)
correct <- StreamingGroupBy(group, Object, GroupBy(c(Predicate, Subject), correct = Sum(Rule == 0)))

distinct <- Load(distinct_intermediates)
join <- Join(distinct, c(Object, Predicate, Subject),
             correct, c(Object, Predicate, Subject))
join <- Generate(join, Rule = distinct@Rule)
join <- join[Rule != 0]  ## Rule 0 just served as a placeholder.

## Step 6.
agg <- GroupBy(join, Rule, Sum(correct), total = Count())
rules <- ReadCSV("/pcluster/freebase/rules/rules6.csv", sep = " ", line.number = ID,
                 c(p = base::Factor(dict = "fb_predicate", bytes = 4),
                   q = base::Factor(dict = "fb_predicate", bytes = 4),
                   r = base::Factor(dict = "fb_predicate", bytes = 4)))
agg <- Join(agg, Rule, rules, ID)

## Step 7.
result <- as.data.frame(agg, Rule, p, q, r, support = correct, confidence = correct / total)
