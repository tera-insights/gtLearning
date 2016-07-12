library(gtLearning)
library(gtStats)

## The time at which this query starts, used for benchmarking.
begin <- Sys.time()

## State 1: A mapping of object -> predicate -> subject and vice-versa. Steps 1 & 2.
facts <- Multiplexer(Load(fb_facts),
                     Segmenter(inner.GLA = Group(c(s = subjectID, p = predicate$GetID()), objectID)),
                     Segmenter(inner.GLA = Group(c(O = objectID, P = predicate$GetID()), subjectID)))

## State 2: A mapping of object -> rules relevant to that object.
## First, the rule pruning is done. For each rule, every relevant object is joined to it.
## The result is a table containing (rule, object) pairs, along with the number of facts
## for body1 and body2 for that pair.
## A rule is filtered out if, for any join argument, both predicate counts are greater than t.
counts1 <- Segmenter(GroupBy(Load(fb_facts), c(arg1 = objectID, predicate), count1 = Count()))
counts2 <- Segmenter(GroupBy(Load(fb_facts), c(arg2 = subjectID, predicate), count2 = Count()))

rules <- ReadCSV("/pcluster/freebase/rules/rules4.csv", sep = " ", line.number = ID,
                 c(head  = base::Factor(dict = "fb_predicate", bytes = 4),
                   body1 = base::Factor(dict = "fb_predicate", bytes = 4),
                   body2 = base::Factor(dict = "fb_predicate", bytes = 4)))

data <- Join(counts1, predicate, rules, body1)
data <- Cache(Join(data, c(arg1, body2), counts2, c(arg2, predicate)))
## This performs the rule pruning, checking the NF for each rule.
t <- 100  ## The constraint parameter, as described in Definition 4.
agg <- GroupBy(data, c(ID, head, body1, body2), fragment.size = 200, Gather(arg1),
               num_failed = Sum(count1 > .(t) && count2 > .(t)), count = Sum(count1 * count2))
data <- Cache(agg[num_failed == 0])

## This creates a rule -> argument mapping describing the relevant arguments per rule.
rules <- Segmenter(Group(data, c(ID, head = head$GetID(), body1 = body1$GetID(), body2 = body2$GetID()),
                         arg1, key.array = TRUE))

## Step 3 / Algorithm 4. The Group-Join GIST.
groupjoin <- GroupJoin(list(facts, rules), c(Subject, Predicate, Object, Rule), rule = 4, frag = 1E4)

## This is used to stored the intermediate result.
Store(groupjoin, groupjoin_intermediates, .overwrite = TRUE)

## The facts are appended to the intermediates results, which represents line 2 of Algorithm 4.
data <- Generate(Load(fb_facts), Rule = 0)
Store(data, groupjoin_intermediates, Object = objectID, Subject = subjectID, Predicate = predicate)

## Step 4. Remove duplicate rule IDs per fact.
data <- Load(groupjoin_intermediates)
group <- Segmenter(Group(data, Object, c(Predicate, Subject, Rule), frag = 1E5), fragment = TRUE)
distinct <- StreamingGroupBy(group, c(group = 1), Distinct(c(Object, Subject, Predicate, Rule)))

Store(distinct, distinct_intermediates, .overwrite = TRUE)

## Step 5 / Algorithm 5. We don't need to deduplicate rule IDs for this next GroupBy because
## only non-zero rule IDs are repeated. The result of the Sum is still always 0 or 1.
data <- Load(distinct_intermediates)
group <- Segmenter(Group(data, Object, c(Predicate, Subject, Rule), frag = 1E5, delete = TRUE), fragment = TRUE)
correct <- StreamingGroupBy(group, Object, GroupBy(c(Predicate, Subject), correct = Sum(Rule == 0)))

distinct <- Load(distinct_intermediates)
join <- Join(distinct, c(Object, Predicate, Subject),
             correct, c(Object, Predicate, Subject))
join <- Generate(join, Rule = distinct@Rule)
join <- join[Rule != 0]  ## Rule 0 just served as a placeholder.

## Step 6.
agg <- GroupBy(join, Rule, Sum(correct), total = Count())

## Step 7.
agg <- OrderBy(agg, confidence = dsc(correct / total), support = dsc(correct))[support > 0]
rules <- ReadCSV("/pcluster/freebase/rules/rules4.csv", sep = " ", line.number = ID,
                 c(p = base::Factor(dict = "fb_predicate", bytes = 4),
                   q = base::Factor(dict = "fb_predicate", bytes = 4),
                   r = base::Factor(dict = "fb_predicate", bytes = 4)))
agg <- Join(agg, Rule, rules, ID)
result <- as.data.frame(agg, Rule, p, q, r, support, confidence)
time <- Sys.time() - begin
