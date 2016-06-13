library(gtLearning)
library(gtStats)

## The two inputs.
facts <- Load(fb_facts)
rules <- ReadCSV("/pcluster/freebase/rules/rules2.csv", sep = " ", line.number = Rule,
                 c(head = base::Factor(dict = "fb_predicate", bytes = 4),
                   body = base::Factor(dict = "fb_predicate", bytes = 4)))

## Step 1, 2, and 3 are just a simple join, as the rule is simple.
## The subject and object are transposed in the Store.
join <- Join(facts, predicate, rules, body)
Store(join, groupjoin_intermediates, Object = subjectID, Subject = objectID, Predicate = head, .overwrite = TRUE)

## The facts are appended to the intermediates results, which represents line 2 of Algorithm 4.
data <- Generate(Load(fb_facts), Rule = 0)
Store(data, groupjoin_intermediates, Object = objectID, Subject = subjectID, Predicate = predicate)

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
agg <- GroupBy(join, Rule, Sum(correct), total = Count())[correct != 0]

## Step 7.
agg <- OrderBy(agg, confidence = dsc(correct / total), support = dsc(correct))
rules <- ReadCSV("/pcluster/freebase/rules/rules2.csv", sep = " ", line.number = Rule,
                 c(p = base::Factor(dict = "fb_predicate", bytes = 4),
                   q = base::Factor(dict = "fb_predicate", bytes = 4)))
agg <- Join(agg, Rule, rules, Rule)
result <- as.data.frame(agg, p, q, support, confidence)
