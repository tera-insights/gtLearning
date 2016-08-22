## The overall algorithm for grounding is as follows:
## 1. Apply each Rule 1 - 6 in that order on the current set of facts, placing
##    each set of inferred facts in inferred_{rule}.
## 2. Append each set of inferred facts to the current set of facts.
## 3. Filter out duplicate facts.
## 4. Check for functional constraints.
## 5. Repeat the above 3 more times, loading from the latest set of facts.
## 6. Perform grounding.
library(gtLearning)

## Due to the complexity of this query, functionality is compartmentalized into
## several sub-routines, which are as follows:

## A. Attach class information.
## This takes in a table of facts (Predicate, Subject, and Object) and a mapping
## of ID -> Class. All column names must be exact. The class information for
## both Subject and Object is then attached and labelled as SubjectClass and
## ObjectClass. Entity, Subject, and Object must all have the same type. The two
## attached columns have the same type as ClassID. The class mapping is simply
## specified as a character naming the relation, as the data is pre-loaded.
AttachClass <- function(facts, classes = "Entities") {
  ## The relation containing the class is loaded twice.
  map.sub <- Load(classes)
  map.obj <- Load(classes)

  ## The information is attached via joins.
  data <- Join(facts, Object, map.obj, ID)
  data <- Join(data, Subject, map.sub, ID)

  ## The columns are renamed and the result is returned.
  Generate(data, SubjectClass = map.sub@Class, ObjectClass = map.obj@Class)
}

## B. Merge inferred facts.
## This takes in a character naming a relation and a list of tables of facts.
## Each table is concatenated to the given relation. Duplicated facts are then
## removed and the result is returned. Every table involved should have columns
## labelled Predicate, Subject, and Object.
Merge <- function(relation, tables) {
  ## Each table is concatenated to the relation.
  for (table in tables)
    Store(table, relation)

  ## The merge data is loaded and grouped on the Predicate.
  data <- Segmenter(Group(Load(relation), Predicate, c(Object, Subject)))

  ## The duplicated (Object, Subject) pairs are filtered out for each predicate.
  StreamingGroupBy(data, Predicate, Distinct(c(Object, Subject)))
}

## C. Perform inference.
## This performs inference given the names of relations containing the facts and
## rules, as well as the rule type to perform. A waypoint is returned containing
## the inferred facts, with column names Predicate, Subject, and Object.

## See queries for individual rules for a more detailed description.
Infer <- function(facts, rules, type) {
  ## The facts and rules are loaded.
  facts <- Load(facts)
  rules <- Load(rules)

  ## Class information is attached to the facts.
  facts <- AttachClass(facts, "Entities")

  if (type < 3) {
    ## Rules of type 1 and 2 are much simpler to perform, requiring only a join.
    join <- Join(facts, c(Predicate, SubjectClass, ObjectClass),
                 rules, c(Body1,     Class1,       Class2))

    if (type == 2)
      Generate(join, Subject = Object, Object = Subject, .overwrite = TRUE)
    else
      join
  } else {
    ## The necessary groupings are performed.
    if (type == 3)
      facts <- Multiplexer(facts,
                           Segmenter(inner.GLA = Group(c(s = Subject, p = Predicate$GetID()), Object)))
    else if (type == 3 || type == 4)
      facts <- Multiplexer(facts,
                           Segmenter(inner.GLA = Group(c(s = Subject, p = Predicate$GetID()), Object)),
                           Segmenter(inner.GLA = Group(c(O = Object, P = Predicate$GetID()), Subject)))
    else
      facts <- Multiplexer(facts,
                           Segmenter(inner.GLA = Group(c(O = Object, P = Predicate$GetID()), Subject)))

    ## The rule -> argument mapping is created.
    facts1 <- AttachClass(eval(substitute(Load(rel), list(rel = as.symbol(facts)))), "Entities")
    if (type == 3 || type == 5)
      counts1 <- Segmenter(GroupBy(facts1,
                                   c(Arg = Subject, Class = SubjectClass, Predicate),
                                   Count1 = Count()))
    else
      counts1 <- Segmenter(GroupBy(facts1,
                                   c(Arg = Object, Class = ObjectClass, Predicate),
                                   Count1 = Count()))

    facts2 <- AttachClass(eval(substitute(Load(rel), list(rel = as.symbol(facts)))), "Entities")
    if (type == 3 || type == 4)
      counts2 <- Segmenter(GroupBy(facts1,
                                   c(Arg = Subject, Class = SubjectClass, Predicate),
                                   Count2 = Count()))
    else
      counts2 <- Segmenter(GroupBy(facts2,
                                   c(Arg = Object, Class = ObjectClass, Predicate),
                                   Count2 = Count()))

    ## This join checks the class of the inner argument of the rule.
    data <- Join(counts1, c(Predicate, Class), rules, c(Body1, Class2))
    data <- Join(data, c(Arg, Class, Body2), counts2, c(Arg, Class, Predicate))
    t <- 100  ## The constraint parameter, as described in Definition 4.
    agg <- GroupBy(data, c(ID, Head, Body1, Body2), fragment.size = 200, Gather(Arg),
                   num_failed = Sum(count1 > .(t) && count2 > .(t)), count = Sum(count1 * count2))
    data <- Cache(agg[num_failed == 0])

    ## This creates a rule -> argument mapping describing the relevant arguments per rule.
    rules <- Segmenter(Group(data, c(ID, Head = Head$GetID(), Body1 = Body1$GetID(), Body2 = Body2$GetID()),
                             Arg, key.array = TRUE))

    data <- GroupJoin(list(facts, rules), c(Subject, Predicate, Object, Rule), rule = rule, frag = 1E4)
    data <- AttachClass(data, "Entities")

    ## Checking the class information of the outer arguments in each rule.
    rules <- eval(substitute(Load(rel), list(rel = as.symbol(rules))))
    Join(data, c(Rule, SubjectClass, ObjectClass), rules, c(Rule, Class1, Class3))
  }
}

## D. Perform functional constraint checking.
## This sub-routine takes in names for three relations. The first two should
## contain facts and functional constrains. The facts that pass the checks are
## then placed into the third relation. The facts relations should contain the
## same columns as the previous sub-routines and the contrains relation should
## have columns named Predicate, Code, and Maximum.
CheckFunctionalContraints <- function(facts, constraints, result) {
  ## Loading the two specified relations
  facts <- Load(facts)
  constraints <- Load(constraints)

  ## The predicates for the facts are matched to their constraint.
  outer.join <- Join(facts, Predicate, contrains, Predicate, left.outer = TRUE)

  ## The facts for which there are no constraints are simply copied.
  Store(outer.join[IsNull(Code)], result, .overwrite = TRUE)

  ## A column is created that holds either the object or subject for a fact,
  ## whichever is relevant to the contrainst on that fact.
  ## TODO: This will not work for predicates that have contraints on both the
  ##       object and subject.
  facts <- Generate(outer.join[!IsNull(Code)],
                    Data = if (Code == 1) Subject else Object)

  ## For each predicate, the corresponding facts are grouped by the value of the
  ## argument with a contrainst on it.
  data <- GroupBy(facts, c(Predicate, Data, Maximum),
                  NumValues = Count(), Gather(Subject, Object))

  ## Facts are removed if the same argument appeared more than the legal maximum
  ## number of times for a given predicate. This is then appended to the result.
  Store(data[NumValues <= Maximum], result)
}

## The main routine is as follows:

## The relation for intermediate results is created.
if (!"inferred_facts" %in% get.relations())
  Create(inferred_facts,
         Predicate = Int,
         Subject   = Int,
         Object    = Int)

## Perform the inference iteratively.
for (iteration in 0:3) {
  ## The input and output for the current iteration.
  facts_rel <- if (iteration == 0) "Relationships" else paste0("facts", iteration)
  result_rel <- paste0("facts", iteration + 1)

  ## Creating the relation for the results of the current iteration.
  if (result_rel %in% get.relations())
    Create(result_rel,
           Predicate = Int,
           Subject   = Int,
           Object    = Int)

  ## The original table of facts is copied into the intermediate results.
  Store(Load(facts_rel), "inferred_facts", .overwrite = TRUE)

  ## Each type of rule is applied and the inferred facts are added.
  tables <- mapply(Infer, facts_rel, paste0("Rules_", 1:6), 1:6)
  Store(Merge("inferred_facts", tables), "facts")

  ## The constraint checking is performed.
  CheckFunctionalConstraints("inferred_facts", "Constraints", result_rel)
}

## TODO: Perform the grounding.
