<?
// This GIST is used to perform the GroupJoin algorithm discussed in the paper,
// Ontological Pathfinder: Mining First Order Knowledge from Large Knowledge
// Bases. It takes in two states containing the knowledge base and rules. The
// first should be a two-layer mapping of object -> predicate -> subject. Each
// distinct object should be mapped to a sub-map mapping each predicate to the
// various subjects for that object-predicate pairing. The second state simply
// needs to be an iterable collection of the rules, where each rule is a tuple
// of predicates whose first element is the implied predicate.
function GroupJoin($t_args, $outputs, $states)
{
    // Class name is randomly generated.
    $className = generate_name('GroupJoin');

    // Processing of input state information.
    $numStates = count($states);
    grokit_assert($numStates == 2,
                  "GroupJoin: Expected 2 states. Received $numStates.");
    $states_ = array_combine(['facts_state', 'rules_state'], $states);

    $factsInputs = $states_['facts_state']->input();
    // If the facts state is a Segmenter on top of a GroupBy, the first input
    // used for segmenter is removed.
    if ($states_['facts_state']->name() == 'BASE::SEGMENTER')
        $factsInputs = array_slice($factsInputs, 1);

    // Initialization of local variables from template arguments.

    // Setting output types.
    // The inputs to the facts state should be in the order of obj, pred, subj.
    $outputs_ = array_combine(['subj', 'pred', 'obj'], $factsInputs);
    $outputs_['rule'] = lookupType('base::int');
    $outputs = array_combine(array_keys($outputs), $outputs_);

    $sys_headers  = ['math.h', 'unordered_map'];
    $user_headers = [];
    $lib_headers  = ['base\gist.h'];
    $libraries    = [];
    $extra        = [];
    $result_type  = ['fragment'];
?>

// The containers for the facts and rules.
using Facts = <?=$states_['facts_state']?>;
using Rules = <?=$states_['rules_state']?>::Vector;

// The map used for the predicate -> subject mapping per object.
using Map = Facts::InnerGLA::MapType;

// The types used for objects and predicates.
using Object = <?=$outputs_['obj']?>;
using Predicate = <?=$outputs_['pred']?>;

// The number of predicates in each rule.
static const constexpr size_t kRuleLength = std::tuple_size<Rules::value_type>::value;

// The constraint paramter, as described in Definition 4.
static const constexpr size_t kConstraint = 100;

// The rules information is changed from a vector of predicate tuples to a
// simplified C array. This allows for easier reference.
using RulesMatrix = std::array<Predicate, kRuleLength>*;

// The iterator for the fragment result type. This is an abstract function, as
// their are two different types of fragments each with their own sub-class.
class <?=$className?>_Iterator {
 protected:
  Object object;            // The associated object.
  const Facts& facts;       // The mapping for the facts.
  const RulesMatrix rules;  // The container of rules.
  size_t num_rules;         // The number of rules.

 public:
  <?=$className?>_Iterator(const Object& object, int num_rules,
                           const Facts& facts, const RulesMatrix rules)
      : object(object),
        num_rules(num_rules),
        facts(facts),
        rules(rules) {
  }

  virtual ~<?=$className?>_Iterator() {}

  virtual bool GetNextResult(<?=typed_ref_args($outputs_)?>) = 0;
};

// This iterator is used for the first loop structure in Algorithm 4. The input
// facts are simply outputted along with a rule ID of 0.
class <?=$className?>_Iterator_Copy : public <?=$className?>_Iterator {
 private:
  // The iterator across the inner mapping for predicates -> subject.
  Map::const_iterator predicate_iterator;

  // Iterators for the current and last subjects for the current predicate.
  Map::mapped_type::Vector::const_iterator value, upper;

 public:
  // This sets up the iterators for traversing the predicates and subjects.
  <?=$className?>_Iterator_Copy(const Object& object, int num_rules,
                                const Facts& facts, const RulesMatrix rules)
      : <?=$className?>_Iterator(object, num_rules, facts, rules),
        predicate_iterator(facts.Get(object).GetMap().begin()),
        value(predicate_iterator->second.GetList().begin()),
        upper(predicate_iterator->second.GetList().end()) {
  }

  bool GetNextResult(<?=typed_ref_args($outputs_)?>) {
    // The predicate is advanced if output has finished for the current one.
    if (value == upper) {
      predicate_iterator++;
      // Output is halted once all predicates have been traversed.
      if (predicate_iterator == facts.Get(object).GetMap().end()) {
        return false;
      } else {
        value = predicate_iterator->second.GetList().begin();
        upper = predicate_iterator->second.GetList().end();
      }
    }
    pred = predicate_iterator->first.GetKey0();
    subj = std::get<0>(*value);
    obj = object;
    rule = 0;
    ++value;
    return true;
  }
};

// This fragment is used to perform the second loop structure in Algorith 4 and
// the output corresponds to line 7. The body predicates for each rule are
// iterated over recursively, meaning an iterator must be stored for each body
// predicate. The upper and lower bounds must also be stored for each, except
// that a lower bound is not needed for the outermost predicate because it will
// never need to be rolled over. Regardless, this extra lower bound is kept for
// sake of simplicity.
// TODO: This is currently hard-coded for the single case of length-3 rules. As
//   such, it appears overly complicated. It is planned to allow arbitrary long
//   rules in the future and so it should not be simplified.
class <?=$className?>_Iterator_Loop : public <?=$className?>_Iterator {
 private:
  // The index for the rule currently being used for output.
  size_t rule_index;

  // Each predicate has iterators for the current, first, and last subjects.
  std::array<Map::mapped_type::Vector::const_iterator, 2> value, lower, upper;

 public:
  // rule_index is initialized as -1 because AdvanceRule is immediately called.
  // This is done instead of calling InitializeRule directly to implicitly check
  // that at least one rule was provided in the input state to the GIST.
  <?=$className?>_Iterator_Loop(const Object& object, int num_rules,
                                const Facts& facts, const RulesMatrix rules)
      : <?=$className?>_Iterator(object, num_rules, facts, rules),
        rule_index(-1) {
    AdvanceRule();
  }
  // This initializes each iterator for the current rule.
  void InitializeRule() {
    // This is used to check the constraints in Definition 4 for this rule.
    size_t minimum_size = kConstraint + 1;
    int i;
    for (i = 0; i < 2; i++) {
      // There are no subjects for this object-predicate pair. This means that
      // the rule containing this predicate will have no output, so there is no
      // need to set up the iterators.
      if (!facts.Get(object).Contains(rules[rule_index][i]))
        break;
      // List is a vector of the subjects for the current predicate.
      auto list = facts.Get(object).Get(rules[rule_index][i]).GetList();
      lower[i] = value[i] = list.begin();
      upper[i] = list.end();
      minimum_size = std::min(list.size(), minimum_size);
    }
    // If i has not been fully incremented, one of the predicates was missing.
    // This rule will not have any output, so we advance to the next one. The
    // rule is also skipped if it is non-functional.
    if (i < 2 || minimum_size > kConstraint)
      AdvanceRule();
  }

  // This advances the rule by incrementing the index and initializing the next
  // rule after checking that output hasn't finished.
  void AdvanceRule() {
    rule_index++;
    if (rule_index < num_rules)
      InitializeRule();
  }

  // This operator is used to increment the multiple inner iterators. In the
  // case that end of this rule is reached, the rule is advanced instead.
  void Increment() {
    for (int i = 1; i >= 0; i--)
      if (++value[i] == upper[i])
        value[i] = lower[i];
      else
        break;  // One inner iterator has been incremented successfully.
    // The subjects for the outermost predicate have been traverses, meaning
    // output has finished for the current rule.
    if (value[0] == upper[0])
      AdvanceRule();
  }

  bool GetNextResult(<?=typed_ref_args($outputs_)?>) {
    if (rule_index >= num_rules)
      return false;
    pred = rules[rule][2];
    rule = rule_index + 1;
    subj = std::get<0>(*value[0]);
    obj = std::get<0>(*value[1]);
    Increment();
    return true;
  }
};

class <?=$className?> {
 public:
  // The various data structures for the GIST framework.
  using cGLA = HaltingGLA;
  using Task = uint32_t;
  using LocalScheduler = SimpleScheduler<Task>;
  using WorkUnit = std::pair<LocalScheduler*, cGLA*>;
  using WorkUnits = std::vector<WorkUnit>;

  // The iterator for the fragment result type.
  using Iterator = <?=$className?>_Iterator;

 private:
  // References to the informations stored in the given states.
  const Facts& facts;
  const RulesMatrix rules;

  // The number of distinct objects.
  Facts::MapType::size_type num_objects;

  // The number of rules.
  size_t num_rules;

  // The iterator used to traverse the facts and list all objects.
  Facts::MapType::const_iterator iterator;

  // The mutex used by each fragment for locking the on the iterator.
  std::mutex m_fragments;

 public:
  <?=$className?>(<?=const_typed_ref_args($states_)?>)
      : facts(facts_state),
        rules((decltype(rules)) rules_state.GetList().data()),
        num_objects(facts.size()),
        num_rules(rules_state.GetList().size()) {
  }

  // No workers are allocated. All work is done during the output phase.
  void PrepareRound(WorkUnits& workers, int num_threads) {
    workers = WorkUnits();
  }

  void DoStep(Task& task, cGLA& gla) {
  }

  // There are 2 fragments per object. They perform the two separate loop
  // structures present in Algorithm 4.
  int GetNumFragments() {
    iterator = facts.GetMap().cbegin();
    return 2 * num_objects;
  }

  Iterator* Finalize(int fragment) {
    // The iterator is locked on while accessing the key information.
    std::unique_lock<std::mutex> guard(m_fragments);
    auto object = iterator->first.GetKey0();
    // This fragment is used for outputting the knowledge base facts.
    if (fragment % 2 == 0) {
      return new <?=$className?>_Iterator_Copy(object, num_rules, facts, rules);
    } else {
      ++iterator;
      return new <?=$className?>_Iterator_Loop(object, num_rules, facts, rules);
    }
  }

  bool GetNextResult(Iterator* it, <?=typed_ref_args($outputs_)?>) {
    return it->GetNextResult(<?=args($outputs_)?>);
  }
};

<?
    return [
        'kind'            => 'GIST',
        'name'            => $className,
        'system_headers'  => $sys_headers,
        'user_headers'    => $user_headers,
        'lib_headers'     => $lib_headers,
        'libraries'       => $libraries,
        'extra'           => $extra,
        'iterable'        => true,
        'intermediate'    => false,
        'output'          => $outputs,
        'result_type'     => $result_type,
    ];
}
?>
