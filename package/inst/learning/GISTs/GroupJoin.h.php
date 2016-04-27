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
    grokit_assert($numStates == 2),
                  "GroupJoin: Expected 2 states. Received $numStates.");
    $states_ = array_merge(['facts_state', 'rules_state'], $states);

    $factsInputs = $states_['facts_state']->input();
    // If the facts state is a Segmenter on top of a GroupBy, the first input
    // used for segmenter is removed.
    if ($states_['facts_state']->name() == 'BASE::SEGMENTER')
        $factsInputs = array_slice($factsInputs, 1);

    // The rules state should be a base::Gather GLA with 3 identical inputs.
    $rulesInputs = $states_['rules_state']->input();
    $ruleType = array_get_index($rulesInputs, 0);

    // Initialization of local variables from template arguments.

    // Setting output types.
    // The inputs to the facts state should be in the order of obj, pred, subj.
    $outputs_ = array_merge(['obj', 'pred', 'subj'], $factsInputs);
    $outputs_['rule'] = $ruleType;
    $outputs = array_combine(array_keys($outputs), $outputs);

    $sys_headers  = ['math.h', 'unordered_map'];
    $user_headers = [];
    $lib_headers  = [];
    $libraries    = [];
    $extra        = [];
    $result_type  = ['fragment'];
?>

using namespace std;

class <?=$className?>;

class <?=$className?> {
 public:
  // The various data structures for the GIST framework.
  using cGLA = HaltingGLA;
  using Task = uint32_t;
  using LocalScheduler = BlockScheduler<Task>;
  using WorkUnit = std::pair<LocalScheduler*, cGLA*>;
  using WorkUnits = std::vector<WorkUnit>;

  // The containers for the facts and rules.
  using Facts = <?=$states_['facts']?>::MapType;
  using Rules = <?=$states_['rules']?>::Vector;

  // The map used for the predicate -> subject mapping per object.
  using Map = MapType::InnerGLA::Map;

  // The types used for objects and predicates.
  using Object = Facts::key_type;
  using Predicate = Map::key_type;

  // The number of predicates in each rule.
  const constexpr auto kNumRules = std::tuple_size<Rules::value_type>::size;

  // The rules information is changed from a vector of predicate tuples to a
  // simplified C array. This allows for easier reference.
  using RulesMatrix = std::array<Predicate, kNumRules>>*;

  // The iterator for the fragment result type. The body predicates are iterated
  // over recursively for each rule, meaning an iterator must be stored for each
  // body predicate. The upper and lower bounds must also be stored for each,
  // except that a lower bound is not needed for the outermost predicate because
  // it will never need to be rolled over.
  // TODO: This is currently hard-coded for the single case of length-3 rules.
  //   As such, it appears overly complicated. It is planned to allow arbitrary
  //   length rules in the future and so it should not be simplified.
  class Iterator {
   using namespace std;

   private:
    int fragment;                          // The fragment ID.
    Map::key_type object;                  // The associated object.
    const Facts& facts;                    // The mapping for the facts.
    const RulesMatrix rules;               // The container of rules.
    int rule;                              // The index of the current rule.
    array<Map::iterator, 2> values;        // Current value for each iterator.
    array<pair<Map::iterator>, 2> bounds;  // The bounds for each iterator.

   public:
    Iterator(int fragment, const Object& object, const Facts& facts,
             const RulesMatrix rules)
        : fragment(fragment),
          object(object),
          facts(facts),
          rules(rules),
          rule(0)  {
      // This fragment just outputs the facts associated with the object.
      if (fragment % 2 == 0) {
        value[0] = facts.at(object).begin();
        upper[0] = facts.at(object).end();
      } else {
        Initialize(rule);
      }
    }

    // This initializes each iterator for a given rule.
    void Initialize(int rule) {
      for (int i = 0; i < 2; i++) {
        bounds[i] = facts.at(object).equal_range(rules[rule][i]);
        values[i] = bounds[i].first;
      }
    }

    // This operator is used to increment the multiple inner iterators. In the
    // case that end of this rule is reached, the rule is incremented instead.
    Iterator operator++() {
      for (int i = 1; i >= 0; i--)
        if (++values[i] == bounds[i].second)
          values[i] = bounds[i].first;
        else
          break;  // One inner iterator has been incremented successfully.
      if (values[0] == bounds[0]) {
        rule++;
        if (rule < num_rules)
          Initialize(rule);
      }
    }

    bool GetNextResult(Predicate& head, int& rule, Object& sub0, Object& sub1) {
      if (fragment % 2 == 0) {
        if (value[0] == upper[0])
          return false;
        head = value[0]->first;
        sub0 = value[0]->second;
        sub1 = object;
        rule = 0;
        ++value[0];
        return true;
      } else {
        if (rule > num_rules)
          return false;
        head = rules[rule][2];
        rule = this->rule;
        sub0 = values[0]->second;
        sub1 = values[1]->second;
        *this++;
        return true;
      }
    }
  }

 private:
  // References to the informations stored in the given states.
  const Facts& facts;
  const RulesMatrix rules;

  // The number of distinct objects.
  Facts::size_type num_objects;

  // The iterator used to traverse the facts and list all objects.
  Facts::const_iterator iterator;

  // The mutex used by each fragment for locking the on the iterator.
  std::mutex m_fragments;

 public:
  <?=$className?>(<?=const_typed_ref_args($states_)?>)
      : facts(facts_state.GetMap()),
        rules(rules_state.GetList().data()),
        num_objects(facts.size()) {
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
    iterator = facts.cbegin();
    return 2 * num_objects;
  }

  Iterator* Finalize(int fragment) {
    // The iterator is locked on while accessing the key information.
    unique_lock<std::mutex> guard(m_fragments);
    auto object = iterator->first;
    ++iterator;
    // This fragment is used for outputting the knowledge base facts.
    return new Iterator(fragment, object, facts, rules);
  }

  bool GetNextResult(Iterator* it, <?=typed_ref_args($outputs_)?>) {
    return it->GetNextResult(<?=args(outputs_)?>);
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
