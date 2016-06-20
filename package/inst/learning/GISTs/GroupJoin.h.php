<?
// This GIST is used to perform the GroupJoin algorithm discussed in the paper,
// Ontological Pathfinder: Mining First Order Knowledge from Large Knowledge
// Bases. It takes in two states containing the knowledge base and rules. The
// first should be a two-layer mapping of object -> predicate -> subject. Each
// distinct object should be mapped to a sub-map mapping each predicate to the
// various subjects for that object-predicate pairing. The second state simply
// needs to be an iterable collection of the rules, where each rule is a tuple
// of predicates whose first element is the implied predicate.

// Template Args:
// fragment.size: The number of groups outputted per fragment.

// Resources:
// functional: reference_wrapper
function GroupJoin($t_args, $outputs, $states) {
    // Class name is randomly generated.
    $className = generate_name('GroupJoin');

    // Processing of template arguments.
    $fragSize = get_default($t_args, 'fragment.size', 2000000);
    $ruleType = $t_args['rule.type'];

    // Processing of input state information.
    $numStates = count($states);
    grokit_assert($numStates == 2,
                  "GroupJoin: Expected 2 states. Received $numStates.");
    $states_ = array_combine(['facts_state', 'rules_state'], $states);
    // This relies heavily on the structure of the input and should be changed.
    // The facts_state is a multiplexer with at least one GLA. That first GLA is
    // a Segmenter, so its first input is skipped. The inner GLA has 3 inputs,
    // which are subject, predicate, object or the reverse. The order does not
    // matter because subject and object have the same type.
    $factsInputs = array_slice($states_['facts_state']->input(), 1, 3);

    // Setting output types.
    // The inputs to the facts state should be in the order of obj, pred, subj.
    $outputs_ = array_combine(['subj', 'pred', 'obj'], $factsInputs);
    $outputs_['id'] = lookupType('base::int');
    $outputs = array_combine(array_keys($outputs), $outputs_);

    $sys_headers  = ['functional'];
    $user_headers = [];
    $lib_headers  = ['base\gist.h', 'tuple.h'];
    $libraries    = [];
    $properties   = [];
    $extra        = [];
    $result_type  = ['fragment'];
?>

namespace <?=$className?>_namespace {
// The segmented states for the facts and rules.
using Multiplexer = <?=$states_['facts_state']?>;
using Rules = <?=$states_['rules_state']?>;
using Facts = Multiplexer::GLA0;

// Various types associated with the Group inner GLAs.
using FactsGroup = Facts::InnerGLA;
using FactsMap = FactsGroup::ConstantState::Map;
using FactsValues = FactsGroup::ConstantState::Values;
using RulesGroup = Rules::InnerGLA;
using RulesMap = RulesGroup::ConstantState::Map;
using RulesValues = RulesGroup::ConstantState::Keys;
using RulesIterator = RulesMap::const_iterator;

// The types used for encode the various components of each rule.
using Argument = <?=$outputs_['obj']?>;
using Predicate = <?=$outputs_['pred']?>;
using RuleID = <?=$outputs_['id']?>;

// The number of objects per fragment.
constexpr std::size_t kFragmentSize = <?=$fragSize?>;

// The number of segments.
constexpr std::size_t kNumSegments = Facts::NUM_STATES;

// There are always 3 arguments for a pair of body predicates.
constexpr std::size_t kLength = 3;

// The type of the rule. It is shifted by 3 because the only relevant types were
// originally labelled 3 - 6.
constexpr unsigned int kRule = <?=$ruleType?> - 3;

// Each of the two non-shared arguments is either a subject or an object, which
// is determined by the rule type. This array encodes the type of argument each
// is: 0 for subject and 1 for object.
constexpr std::array<std::size_t, 2> kArgTypes = {kRule % 2, kRule / 2};

// The bounds for the possible finite values for a predicate.
const Predicate kMinValue = MinValue<Predicate>();
const Predicate kMaxValue = MaxValue<Predicate>();

// This iterator is used to perform the second loop structure in Algorithm 4 and
// the output corresponds to line 7. The body predicates for each rule are
// iterated over recursively, meaning an iterator must be stored for each body
// predicate. The upper and lower bounds must also be stored for each, except
// that a lower bound is not needed for the join argument because it will
// never need to be rolled over. Regardless, this extra lower bound is kept for
// sake of simplicity.
class RuleIterator {
 private:
  // A reference to the current rule.
  const RulesValues rule;

  // The mapping of rules to the join arguments for which they have output.
  const Rules& rules;

  // The s-p-o and o-p-s mappings, in that order.
  std::array<const Facts*, 2> facts;

  // There are iterators for the current, first, and last values for each of the
  // three arguments present. The arguments are ordered as left, right, join.
  std::array<const FactsValues*, kLength> value, lower, upper;

 public:
  // The rule is guaranteed to have at least one join argument, meaning the
  // first rule can be initialized without checking if it is past the end.
  RuleIterator(const RulesValues& rule, const Rules& rules,
               const Facts* spo, const Facts* ops)
      : rule(rule),
        rules(rules),
        facts{{spo, ops}} {
    // The iterators for the join argument are constructed.
    auto& rules_map = rules.GetInnerGLA(rule[0]).GetConstantState().GetInfo();
    auto& pair = rules_map.at(rule);
    value[kLength - 1] = lower[kLength - 1] = pair.second;
    upper[kLength - 1] = lower[kLength - 1] + *pair.first;
    InitializeJoin();
  }

  // This initializes the two other iterators for the current join argument.
  void InitializeJoin() {
    // The value of the current join argument is retrieved.
    Argument join = std::get<0>(*value[kLength - 1]);
    for (int p_index = 0; p_index < kLength - 1; p_index++) {
      // The Group GLA containing the join variable as a key.
      const FactsGroup& facts_GLA = facts[kArgTypes[p_index]]->GetInnerGLA(join);
      // The mapping (either sp-o or op-s) for that GLA.
      auto& facts_map = facts_GLA.GetConstantState().GetInfo();
      // The entry in that mapping for the join variable and the body predicate.
      if (facts_map.count(std::make_tuple(join, rule[2 + p_index])) == 0)
        std::cout << "Missing key: {" << join << ", " << (rule[2 + p_index]) << "} in facts_map " << (kArgTypes[p_index]) << std::endl;
      auto& pair = facts_map.at(std::make_tuple(join, rule[2 + p_index]));
      lower[p_index] = value[p_index] = pair.second;
      upper[p_index] = lower[p_index] + *pair.first;
    }
  }

  // This advances the join argument and initializes the new one, if valid.
  void AdvanceJoin() {
    if (++value[kLength - 1] != upper[kLength - 1])
      InitializeJoin();
  }

  // This method is used to increment the multiple inner iterators. In the case
  // that end of the output for the current join argument is reached, then the
  // join argument is advanced instead.
  void Increment() {
    for (int p_index = kLength - 2; p_index >= 0; p_index--)
      if (++value[p_index] == upper[p_index])
        value[p_index] = lower[p_index];
      else
        return;  // One inner iterator has been incremented successfully.
    // The iterators for both outer arguments  have been traversed, meaning
    // output has finished for the current join argument.
    AdvanceJoin();
  }

  bool GetNextResult(<?=typed_ref_args($outputs_)?>) {
    if (value[kLength - 1] == upper[kLength - 1])
      return false;
    id = std::get<0>(rule);
    pred = std::get<1>(rule);
    subj = std::get<0>(*value[0]);
    obj  = std::get<0>(*value[1]);
    Increment();
    return true;
  }
};

// This is the iterator actually used for the fragments. It will perform output
// for several objects, using loop and copy iterators for each.
class FragmentIterator {
 private:
  // The smallest object for which this iterator does not produce outputs. These
  // are copied by value because boundaries are shared by adjacent fragments.
  RulesIterator it, end;

  // Various information stored to create new iterators with.
  const Facts* spo, *ops;
  const Rules& rules;

  // The loop iterator used per rule.
  std::unique_ptr<RuleIterator> rule_iter;

 public:
  // The iterators are copied, which is necessary because the fragments before
  // and after this will be using the begin and end iterators, respectively.
  FragmentIterator(const RulesIterator& begin, const RulesIterator& end,
                    const Facts* spo, const Facts* ops, const Rules& rules)
      : it(begin),
        end(end),
        spo(spo),
        ops(ops),
        rules(rules),
        rule_iter(new RuleIterator(it->first, rules, spo, ops)) {}

  // The next result is outputted, incrementing any of the inner iterators or
  // the current object along the way.
  bool GetNextResult(<?=typed_ref_args($outputs_)?>) {
    bool result = rule_iter->GetNextResult(<?=args($outputs_)?>);
    if (!result) {  // Output is finished for current rule.
      if (++it == end) {
        return false;
      } else {
        rule_iter.reset(new RuleIterator(it->first, rules, spo, ops));
        return GetNextResult(<?=args($outputs_)?>);
      }
    } else {
      return true;
    }
  }
};
}  // Closing the namespace

using namespace <?=$className?>_namespace;

class <?=$className?> {
 public:
  // The various data structures for the GIST framework.
  using cGLA = HaltingGLA;
  using Task = uint32_t;
  using LocalScheduler = SimpleScheduler<Task>;
  using WorkUnit = std::pair<LocalScheduler*, cGLA*>;
  using WorkUnits = std::vector<WorkUnit>;

  // The iterator for the fragment result type.
  using Iterator = FragmentIterator;

 private:
  // References to the informations stored in the given states.
  const Facts* spo, *ops;
  const Rules& rules;

  // Each rules set is broken up into intervals for the fragment result. These
  // rules represent the bounds of those intervals. Each segment is broken up
  // separately such that no fragment crosses a segment.
  std::array<std::vector<RulesIterator>, kNumSegments> iterators;

  // This vector maps the fragment number to a pair containing its corresponding
  // segment number and the number of fragments used by previous segments.
  std::vector<std::pair<std::size_t, std::size_t>> map;

 public:
  <?=$className?>(<?=const_typed_ref_args($states_)?>)
 <? if ($ruleType == 3) { ?>
      : spo(&facts_state.GetGLA0()),
        ops(nullptr),
<?  } else if ($ruleType == 4 || $ruleType == 5) { ?>
      : spo(&facts_state.GetGLA0()),
        ops(&facts_state.GetGLA1()),
<?  } else { ?>
      : spo(nullptr),
        ops(&facts_state.GetGLA0()),
<?  } ?>
        rules(rules_state) {
  }

  // No workers are allocated. All work is done during the output phase.
  void PrepareRound(WorkUnits& workers, int num_threads) {
    workers = WorkUnits();
  }

  // There are no tasks, so this is never called.
  void DoStep(Task& task, cGLA& gla) {}

  // Each segment is divided into fragments independently. Each rule set is
  // split into fragments containing at most kFragmentsSize distinct rules.
  // It should not be expected that the output is split evenly across rules,
  // so these fragments are not necessarily of equal size.
  int GetNumFragments() {
    std::size_t num_fragments = 0;
    // The segments are iterated over in order.
    for (std::size_t segment = 0; segment < kNumSegments; segment++) {
      // The grouping and its info map for the current segment.
      RulesGroup* group = rules.GetConstantState().segments.Peek(segment);
      const RulesMap& info = group->GetConstantState().GetInfo();
      // The rules set is traversed in ascending order per segment. The current
      // rules is copied whenever it lies on a chunk boundary. Note that this
      // loop immediately pushes back a copy of the first rule.
      RulesIterator it = info.begin(), end = info.end();
      for (std::size_t index = 0, total_count = 0; it != end; index++, ++it) {
        if (index % kFragmentSize == 0) {
          iterators[segment].push_back(it);
          map.emplace(map.end(), segment, num_fragments);
        }
      }
      // The upper boundary for the last fragment is the end of the map. This
      // fragment does not necessary have as many elements as the rest.
      iterators[segment].push_back(it);
      num_fragments += iterators[segment].size() - 1;
    }
    std::cout << "There are " << num_fragments << " fragments" << std::endl;
    return num_fragments;
  }

  Iterator* Finalize(int fragment) const {
    std::size_t segment = map[fragment].first;
    std::size_t index = fragment - map[fragment].second;
    return new Iterator(iterators[segment][index],
                        iterators[segment][index + 1],
                        spo, ops, rules);
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
        'properties'      => $properties,
        'extra'           => $extra,
        'iterable'        => true,
        'intermediate'    => false,
        'output'          => $outputs,
        'result_type'     => $result_type,
    ];
}
?>
