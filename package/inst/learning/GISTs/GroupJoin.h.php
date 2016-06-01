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
function GroupJoin($t_args, $outputs, $states) {
    // Class name is randomly generated.
    $className = generate_name('GroupJoin');

    // Processing of input state information.
    $numStates = count($states);
    grokit_assert($numStates == 2,
                  "GroupJoin: Expected 2 states. Received $numStates.");
    $states_ = array_combine(['facts_state', 'rules_state'], $states);
    $factsInputs = array_slice($states_['facts_state']->input(), 1);

    // Processing of template arguments.
    $fragSize = get_default($t_args, 'fragment.size', 2000000);

    // Setting output types.
    // The inputs to the facts state should be in the order of obj, pred, subj.
    $outputs_ = array_combine(['subj', 'pred', 'obj'], $factsInputs);
    $outputs_['rule'] = lookupType('base::int');
    $outputs = array_combine(array_keys($outputs), $outputs_);

    $sys_headers  = ['math.h', 'unordered_map'];
    $user_headers = [];
    $lib_headers  = ['base\gist.h', 'tuple.h'];
    $libraries    = [];
    $properties   = [];
    $extra        = [];
    $result_type  = ['fragment'];
?>

namespace <?=$className?>_namespace {
// The segmented states for the facts and rules.
using Facts = <?=$states_['facts_state']?>;
using Rules = <?=$states_['rules_state']?>;

// Various types associated with the Group inner GLAs.
using FactsGroup = Facts::InnerGLA;
using FactsMap = FactsGroup::ConstantState::Map;
using FactsValues = FactsGroup::ConstantState::Values;
using RulesGroup = Rules::InnerGLA;
using RulesMap = RulesGroup::ConstantState::Map;
using RulesValues = RulesGroup::ConstantState::Values;

// The types used for objects and predicates.
using Object = <?=$outputs_['obj']?>;
using Predicate = <?=$outputs_['pred']?>;

// The constraint parameter, as described in Definition 4.
constexpr std::size_t kConstraint = 100;

// The number of objects per fragment.
constexpr std::size_t kFragmentSize = <?=$fragSize?>;

// The number of segments.
constexpr std::size_t kNumSegments = Facts::NUM_STATES;

// The bounds for the possible finite values for a predicate.
const Predicate kMinValue = MinValue<Predicate>();
const Predicate kMaxValue = MaxValue<Predicate>();

// This is an abstract class, simply used to hold the shared functionality for
// the iterators below.
class <?=$className?>_Simple_Iterator {
 protected:
  Object object;              // The current object.
  const Facts* facts;         // The mapping for the facts.
  const Rules* rules;         // The mapping for the rules.
  const FactsMap* facts_map;  // The map associated with the facts.
  const RulesMap* rules_map;  // The map associated with the Rules segment.

 public:
  <?=$className?>_Simple_Iterator(const Object& object,
                                  const Facts* facts, const Rules* rules)
      : object(object),
        facts(facts),
        rules(rules),
        facts_map(&facts->GetInnerGLA(object)->GetConstantState().GetInfo()),
        rules_map(&rules->GetInnerGLA(object)->GetConstantState().GetInfo()) {
  }

  <?=$className?>_Simple_Iterator() = default;

  virtual ~<?=$className?>_Simple_Iterator() {}

  virtual bool GetNextResult(<?=typed_ref_args($outputs_)?>) = 0;
};

// This iterator is used for the first loop structure in Algorithm 4. The input
// facts are simply outputted along with a rule ID of 0.
class <?=$className?>_Copy_Iterator : public <?=$className?>_Simple_Iterator {
 private:
  // Iterators for the current and past-the-end predicate for the object.
  FactsMap::const_iterator it, end;

  // Indices for the current and last subject for the (object, predicate) pair.
  std::size_t index, size;

 public:
  // This sets up the iterators for traversing the predicates and subjects.
  <?=$className?>_Copy_Iterator(const Object& object,
                                const Facts* facts, const Rules* rules)
      : <?=$className?>_Simple_Iterator(object, facts, rules),
        it(facts_map->lower_bound(std::make_tuple(object, kMinValue))),
        end(facts_map->upper_bound(std::make_tuple(object, kMaxValue))),
        index(0),
        size(*it->second.first) {
  }

  bool GetNextResult(<?=typed_ref_args($outputs_)?>) {
    // The predicate is advanced if output has finished for the current one.
    if (index == size) {
      // Output is halted once all predicates have been traversed.
      if (++it == end) {
        return false;
      } else {
        index = 0;
        size = (long) *it->second.first;
      }
    }
    pred = std::get<1>(it->first);
    subj = std::get<0>(it->second.second[index]);
    obj = object;
    rule = 0;
    index++;
    return true;
  }
};

// This iterator is used to perform the second loop structure in Algorith 4 and
// the output corresponds to line 7. The body predicates for each rule are
// iterated over recursively, meaning an iterator must be stored for each body
// predicate. The upper and lower bounds must also be stored for each, except
// that a lower bound is not needed for the outermost predicate because it will
// never need to be rolled over. Regardless, this extra lower bound is kept for
// sake of simplicity.
// TODO: This is currently hard-coded for the single case of length-3 rules. As
//   such, it appears overly complicated. It is planned to allow arbitrary long
//   rules in the future and so it should not be simplified.
class <?=$className?>_Loop_Iterator : public <?=$className?>_Simple_Iterator {
 private:
  // The index used to loop across the predicates.
  int p_index;

  // The iterator used to traverse the rules for the current object.
  RulesValues* it, *begin, *end;

  // Each predicate has iterators for the current, first, and last subjects.
  std::array<const FactsValues*, 2> value, lower, upper;

 public:
  // The object is guaranteed to match at least one rule, meaning the first rule
  // can be initialized without checking if it is past the end.
  <?=$className?>_Loop_Iterator(const Object& object, const Facts* facts,
                                const Rules* rules)
      : <?=$className?>_Simple_Iterator(object, facts, rules) {
    auto& pair = rules_map->lower_bound(std::make_tuple(object))->second;
    begin = it = pair.second;
    end = begin + *pair.first;
    InitializeRule();
  }

  // A default constructed iterator is never used.
  <?=$className?>_Loop_Iterator() = default;

  // This initializes each iterator for the current rule.
  void InitializeRule() {
    for (p_index = 0; p_index < 2; p_index++) {
      // Info is a pair containing the number of subjects and the offset.
      auto& pair = facts_map->at(std::make_tuple(object, (*it)[p_index]));
      lower[p_index] = value[p_index] = pair.second;
      upper[p_index] = lower[p_index] + *pair.first;
    }
  }

  // This advances the rule by incrementing the index and initializing the next
  // rule after checking that output hasn't finished.
  void AdvanceRule() {
    if (++it != end)
      InitializeRule();
  }

  // This operator is used to increment the multiple inner iterators. In the
  // case that end of this rule is reached, the rule is advanced instead.
  void Increment() {
    for (p_index = 1; p_index >= 0; p_index--)
      if (++value[p_index] == upper[p_index])
        value[p_index] = lower[p_index];
      else
        break;  // One inner iterator has been incremented successfully.
    // The subjects for the outermost predicate have been traversed, meaning
    // output has finished for the current rule.
    if (p_index < 0)
      AdvanceRule();
  }

  bool GetNextResult(<?=typed_ref_args($outputs_)?>) {
    if (it == end)
      return false;
    pred = (*it)[2];
    rule = (*it)[3];
    subj = std::get<0>(*value[0]);
    obj = std::get<0>(*value[1]);
    Increment();
    return true;
  }
};

// This is the iterator actually used for the fragments. It will perform output
// for several objects, using loop and copy iterators for each.
class <?=$className?>_Iterator : public <?=$className?>_Simple_Iterator {
 public:
    using Simple_Iterator = <?=$className?>_Simple_Iterator;
    using Copy_Iterator = <?=$className?>_Copy_Iterator;
    using Loop_Iterator = <?=$className?>_Loop_Iterator;

 private:
  // The smallest object for which this iterator does not produce outputs.
  const Object& end;

  // The copy and loop iterators used per object.
  Copy_Iterator copy_iter;
  Loop_Iterator loop_iter;

  // A value recording the current state for this iterator. The values are:
  // 0 - copy iterator, 1 - loop iterator.
  int state;

 public:
  // The iterators are copied, which is necessary because the fragments before
  // and after this will be using the begin and end iterators, respectively.
  <?=$className?>_Iterator(const Object& begin, const Object& end,
                           const Facts* facts, const Rules* rules)
      : Simple_Iterator(begin, facts, rules),
        end(end),
        copy_iter(object, facts, rules),
        state(0) {}

  // The next result is outputted, incrementing any of the inner iterators or
  // the current object along the way.
  bool GetNextResult(<?=typed_ref_args($outputs_)?>) {
    if (state == 0) {
      bool result = copy_iter.GetNextResult(<?=args($outputs_)?>);
      if (result) {
        return true;  // A result has been received.
      } else if (rules_map->count(std::make_tuple(object)) > 0) {
          // This object has a loop iterator.
          state = 1;  // No result so far so the loop iterator is began.
          loop_iter = Loop_Iterator(object, facts, rules);
          return GetNextResult(<?=args($outputs_)?>);
      }  // No loop iterator for this object, the object is advanced below.
    } else {
      bool result = loop_iter.GetNextResult(<?=args($outputs_)?>);
      if (result)
        return true;  // A result has been received.
    }
    // Output for this object has finished. The object must be advanced.
    // if (object % 10000 == 0)
      // std::cout << "Finished object: " << iter->first.GetKey0() << std::endl;
    // The object is advanced to the next smallest value in the info map.
    object = std::get<0>(facts_map->upper_bound(std::make_tuple(object, kMaxValue))->first);
    if (object == end) {
      return false;  // No more objects, so the output is finished.
    } else {
      // Iterators for the next object are created and output is began.
      copy_iter = Copy_Iterator(object, facts, rules);
      state = 0;
      return GetNextResult(<?=args($outputs_)?>);
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
  using Iterator = <?=$className?>_Iterator;

 private:
  // References to the informations stored in the given states.
  const Facts& facts;
  const Rules& rules;

  // Each object set is broken up into intervals for the fragment result. These
  // objects represent the bounds of those intervals. Each segment is broken up
  // separately such that no fragment crosses a segment.
  std::array<std::vector<Object>, kNumSegments> objects;

  // This vector maps the fragment number to a pair containing its corresponding
  // segment number and the number of fragments used by previous segments.
  std::vector<std::pair<std::size_t, std::size_t>> map;

 public:
  <?=$className?>(<?=const_typed_ref_args($states_)?>)
      : facts(facts_state),
        rules(rules_state) {
  }

  // No workers are allocated. All work is done during the output phase.
  void PrepareRound(WorkUnits& workers, int num_threads) {
    workers = WorkUnits();
  }

  void DoStep(Task& task, cGLA& gla) {}

  // Each segment is divided into fragments independently. Each object set is
  // split into fragments containing at most kFragmentsSize distinct objects.
  // It should not be expected that the output is split evenly across objects,
  // so these fragments are not necessarily of equal size.
  int GetNumFragments() {
    std::size_t num_fragments = 0;
    // The segments are iterated over in order.
    for (std::size_t segment = 0; segment < kNumSegments; segment++) {
      // The grouping and its info map for the current segment.
      FactsGroup* group = facts.GetConstantState().segments.Peek(segment);
      const FactsMap& info = group->GetConstantState().GetInfo();
      // The object set is traversed in ascending order per segment. The current
      // object is copied whenever it lies on a chunk boundary. Note that this
      // loop immediately pushes back a copy of the minimal object.
      FactsMap::const_iterator it = info.begin(), end = info.end();
      Object obj;
      for (std::size_t index = 0; it != end; index++,
           it = info.upper_bound(std::make_tuple(obj, kMaxValue))) {
        obj = std::get<0>(it->first);
        if (index % kFragmentSize == 0) {
          objects[segment].push_back(obj);
          map.emplace(map.end(), segment, num_fragments);
        }
      }
      // The upper boundary for the last fragment is the end of the map. This
      // fragment does not necessary have as many elements as the rest.
      objects[segment].push_back(std::get<0>(it->first));
      num_fragments += objects[segment].size() - 1;
    }
    std::cout << "There are " << num_fragments << " fragments" << std::endl;
    return num_fragments;
  }

  Iterator* Finalize(int fragment) const {
    std::size_t segment = map[fragment].first;
    std::size_t index = fragment - map[fragment].second;
    return new Iterator(objects[segment][index], objects[segment][index + 1],
                        &facts, &rules);
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
