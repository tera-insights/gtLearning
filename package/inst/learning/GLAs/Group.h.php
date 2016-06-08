<?
// This GLA accumulates its inputs into a single large vector. It is similar to
// base::Gather with several key differences that increase performance.
// 1) It can group the inputs based on their values before storing them.
// 2) Memory allocation is done only once for a single vector but insertions are
//    done in parallel across many processes.
// The primary trade-off is that two iterations are made over the data, as an
// initial iteration is needed to set up the vector in the shared state.

// Template Arguments:
// split: The number of inputs to be grouped on.
// use.array: Whether an array is used instead of a tuple to store the inputs
//   that aren't grouped on.
// debug: An integer code describing the level of debug messages to output.
// fragment.size: The number of groups outputted per fragment.

// Resources:
// gist.h: Various GIST components.
function Group($t_args, $inputs, $outputs, $states) {
    // Class name is randomly generated.
    $className = generate_name('Group');

    // Processing of template arguments.
    $split = $t_args['split'];
    grokit_assert(is_integer($split), 'Group: split should be an integer.');
    grokit_assert(0 < $split && $split < \count($inputs),
                  'Group: split should be in the interval (0, #inputs).');
    $useArray = get_default($t_args, 'use.array', false);
    $debug = get_default($t_args, 'debug', 0);
    $fragSize = get_default($t_args, 'fragment.size', 2000000);
    $deleteContents = get_default($t_args, 'delete.contents', false);

    // Processing of inputs.
    $keys = array_slice($inputs, 0, $split);
    $vals = array_slice($inputs, $split);

    // Local names for the inputs are generated.
    foreach (array_values($keys) as $index => $type)
        $inputs_["key$index"] = $type;
    foreach (array_values($vals) as $index => $type)
        $inputs_["val$index"] = $type;

    // Re-assigned using local names for the inputs.
    $keys = array_slice($inputs_, 0, $split);
    $vals = array_slice($inputs_, $split);

    // Checking that use.array is valid.
    if ($useArray) {
        $innerType = array_get_index($vals, 0);
        foreach ($vals as $type)
             grokit_assert($innerType == $type,
                           'Group: array must contain equivalent types.');
        $numVals = \count($vals);
    }

    // The GroupBy GLA used for enumerating the groups in the first iteration.
    $countGLA = lookupGLA('base::Count', [], [], ['count' => NULL]);
    $group = array_combine(array_keys($keys), array_keys($keys));
    $templateArgs = ['group' => $group, 'aggregate' => $countGLA];
    $outputsGLA = array_merge($keys, ['count' => NULL]);
    $groupByGLA = lookupGLA('base::GroupBy', $templateArgs, $keys, $outputsGLA);

    // Processing of outputs.
    grokit_assert(\count($inputs) == \count($outputs),
                  'Group: expected equal number of inputs and outputs.');
    $outputs = array_combine(array_keys($outputs), $inputs);
    $outputs_ = $inputs_;

    $sys_headers  = [];
    $user_headers = [];
    $lib_headers  = [];
    $libraries    = [];
    $properties   = [];
    $extra        = [];
    $result_type  = ['fragment', 'multi'];
    $post_finalize = $deleteContents;
?>

class <?=$className?>;

<?  $constantState = lookupResource(
        'learning::Group_Constant_State',
        ['className' => $className, 'keys' => $keys, 'values' => $vals,
         'use.array' => $useArray]
    ); ?>

class <?=$className?> {
 public:
  // The constant state for the iterable GLA.
  using ConstantState = <?=$constantState?>;

  // The class used for the GroupBy GLA.
  using GroupByGLA = <?=$groupByGLA?>;

  // The number of groups per fragment.
  static constexpr std::size_t kFragmentSize = <?=$fragSize?>;

  // The type used for the mapping in the constant state.
  using Map = ConstantState::Map;

  // The iterator for the fragment result type;
  using Iterator = struct {
    Map::const_iterator current, end;
    std::size_t index;
  };

 private:
  // The aggregate used during the first round to compute counts;
  GroupByGLA::ConstantState constant_state;
  GroupByGLA aggregate;

  // The shared state modified during the second round.
  ConstantState& state_;

  // Fields used for the multi return type.
  std::size_t item_counter;
  Map::iterator group_iter;

  // Fields used for the fragment result type.
  std::vector<Map::const_iterator> iterators;

 public:
  <?=$className?>(const ConstantState& state)
      : aggregate(constant_state),
        state_(const_cast<ConstantState&>(state)) {}

  void AddItem(<?=const_typed_ref_args($inputs_)?>) {
    if (state_.iteration == 0) {
      aggregate.AddItem(<?=args($keys)?>);
    } else {
      auto keys = std::make_tuple(<?=args($keys)?>);
<?  if ($useArray) { ?>
      ConstantState::Values vals {<?=args($vals)?>};
<?  } else { ?>
      auto vals = std::make_tuple(<?=args($vals)?>);
<?  } ?>
      auto& info = state_.info[keys];
      info.second[(*info.first)++] = vals;
    }
  }

  void AddState(const <?=$className?>& other) {
    if (state_.iteration == 0)
      aggregate.AddState(const_cast<GroupByGLA&>(other.aggregate));
  }

  bool ShouldIterate(ConstantState& state) {
    if (state.iteration++ == 0) {
      aggregate.Finalize();
      <?=array_template('{val} {key};', PHP_EOL, $keys)?>;
      long count, total_count = 0;
      while (aggregate.GetNextResult(<?=args($keys)?>, count)) {
        std::unique_ptr<std::atomic_long> ptr (new std::atomic_long(count));
        state.info.emplace(std::make_tuple(<?=args($keys)?>),
                           std::make_pair(std::move(ptr), nullptr));
        total_count += count;
      }
<?  if ($debug > 0) { ?>
      std::cout << "There are " << state.info.size() << " groups." << std::endl;
      std::cout << "There are " << total_count << " elements." << std::endl;
<?  } ?>
      state.data.reset(new ConstantState::Values[total_count]);
      total_count = 0;
      for (auto it = state.info.begin(); it != state.info.end(); ++it) {
        it->second.second = state.data.get() + total_count;
        total_count += *it->second.first;
        *it->second.first = 0;
      }
      return true;
    } else {
      return false;
    }
  }

  // Finalize for the multi return type.
  void Finalize() {
    group_iter = state_.info.begin();
    item_counter = 0;
  }

  // The GetNextResult for the multi return type. It simply iterates over each
  // group in order, outputting the values in the order they were seen.
  bool GetNextResult(<?=typed_ref_args($outputs_)?>) {
    if (group_iter == state_.info.end())
      return false;
<?  foreach (array_keys($keys) as $index => $name) { ?>
    <?=$name?> = std::get<<?=$index?>>(group_iter->first);
<?  } ?>
<?  foreach (array_keys($vals) as $index => $name) { ?>
    <?=$name?> = std::get<<?=$index?>>(group_iter->second.second[item_counter]);
<?  } ?>
    if (++item_counter == *group_iter->second.first) {
      ++group_iter;
      item_counter = 0;
    }
    return true;
  }

  // The groups are traversed in order and the boundaries for each are stored in
  // the iterators vector. The number of groups per fragment is determined by
  // kFragmentSize.
  int GetNumFragments() {
    std::size_t num_fragment = 0;
    auto it = state_.info.cbegin();
    // The iterator is incremented kFragmentSize times between each boundary.
    // Note that this loop immediately pushes back an iterator pointing to the
    // minimal grouping.
    for (std::size_t index = 0; it != state_.info.cend(); ++it, index++)
      if (index % kFragmentSize == 0)
        iterators.push_back(it);
    // This pushes back the past-the-end iterator, the upper boundary for the
    // last fragment, which can have fewer than kFragmentSize groups.
    iterators.push_back(it);
<?  if ($debug > 0) { ?>
    std::cout << "There are " << iterators.size() << " iterators." << std::endl;
<?  } ?>
    return iterators.size() - 1;
  }

  // Simply creates a pair representing an interval from adjacent boundaries.
  Iterator* Finalize(int fragment) const {
    return new Iterator{iterators[fragment], iterators[fragment + 1], 0};
  }

  bool GetNextResult(Iterator* it, <?=typed_ref_args($outputs_)?>) {
  if (it->current == it->end)
    return false;
<?  foreach (array_keys($keys) as $index => $name) { ?>
    <?=$name?> = std::get<<?=$index?>>(it->current->first);
<?  } ?>
<?  foreach (array_keys($vals) as $index => $name) { ?>
    <?=$name?> = std::get<<?=$index?>>(it->current->second.second[it->index]);
<?  } ?>
    if (++it->index == *it->current->second.first) {
      ++it->current;
      it->index = 0;
    }
    return true;
  }

<?  if ($post_finalize) { ?>
  // This is only done to free-up memory after the results phase.
  void PostFinalize() {
    if (state_.iteration < 2)
      return;
    state_.data.reset();
    state_.info.clear();
  }
<?  } ?>

  const ConstantState& GetConstantState() const {
    return state_;
  }
};

using <?=$className?>_Iterator = <?=$className?>::Iterator;

<?
    return [
        'kind'            => 'GLA',
        'name'            => $className,
        'system_headers'  => $sys_headers,
        'user_headers'    => $user_headers,
        'lib_headers'     => $lib_headers,
        'libraries'       => $libraries,
        'properties'      => $properties,
        'extra'           => $extra,
        'iterable'        => true,
        'intermediates'   => false,
        'generated_state' => $constantState,
        'input'           => $inputs,
        'output'          => $outputs,
        'result_type'     => $result_type,
        'post_finalize'   => $post_finalize,
    ];
}

// This is the shared state for the Group GLA. It is intended that the GLA does
// not respect the constantness of the state and insteads updates it before the
// ShouldIterate call. This is done to improved performance and limit the amount
// of memory allocation. The state can be updated in parallel because the inner-
// most indices are atomic, meaning two processes cannot see the same index and
// update into the same location before modifying the index.

// Resources:
// tuple: tuple
// utility: pair
// map: map
// atomic: atomic_long
// memory: unique_ptr
function Group_Constant_State($t_args) {
    // Processing of template arguments.
    $className = $t_args['className'];
    $vals = $t_args['values'];
    $keys = $t_args['keys'];

    // Information for array usage.
    $useArray = $t_args['use.array'];
    $numVals = \count($vals);
    $innerType = array_get_index($vals, 0);

    $sys_headers  = ['tuple', 'utility', 'map', 'atomic', 'memory'];
    $user_headers = [];
    $lib_headers  = [];
    $libraries    = [];
    $properties   = [];
    $extras       = [];
?>

class <?=$className?>ConstantState {
 public:
  // The keys and values are packed into tuples.
  using Keys = std::tuple<<?=typed($keys)?>>;
<?  if ($useArray) { ?>
  static constexpr std::size_t kNumValues = <?=$numVals?>;
  using ValueType = <?=$innerType?>;
  using Values = std::array<ValueType, kNumValues>;
<?  } else { ?>
  using Values = std::tuple<<?=typed($vals)?>>;
<?  } ?>
  using Map = std::map<Keys, std::pair<std::unique_ptr<std::atomic_long>, Values*>>;

 private:
  // The data for all the groups is stored abstractly at this location.
  std::unique_ptr<Values> data;

  // The address and count information for each group.
  Map info;

  // The current iteration.
  int iteration;

 public:
  friend class <?=$className?>;

  <?=$className?>ConstantState()
      : iteration(0) {}

  const Map& GetInfo() const {
    return info;
  }

  const Values* GetData() const {
    return data.get();
  }
};
<?
    return [
        'kind'           => 'RESOURCE',
        'name'           => $className . 'ConstantState',
        'system_headers' => $sys_headers,
        'user_headers'   => $user_headers,
        'lib_headers'    => $lib_headers,
        'libraries'      => $libraries,
        'configurable'   => false,
    ];
}
?>
