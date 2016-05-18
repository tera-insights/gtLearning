<?
// This GLA accumulates its inputs into a single large vector. It is similar to
// base::Gather with several key differences that increase performance.
// 1) It can group the inputs based on their values before storing them.
// 2) Memory allocation is done only once for a single vector but insertions are
//    done in parallel across many processes.
// The primary trade-off is that two iterations are made over the data, as an
// initial iteration is needed to set up the vector in the shared state.
function Group($t_args, $inputs, $outputs, $states) {
    // Class name is randomly generated.
    $className = generate_name('Group');

    // Processing of template arguments.
    $split = $t_args['split'];
    grokit_assert(is_integer($split), 'Group: split should be an integer.');
    grokit_assert(0 < $split && $split < \count($inputs),
                  'Group: split should be in the interval (0, #inputs).');

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

    $sys_headers  = ['math.h', 'unordered_map'];
    $user_headers = [];
    $lib_headers  = ['base\gist.h'];
    $libraries    = [];
    $properties   = [];
    $extra        = [];
    $result_type  = ['multi'];
?>

class <?=$className?>;

<?  $constantState = lookupResource(
        'learning::Group_Constant_State',
        ['className' => $className, 'keys' => $keys, 'values' => $vals]
    ); ?>

class <?=$className?> {
 public:
  // The shared stated for the iterable GLA.
  using State = <?=$constantState?>;

  // The class used for the GroupBy GLA.
  using GroupByGLA = <?=$groupByGLA?>;

 private:
  // The aggregate used during the first round to compute counts;
  GroupByGLA::ConstantState constant_state;
  GroupByGLA aggregate;

  // The shared state modified during the second round.
  State& state_;

  std::size_t item_counter;
  decltype(state_.info)::iterator group_iter;

 public:
  <?=$className?>(const State& state)
      : aggregate(constant_state),
        state_(const_cast<State&>(state)) {}

  void AddItem(<?=const_typed_ref_args($inputs_)?>) {
    if (state_.iteration == 0) {
      aggregate.AddItem(<?=args($keys)?>);
    } else {
      auto keys = std::make_tuple(<?=args($keys)?>);
      auto vals = std::make_tuple(<?=args($vals)?>);
      auto& info = state_.info[keys];
      state_.data[info.second + (*info.first)++] = vals;
    }
  }

  void AddState(const <?=$className?>& other) {
    if (state_.iteration == 0)
      aggregate.AddState(const_cast<GroupByGLA&>(other.aggregate));
  }

  bool ShouldIterate(State& state) {
    if (state_.iteration++ == 0) {
      aggregate.Finalize();
      <?=array_template('{val} {key};', PHP_EOL, $keys)?>;
      long count, total_count = 0;
      while (aggregate.GetNextResult(<?=args($keys)?>, count)) {
        std::unique_ptr<std::atomic_long> ptr (new std::atomic_long(count));
        state.info.emplace(std::make_tuple(<?=args($keys)?>),
                           std::make_pair(std::move(ptr), 0));
        total_count += count;
      }
      state.data = new State::Values[total_count];
      total_count = 0;
      for (auto it = state.info.begin(); it != state.info.end(); ++it) {
        it->second.second = total_count;
        total_count += *it->second.first;
        *it->second.first = 0;
      }
      return true;
    } else {
      return false;
    }
  }

  void Finalize() {
    group_iter = state_.info.begin();
    item_counter = 0;
  }

  bool GetNextResult(<?=typed_ref_args($outputs_)?>) {
    if (group_iter == state_.info.end())
      return false;
<?  foreach (array_keys($keys) as $index => $name) { ?>
    <?=$name?> = std::get<<?=$index?>>(group_iter->first);
<?  } ?>
<?  foreach (array_keys($vals) as $index => $name) { ?>
    <?=$name?> = std::get<<?=$index?>>(state_.data[group_iter->second.second + item_counter]);
<?  } ?>
    if (++item_counter == *group_iter->second.first) {
      ++group_iter;
      item_counter = 0;
    }
    return true;
  }
};

<?
    return [
        'kind'             => 'GLA',
        'name'             => $className,
        'system_headers'   => $sys_headers,
        'user_headers'     => $user_headers,
        'lib_headers'      => $lib_headers,
        'libraries'        => $libraries,
        'properties'       => $properties,
        'extra'            => $extra,
        'iterable'         => true,
        'generated_state'  => $constantState,
        'input'            => $inputs,
        'output'           => $outputs,
        'result_type'      => $result_type,
    ];
}

// This is the shared state for the Group GLA. It is intended that the GLA does
// not respect the constantness of the state and insteads updates it before the
// ShouldIterate call. This is done to improved performance and limit the amount
// of memory allocation. The state can be updated in parallel because the inner-
// most indices are atomic, meaning two processes cannot see the same index and
// update into the same location before modifying the index.
function Group_Constant_State($t_args) {
    // Processing of template arguments.
    $className = $t_args['className'];
    $vals = $t_args['values'];
    $keys = $t_args['keys'];

    $sys_headers  = ['tuple', 'utility', 'map', 'atomic'];
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
  using Values = std::tuple<<?=typed($vals)?>>;

 private:
  // The data for all the groups is stored abstractly at this location.
  Values* data;

  // The address and count information for each group.
  std::map<Keys, std::pair<std::unique_ptr<std::atomic_long>, std::size_t>> info;

  // The current iteration.
  int iteration;

 public:
  friend class <?=$className?>;

  <?=$className?>ConstantState()
      : iteration(0) {}
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
