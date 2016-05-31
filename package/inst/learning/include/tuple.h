// This header file contains several tools relating to tuple.

#include <tuple>
#include <limits>

// This function returns the maximal value for a given type
template <typename T>
inline constexpr T MaxValue() {
  return std::numeric_limits<T>::has_infinity
      ? std::numeric_limits<T>::infinity()
      : std::numeric_limits<T>::max();
}

template <typename T>
inline constexpr T MaxValue(T) {
  return MaxValue<T>();
}

// This function returns the minimal value for a given type. It expects that any
// type with a positive infinity has a negative infinity equal to the negation
// of that positive infinity.
template <typename T>
inline constexpr T MinValue() {
  return std::numeric_limits<T>::has_infinity
      ? -std::numeric_limits<T>::infinity()
      : std::numeric_limits<T>::lowest();
}
