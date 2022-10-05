#include "manager/metadata/helper/ptree_helper.h"

#include <boost/foreach.hpp>
#include "manager/metadata/metadata.h"

namespace ptree_helper {

using namespace manager;
using boost::property_tree::ptree;

// ==========================================================================
// ptree_helper functions.

/**
 * @brief
 */
boost::property_tree::ptree make_array_ptree(const std::vector<int64_t>& v) {
  ptree pt;
  ptree child;
  for (const auto& value : v) {
    pt.put("", value);
    child.push_back(std::make_pair("", pt));
  }

  return child;
}

/**
 * @brief
 */
std::vector<int64_t> make_vector_int(boost::property_tree::ptree pt, 
                                    std::string_view key) {
  std::vector<int64_t> v;
  BOOST_FOREACH (const auto& child, pt.get_child(key.data())) {
    const auto& value = child.second;
    auto opt = value.get_optional<int64_t>("");
    auto e = opt ? opt.get() : metadata::INVALID_VALUE;
    v.emplace_back(e);
  }

  return v;
}

} // namespace ptree_helper
