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
  for (const auto& value : v) {
    pt.put("", value);
  }
  return pt;
}

/**
 * @brief
 */
std::vector<int64_t> make_vector(boost::property_tree::ptree pt, 
                                std::string_view key) {
  std::vector<int64_t> v;
  BOOST_FOREACH (const auto& node, pt.get_child(key.data())) {
    const auto& value = node.second;
    auto opt = value.get_optional<int64_t>("");
    opt ? v.emplace_back(opt.get()) : v.emplace_back(metadata::INVALID_VALUE);
  }
  return v;
}

} // namespace ptree_helper
