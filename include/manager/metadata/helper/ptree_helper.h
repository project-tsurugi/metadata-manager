#pragma once

#include <vector>
#include <string_view>
#include <boost/property_tree/ptree.hpp>

namespace ptree_helper {
  boost::property_tree::ptree make_array_ptree(const std::vector<int64_t>& v); 
  std::vector<int64_t> make_vector_int(boost::property_tree::ptree pt, 
                                    std::string_view key);
}
