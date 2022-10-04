#include <string_view>
#include <memory>
#include "manager/metadata/metadata.h"
#include "manager/metadata/tables.h"
#include "manager/metadata/indexes.h"

namespace manager::metadata {
  /**
   * @brief
   */
  std::unique_ptr<Metadata> get_tables(std::string_view database) {
    return std::make_unique<Tables>(database);
  }

  /**
   * @brief
   */
  std::unique_ptr<Metadata> get_indexes(std::string_view database) {
    return std::make_unique<Indexes>(database);
  }
}
