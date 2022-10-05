#include "manager/metadata/metadata_factory.h"

namespace manager::metadata {
  /**
   * @brief
   */
  std::unique_ptr<Metadata> get_tables_ptr(std::string_view database) {
    return std::make_unique<Tables>(database);
  }

  /**
   * @brief
   */
  std::unique_ptr<Metadata> get_indexes_ptr(std::string_view database) {
    return std::make_unique<Indexes>(database);
  }

} // namespace manager::metadata