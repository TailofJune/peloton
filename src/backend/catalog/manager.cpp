/*-------------------------------------------------------------------------
 *
 * manager.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/manager.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/catalog/manager.h"

namespace nstore {
namespace catalog {


Manager& Manager::GetInstance() {
    static Manager manager;
    return manager;
}

void Manager::SetLocation(const oid_t oid, void *location) {
    locator.insert(std::pair<oid_t, void*>(oid, location));
}

void Manager::SetLocation(const oid_t db_oid, const oid_t table_oid, void *location) {
    std::pair<oid_t, oid_t> db_and_table_oid_pair; // TODO:: RENAME
    db_and_table_oid_pair = std::make_pair(db_oid, table_oid);

    locator2.insert(std::pair<std::pair<oid_t, oid_t>, void*>(std::make_pair(db_oid,table_oid), location));
}

void *Manager::GetLocation(const oid_t oid) const {
    void *location = nullptr;
    try {
        location = catalog::Manager::GetInstance().locator.at(oid);
    }
    catch(std::exception& e) {
        // FIXME
    }
    return location;
}

} // End catalog namespace
} // End nstore namespace


