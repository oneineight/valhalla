
#include "mjolnir/pbfadminparser.h"
#include "mjolnir/util.h"
#include "mjolnir/osmpbfparser.h"
#include "mjolnir/sequence.h"
#include "mjolnir/osmadmin.h"
#include "mjolnir/luatagtransform.h"
#include "mjolnir/idtable.h"

#include <future>
#include <utility>
#include <thread>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>

#include <valhalla/baldr/tilehierarchy.h>
#include <valhalla/midgard/logging.h>

using namespace valhalla::midgard;
using namespace valhalla::baldr;
using namespace valhalla::mjolnir;

namespace {
// Will throw an error if this is exceeded. Then we can increase.
constexpr uint64_t kMaxOSMNodeId = 4000000000;

// Node equality
const auto WayNodeEquals = [](const OSMWayNode& a, const OSMWayNode& b) {
  return a.node_id == b.node_id;
};

struct admin_callback : public OSMPBF::Callback {
 public:
  admin_callback() = delete;
  admin_callback(const admin_callback&) = delete;
  virtual ~admin_callback() {}
  // Construct PBFAdminParser based on properties file and input PBF extract
  admin_callback(const boost::property_tree::ptree& pt, OSMData& osmdata)
  : shape_(kMaxOSMNodeId), members_(kMaxOSMNodeId), osmdata_(osmdata) {

    // Initialize Lua based on config
    lua_.SetLuaNodeScript(pt.get<std::string>("admintagtransform.node_script"));
    lua_.SetLuaNodeFunc(pt.get<std::string>("admintagtransform.node_function"));
    lua_.SetLuaWayScript(pt.get<std::string>("admintagtransform.way_script"));
    lua_.SetLuaWayFunc(pt.get<std::string>("admintagtransform.way_function"));
    lua_.SetLuaRelationScript(pt.get<std::string>("admintagtransform.relation_script"));
    lua_.SetLuaRelationFunc(pt.get<std::string>("admintagtransform.relation_function"));
    lua_.OpenLib();
  }

  void node_callback(uint64_t osmid, double lng, double lat, const OSMPBF::Tags &tags) {
    // Check if it is in the list of nodes used by ways
    if (!shape_.IsUsed(osmid)) {
      return;
    }

    ++osmdata_.osm_node_count;

    osmdata_.shape_map.emplace(osmid, PointLL(lng,lat));

    if (osmdata_.shape_map.size() % 500000 == 0) {
      LOG_INFO("Processed " + std::to_string(osmdata_.shape_map.size()) + " nodes on ways");
    }
  }

  void way_callback(uint64_t osmid, const OSMPBF::Tags &tags, const std::vector<uint64_t> &nodes) {

    // Check if it is in the list of ways used by relations
    if (!members_.IsUsed(osmid)) {
      return;
    }

    for (const auto node : nodes) {
      ++osmdata_.node_count;
      // Mark the nodes that we will care about when processing nodes
      shape_.set(node);
    }

    osmdata_.way_map.emplace(osmid,std::list<uint64_t>(nodes.begin(), nodes.end()));
  }

  void relation_callback(const uint64_t osmid, const OSMPBF::Tags &tags, const std::vector<OSMPBF::Member> &members) {
    // Get tags
    auto results = lua_.Transform(OSMType::kRelation, tags);
    if (results.size() == 0)
      return;

    OSMAdmin admin{osmid};

    for (const auto& tag : results) {

      if (tag.first == "name")
        admin.set_name(tag.second);
      else if (tag.first == "admin_level")
        admin.set_admin_level(std::stoi(tag.second));

    }

    std::list<uint64_t> member_ids;

    for (const auto& member : members) {

      if (member.member_type == OSMPBF::Relation::MemberType::Relation_MemberType_WAY) {
        members_.set(member.member_id);
        member_ids.push_back(member.member_id);
        ++osmdata_.osm_way_count;
      }
    }

    admin.set_ways(member_ids);

    osmdata_.admins_.push_back(std::move(admin));
  }

  // Lua Tag Transformation class
  LuaTagTransform lua_;

  // Mark the OSM Ids used by the ways and relations
  IdTable shape_, members_;

  // Pointer to all the OSM data (for use by callbacks)
  OSMData& osmdata_;

};

}

namespace valhalla {
namespace mjolnir {

OSMData PBFAdminParser::Parse(const boost::property_tree::ptree& pt, const std::vector<std::string>& input_files) {
  // Create OSM data. Set the member pointer so that the parsing callback
  // methods can use it.
  OSMData osmdata{"admin_ways.bn", "admin_way_node_ref.bn"};
  admin_callback callback(pt, osmdata);

  // Parse each input file for relations
  LOG_INFO("Parsing relations...")
  for (const auto& input_file : input_files) {
    OSMPBF::Parser::parse(input_file, OSMPBF::Interest::RELATIONS, callback);
  }
  LOG_INFO("Finished with " + std::to_string(osmdata.admins_.size()) + " admin polygons comprised of " + std::to_string(osmdata.osm_way_count) + " ways");


  // Parse the ways.
  LOG_INFO("Parsing ways...");
  for (const auto& input_file : input_files) {
    OSMPBF::Parser::parse(input_file, OSMPBF::Interest::WAYS, callback);
  }
  LOG_INFO("Finished with " + std::to_string(osmdata.way_map.size()) + " ways comprised of " + std::to_string(osmdata.node_count) + " nodes");

  // Parse node in all the input files. Skip any that are not marked from
  // being used in a way.
  LOG_INFO("Parsing nodes...");
  for (const auto& input_file : input_files) {
    OSMPBF::Parser::parse(input_file, OSMPBF::Interest::NODES, callback);
  }
  LOG_INFO("Finished with " + std::to_string(osmdata.osm_node_count) + " nodes");

  //done with pbf
  OSMPBF::Parser::free();

  // Return OSM data
  return osmdata;
}

}
}
