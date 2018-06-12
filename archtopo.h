#include <vector>
#include <array>

#ifndef LB_DEBUG_ARGS
#define LB_DEBUG_ARGS 1
#endif

namespace archtopo {
  // All values are absolute (e.g., total nodes, cores etc)
  typedef struct comm_topo {
    std::vector<int> info;

    comm_topo() {};
    comm_topo(int, int, int, int);
    std::vector<int> neighbors(int, int);
  } comm_topo;

  // This constructor enforces homogeneous topologies
  comm_topo::comm_topo(int total_nodes, int nnpern, int cpernn, int pperc) {
    int nodes = total_nodes;
    int numanodes = total_nodes*nnpern;
    int cores = numanodes*cpernn;
    int pus = cores*pperc;

    info = {total_nodes, numanodes, cores, pus};
  };

  // Determine neighbors
  // Topo level = 1: Node level Neighbors
  // Topo level = 2: NumaNode level Neighbors
  std::vector<int> comm_topo::neighbors(int pe, int topo_level) {
    // Max topology depth = 2 (NumaNode neighbors)
    if (topo_level > 2) {
      return {};
    }

    int total_neighbors = info.back()/info.at(topo_level-1);
    std::vector<int> neighbors;
    neighbors.reserve(total_neighbors);

    std::array<int, 2> coords;
    int x = pe;
    // Look for the neighbors in an specific topo level
    for (int i = 0; i < topo_level; i++) {
      coords[i] = x/(info.back()/info.at(i)); // Determine next path in topology
      x = x % (info.back()/info.at(i)); // Update X in local path
    }

    // Prime candidate for loop unrolling
    for (int i = 0; i < x-1; ++i) {
      neighbors.push_back(pe-x+i);
    }
    // Skip self
    for (int i = x; i < total_neighbors; ++i) {
      neighbors.push_back(pe-x+i);
    }

    if (LB_DEBUG_ARGS) {
      CkPrintf("[%d] Defined neighborhood with %d neighbors.\n", CkMyPe(), total_neighbors);
    }
    return neighbors;
  };
}
