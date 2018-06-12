/***
 * Author: vinicius.mctf@posgrad.ufsc.br (Vinicius Freitas)
 *  Neighborhood friendly loadbalancer
 **/

#include "DistNeighborsLB.h"
#include "elements.h"
#include "DistBaseLB.h"
#include "BaseLB.h"
#include "ckgraph.h"

#include <sstream>
#include <string>

#define AFFINITY_ON 1
#define DIST_LBDBG_ON _lb_args.debug()
#define ub(x,y) (1+y)*x

namespace util_math {
  const static unsigned int doublingPrimes[] = {
    3,
    7,
    17,
    37,
    73,
    157,
    307,
    617,
    1217,
    2417,
    4817,
    9677,
    20117,
    40177,
    80177,
    160117,
    320107,
    640007,
    1280107,
    2560171,
    5120117,
    10000079,
    20000077,
    40000217,
    80000111,
    160000177,
    320000171,
    640000171,
    1280000017,
    2560000217u,
    4200000071u
/* extra primes larger than an unsigned 32-bit integer:
51200000077,
100000000171,
200000000171,
400000000171,
800000000117,
1600000000021,
3200000000051,
6400000000081,
12800000000003,
25600000000021,
51200000000077,
100000000000067,
200000000000027,
400000000000063,
800000000000017,
1600000000000007,
3200000000000059,
6400000000000007,
12800000000000009,
25600000000000003,
51200000000000023,
100000000000000003,
200000000000000003,
400000000000000013,
800000000000000119,
1600000000000000031,
3200000000000000059 //This is a 62-bit number
*/
};

static inline int i_abs(int c) { return c>0?c:-c; }

//This routine returns an arbitrary prime larger than x
static unsigned int primeLargerThan(unsigned int x) {
	 int i=0;
	 while (doublingPrimes[i]<=x) i++;
	 return doublingPrimes[i];
  }
}

inline static int ObjKey(const LDObjid &oid, const int hashSize) {
  // make sure all positive
  return (((util_math::i_abs(oid.id[2]) & 0x7F)<<24)
	 |((util_math::i_abs(oid.id[1]) & 0xFF)<<16)
	 |util_math::i_abs(oid.id[0])) % hashSize;
}

CreateLBFunc_Def(DistNeighborsLB, "Locality based decentralized load balancer")

DistNeighborsLB::DistNeighborsLB(CkMigrateMessage *m) : CBase_DistNeighborsLB(m) {
}

DistNeighborsLB::DistNeighborsLB(const CkLBOptions &opt) : CBase_DistNeighborsLB(opt) {
  lbname = "DistNeighborsLB";
  if (CkMyPe() == 0)
    CkPrintf("[%d] DistNeighborsLB created\n", CkMyPe());
  InitLB(opt);
}

void DistNeighborsLB::turnOn() {
#if CMK_LBDB_ON
  theLbdb->getLBDB()->
    TurnOnBarrierReceiver(receiver);
  theLbdb->getLBDB()->
    TurnOnNotifyMigrated(notifier);
  theLbdb->getLBDB()->
    TurnOnStartLBFn(startLbFnHdl);
#endif
}

void DistNeighborsLB::turnOff() {
#if CMK_LBDB_ON
  theLbdb->getLBDB()->
    TurnOffBarrierReceiver(receiver);
  theLbdb->getLBDB()->
    TurnOffNotifyMigrated(notifier);
  theLbdb->getLBDB()->
    TurnOffStartLBFn(startLbFnHdl);
#endif
}

void DistNeighborsLB::InitLB(const CkLBOptions &opt) {
  thisProxy = CProxy_DistNeighborsLB(thisgroup);
  if (opt.getSeqNo() > 0) turnOff();
}

void DistNeighborsLB::Strategy(const DistBaseLB::LDStats* const stats) {
  my_pe = CkMyPe();
  if (my_pe == 0 && _lb_args.debug() > 0)
    CkPrintf("[%d] In DistNeighborsLB strategy\n", my_pe);

  // Initialize constants
  kRedefineNeighborhood = false;
  kDynamicNeighborhood = true;
  kImbalanceFactor = 0.05;
  kMaxHops = -1; // only 1 hop allowed.

  // Initialize topology // LAPESD-TESLA
  topo = archtopo::comm_topo(1, 2, 10, 2);

  // Initialize local lb data
  my_load = 0.0;
  neighbor_priority = 0.0;
  expected_ans = -1;
  total_migrates = 0;
  mig_acks = 0;
  lb_started = false;
  ending = false;

  my_stats = stats;

  hash_size = 0;

  // MakeCommHash(my_stats); // Necessary in communication step

  // Update local load informations
  int n_tasks = my_stats->n_objs;
  tasks.clear();
  for (int i = 0; i < n_tasks; ++i) {
    auto obj_data = my_stats->objData[i];
    double obj_load = obj_data.wallTime;
    my_load += obj_load;

    if (obj_data.migratable && obj_load > 0.001) {
      tasks.emplace(i, obj_load);
    }
  }

  received_tasks.clear(); // = std::vector<std::pair<int,int> >(); // id, hop_count
  received_from.clear(); // = std::vector<int>();
  sent_tasks.clear();
  DefineNeighborhood(); // Updates neighbors
  DefineNeighborhoodLoad(); // Updates expected_neighbor_load
  // remote_comm_tasks = DefineRemoteCommTasks(); // Determined by DefineNeighborhood

}

// Create Charm_ID Hash table, as seen in BaseLB
void DistNeighborsLB::MakeCommHash() {
  // hash table is already build
  if (obj_hash) return;

  int i;
  hash_size = my_stats->n_objs*2;
  hash_size = util_math::primeLargerThan(hash_size);
  obj_hash = new int[hash_size];
  for (i = 0; i < hash_size; i++) {
    obj_hash[i] = -1;
  }

  for (i = 0; i < my_stats->n_objs; i++) {
    const LDObjid &oid = my_stats->objData[i].objID();
    int hash = ObjKey(oid, hash_size);
    CmiAssert(hash != -1);

    while(obj_hash[hash] != -1)
      hash = (hash+1)%hash_size;

    obj_hash[hash] = i;
  }
}

void DistNeighborsLB::DeleteCommHash() {
  if (obj_hash) delete [] obj_hash;

  obj_hash = NULL;
  for(int i=0; i < my_stats->n_comm; i++) {
    my_stats->commData[i].clearHash();
  }
}

int DistNeighborsLB::GetObjHash(const LDObjid &oid, const LDOMid &mid) {
  #if CMK_LBDB_ON
    CmiAssert(hash_size > 0);
    int hash = ObjKey(oid, hash_size);

    for (int id = 0; id < hash_size; id++) {
      int index = (id+hash)%hash_size;
	     if (index == -1 || obj_hash[index] == -1) return -1;

      if (LDObjIDEqual(my_stats->objData[obj_hash[index]].objID(), oid) &&
            LDOMidEqual(my_stats->objData[obj_hash[index]].omID(), mid))
            return obj_hash[index];
    }
    //  CkPrintf("not found \n");
  #endif
    return -1;
}

int DistNeighborsLB::GetObjHash(const LDObjKey &key) {
  const LDObjid &oid = key.objID();
  const LDOMid  &mid = key.omID();
  return GetObjHash(oid, mid);
}

int DistNeighborsLB::GetSendHash(const LDCommData &c_data) {
  return GetObjHash(c_data.sender);
}

int DistNeighborsLB::GetRecvHash(const LDCommData &c_data) {
  return GetObjHash(c_data.receiver.get_destObj());
}

void DistNeighborsLB::DefineNeighborhood() {
  // Neighbors are defined only once.
  if (defined_neighbors && !kRedefineNeighborhood) return;

  // Reset dataset
  neighbors.clear();

  if (kDynamicNeighborhood) {
    // Define obj_hash, a list of all objects associated to their CharmIDs
    MakeCommHash();

    // Neighborhood is defined based on task communication.
    LDCommData* comm_data = (my_stats->commData);
    int max_iter = my_stats->n_comm;

    auto start_time = CmiWallTimer();
    // For every registered communication
    for (int i = 0; i < max_iter; ++i) {
      // Considering only communication between objects
      if (comm_data[i].recv_type() == LD_OBJ_MSG) {

        const LDCommDesc& to = comm_data[i].receiver;
        int comm_pe = to.dest.destObj.destObjProc;

        // Case local communication
        if (my_pe == comm_pe) {
          // If the current PE is the receiver, we'll do nothing.
          continue;
        } else {
          if (DIST_LBDBG_ON > 4) {
            CkPrintf("[%d] Detected communication to: %d\n", my_pe, comm_pe);
          }
          // This is no local communication, so we'll add comm_pe to our neighborhood.
          neighbors.emplace(comm_pe, 0);

          #if AFFINITY_ON
          // Get CommHash to assign remote_comm_tasks
          int t_id = GetObjHash(comm_data[i].sender);
          if (DIST_LBDBG_ON > 4)
            CkPrintf("[%d] Communicating task is: %d\n", my_pe, t_id);
          remote_comm_tasks.emplace(t_id, comm_pe);
          #endif //AFFINITY
        }
      }
    }
    auto end_time = CmiWallTimer();
    auto time_to_define = end_time - start_time;

    // Debug information on defined neighborhoods.
    if (DIST_LBDBG_ON > 2) {
      for (auto e : neighbors) {
        CkPrintf("[%d] Chosen neighbor: %d\n", my_pe, e.first);
      }
    }
    CkPrintf("[%d] Time elapsed: %lf\n", my_pe, time_to_define);
  } else {
    // Use topo if cannot dynamically define neighborhood
    auto tmp = topo.neighbors(my_pe, 2);
    for (auto e : tmp) {
      neighbors.emplace(e, 0);
    }
  }

  defined_neighbors = true;
  return;
}

void DistNeighborsLB::DefineNeighborhoodLoad() {
  expected_ans = 0;
  for (auto n : neighbors) {
    if (DIST_LBDBG_ON > 2) {
      CkPrintf("[%d] Sharing load (%lf) with neighbor %d\n", my_pe, my_load, n.first);
    }
    thisProxy[n.first].ShareNeighborLoad(my_pe, my_load);
    expected_ans++;
  }
}

void DistNeighborsLB::UpdateMinNeighbor(int source, double load) {
  // Update prime target for migration
  if (load < min_neighbor_load.second) {
    min_neighbor_load.first = source;
    min_neighbor_load.second = load;
  }
}

void DistNeighborsLB::ReturnLoad(int source, double load) {
  neighbors.emplace(source, load);
  expected_ans--;

  // UpdateMinNeighbor(source, load);
  if (expected_ans == 0) {
    // Contribute for average load reduction
    CkCallback cb (CkReductionTarget(DistNeighborsLB, AvgLoadReduction), thisProxy);
    contribute(sizeof(double), &my_load, CkReduction::sum_double, cb);
  }
}

void DistNeighborsLB::ShareNeighborLoad(int source, double load) {
  if (DIST_LBDBG_ON > 2) {
    CkPrintf("[%d] Receiving load from neighbor %d\n", my_pe, source);
  }
  // Not an expected ans
  if (!neighbors.count(source)) {
    // Share load, since we've not shared beforehand
    thisProxy[source].ReturnLoad(source, load);
    // Add to neighbors
    neighbors.emplace(source, load);
  } else {
    // Decrease #expected responses
    expected_ans--;
    // Update neighbor load
    neighbors[source] = load;
  }

  // UpdateMinNeighbor(source, load);
  if (expected_ans == 0) {
    // Contribute for average load reduction
    CkCallback cb (CkReductionTarget(DistNeighborsLB, AvgLoadReduction), thisProxy);
    contribute(sizeof(double), &my_load, CkReduction::sum_double, cb);
  }
}

void DistNeighborsLB::AvgLoadReduction(double x) {
  if (DIST_LBDBG_ON > 1) {
    if (my_pe == 0) CkPrintf("Average Load Reduction\n");
  }
  avg_sys_load = x/CkNumPes();

  double avg_nbr_load = 0.0;
  int negack = 0;
  if (DIST_LBDBG_ON > 2) {
    CkPrintf("[%d] I have found %d neighbors\n", my_pe, neighbors.size());
  }
  for (auto n : neighbors) {
    if (DIST_LBDBG_ON > 1)
      CkPrintf("[%d] Load is (%d = %lf)\n", my_pe, n.first, n.second);
    avg_nbr_load += n.second;
  }
  avg_nbr_load /= neighbors.size();
  if (DIST_LBDBG_ON > 1) {
      CkPrintf("[%d] My average neighbor load was %lf\n", my_pe, avg_nbr_load);
  }

  // > 1: Do not make neighborhood migration
  // ~= 1: Even distribution
  // < 1: Prioritize neighborhood migration
  neighbor_priority = avg_nbr_load / avg_sys_load;

  GlobalReschedule();
}

void DistNeighborsLB::GlobalReschedule() {
  LoadBalance();
}

std::pair<int, bool> DistNeighborsLB::ChooseReceiver() {
  int rec;
  bool remote;
  srand(CmiWallTimer()*neighbor_priority);

  if(DIST_LBDBG_ON > 2) {
    CkPrintf("[%d] Choosing receiver, current neighborhood OL factor is: %lf\n",
      my_pe, neighbor_priority);
  }

  // If the neighborhood has an OL factor higher than 5%
  // Migrate out.
  if (neighbor_priority > 1.05) {
    do {
      rec = rand()%CkNumPes();
    } while (neighbors.count(rec) || rec == my_pe);
    remote = true;

  // If neighborhood has an UL factor higher than 5%
  // Migrate in.
  } else if (neighbor_priority < 0.95) {
    if (neighbors.count(my_pe))
      neighbors.erase(my_pe);
    int it_count = rand()%neighbors.size();

    auto iterator = neighbors.begin();
    for (int i = 0; i > it_count; ++i) {
      iterator++;
    }
    rec = ((std::pair<int,double>)(*iterator)).first;
    remote = false;

  // If near balance neighborhood, pick a PE at random (not self)
  // To increase convergence rate.
  } else {
    do {
      rec = rand()%CkNumPes();
    } while (rec == my_pe);
    // Assume remote comm, for cheaper task choosing step:
    remote = true;
  }

  if (DIST_LBDBG_ON > 2) {
    CkPrintf("[%d] Chosen receiver was: %d\n", my_pe, rec);
  }

  return {rec, remote};
}

std::pair<int, double> DistNeighborsLB::ChooseLeavingTask(int receiver, bool remote_comm) {
  std::pair<int, double> l_task;
  bool choosing = true;
  srand(CmiWallTimer()*receiver);

  // Two behaviors:
  // 1: There are remote_comm_tasks:
  // If !remote_comm, there are no tasks to choose in remote_comm_tasks
  if (remote_comm_tasks.size() && !remote_comm) {
    auto iterator = remote_comm_tasks.begin();
    while (choosing && iterator != remote_comm_tasks.end()) {
      // If this remote_comm_task communicates with the chosen receiver,
      if ((*iterator).second == receiver && !sent_tasks.count((*iterator).first)) {
        int position = (*iterator).first;
        l_task = {position, tasks.at(position)}; // Not working, bro
        sent_tasks.emplace(position, receiver);
        tasks.erase(position);
        remote_comm_tasks.erase(iterator);

        if (DIST_LBDBG_ON > 2) {
          CkPrintf("[%d] Choosing and erasing task %d\n", my_pe, position);
        }
        choosing = false;
      } else {
        iterator++;
      }
    }
  }
  // 2: There are no remote_comm_tasks that communicate with receiver:
  if (choosing) {
    int position = rand()%tasks.size();
    while (sent_tasks.count(position)) {
      position = rand()%tasks.size();
    }
    auto it = tasks.begin();
    for (int i = 0; i < position; ++i) {
      it++;
    }
    l_task = {(*it).first, (*it).second};
    if (remote_comm_tasks.count(position)) {
      remote_comm_tasks.erase(position);
    }
    sent_tasks.emplace(position, receiver);
    tasks.erase(position);
    if (DIST_LBDBG_ON > 2) {
      CkPrintf("[%d] Choosing and erasing task %d\n", my_pe, position);
    }
    choosing = false;
  }

  return l_task;
}

void DistNeighborsLB::SendLoad(int hops) {
  // Determine if PE is overloaded.
  while (my_load > ub(avg_sys_load, kImbalanceFactor)) {
    // Choose who to send.
    std::pair<int,bool> receiver = ChooseReceiver();

    // Choose what to send. Removes task from data structures.
    std::pair<int, double> task = ChooseLeavingTask(receiver.first, receiver.second);

    // Commit migration.
    // AddToMigrationMessage(task.first, task.second, receiver);
    // In this implementation, migrations are being confirmed before commitment

    // Push migration.
    thisProxy[receiver.first].IrradiateLoad(my_pe, task.first, my_load, task.second, hops);

    // Update local information.
    my_load -= task.second;
    total_migrates++;
    if (neighbors.count(receiver.first)) {
      neighbors[receiver.first] += task.second;
    }
  }
}

void DistNeighborsLB::LoadBalance() {
  // Start LB
  if (!lb_started) lb_started = true;

  SendLoad(0);

  // Contribute for final reduction
  // 1. Case underloaded PE.
  // 2. Case overloaded done receiving confirmations.
  if (!ending && (!total_migrates || mig_acks == total_migrates)) {
    ending = true;
    contribute(CkCallback(
      CkReductionTarget(DistNeighborsLB, DoneIrradiating), thisProxy)
    );
  } else {
    if (DIST_LBDBG_ON > 2) {
      CkPrintf("[%d] Still waiting for %d to finish\n", my_pe, total_migrates-mig_acks);
    }
  }
}

void DistNeighborsLB::IrradiateLoad(int from_pe, int remote_id, double pe_load, double load, int hop_count) {
  // Update number of expected received migrations for base class (DistLB)

  // Not a neighbor
  if (!neighbors.count(from_pe)) {
    if (DIST_LBDBG_ON > 1) {
      CkPrintf("[%d] I am receiveing a remote migration: (%d -> %lf)\n", my_pe, from_pe, pe_load);
    }
    remotes.emplace(from_pe, pe_load);
  } else {
    neighbors[from_pe] = pe_load;
  }

  // If task is to be rejected, give it back
  if (my_load+load > ub(avg_sys_load, kImbalanceFactor) && hop_count < 1) {
    if (DIST_LBDBG_ON > 1) {
      CkPrintf("[%d] I am denying a task: (%d -> %lf)\n", my_pe, remote_id, load);
    }
    thisProxy[from_pe].DenyLoad(remote_id, load, my_pe, my_load, true);
    LoadBalance();
  } else {
  // Accepted task
    migrates_expected++;
    if (DIST_LBDBG_ON > 1) {
      CkPrintf("[%d] I am receiveing a task: (%d -> %lf)\n", my_pe, remote_id, load);
    }
    received_from.push_back(from_pe);
    received_tasks.push_back({remote_id, load});
    // my_load += load;
    thisProxy[from_pe].DenyLoad(remote_id, load, my_pe, my_load, false);
    LoadBalance();
  }
}

// Updates migration message (migrateInfo) with task_id.
void DistNeighborsLB::AddToMigrationMessage(int task_id, double task_load, int pe_id) {
  MigrateInfo* migrating = new MigrateInfo;
  migrating->obj = my_stats->objData[task_id].handle;
  migrating->from_pe = my_pe;
  migrating->to_pe = pe_id;
  migrateInfo.push_back(migrating);
}

void DistNeighborsLB::DenyLoad(int task_id, double task_load, int remote_id, double remote_load, bool deny) {
  // Update Remote node information
  if (neighbors.count(remote_id)) {
    neighbors[remote_id] = remote_load;
  } else {
    remotes.emplace(remote_id, remote_load);
  }

  // Reinsert denied task in data structures.
  if (deny) {
    // Update local information.
    my_load += task_load;
    if (DIST_LBDBG_ON > 2)
      CkPrintf("[%d] Task of ID %d has been denied\n", my_pe, task_id);
    // tasks.emplace(task_id, task_load);
    total_migrates--;
    DesperateLoadBalance();

  } else {
    // If not denied, add migration message.
    if (DIST_LBDBG_ON > 2)
      CkPrintf("[%d] Task of ID %d has been confirmed\n", my_pe, task_id);

    AddToMigrationMessage(task_id, task_load, remote_id);
    mig_acks++;
  }

  // Go back to load balance decision.
  LoadBalance();
}

void DistNeighborsLB::DesperateLoadBalance() {
  SendLoad(1);
  LoadBalance();
}

void DistNeighborsLB::DoneIrradiating() {
  // Create Final migration message.
  msg = new(total_migrates, CkNumPes(), CkNumPes(), 0) LBMigrateMsg;
  msg->n_moves = total_migrates;
  for (int i = 0; i < total_migrates; ++i) {
    MigrateInfo* item = (MigrateInfo*) migrateInfo[i];
    msg->moves[i] = *item;
    delete item;
    migrateInfo[i] = 0;
  }
  migrateInfo.clear();

  // Synchronize to start actual migrations.
  contribute(CkCallback(
    CkReductionTarget(DistNeighborsLB, MigrateAfterBarrier), thisProxy));
}

void DistNeighborsLB::MigrateAfterBarrier() {
  ProcessMigrationDecision(msg);
}

#include "DistNeighborsLB.def.h"
