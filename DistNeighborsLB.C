/***
 * Author: vinicius.mctf@posgrad.ufsc.br (Vinicius Freitas)
 *  Neighborhood friendly loadbalancer
 **/

#include "DistNeighborsLB.h"
#include "elements.h"
#include "lbdb.h"
#include "ckgraph.h"

#include <string>

#define AFFINITY_ON 1
#define DIST_LBDBG_ON _lb_args.debug()
#define ub(x,y) (1+y)*x


CreateLBFunc_Def(DistNeighborsLB, "Locality based decentralized load balancer")

DistNeighborsLB::DistNeighborsLB(CkMigrateMessage *m) : CBase_DistNeighborsLB(m) {
}

DistNeighborsLB::DistNeighborsLB(const CkLBOptions &opt) : CBase_DistNeighborsLB(opt) {
  lbname = "DistNeighborsLB";
  if (CkMyPe() == 0)
    CkPrintf("[%d] DistNeighborsLB created\n", CkMyPe());
  InitLB(opt);
}

void DistNeighborsLB::turnOn()
{
#if CMK_LBDB_ON
  theLbdb->getLBDB()->
    TurnOnBarrierReceiver(receiver);
  theLbdb->getLBDB()->
    TurnOnNotifyMigrated(notifier);
  theLbdb->getLBDB()->
    TurnOnStartLBFn(startLbFnHdl);
#endif
}

void DistNeighborsLB::turnOff()
{
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
  kImbalanceFactor = 0.05;
  kMaxHops = -1; // only 1 hop allowed.

  // Initialize data sets
  stats->makeCommHash(); // Necessary in communication step
  my_stats = stats;
  my_load = 0.0;
  neighbor_priority = 0.0;
  expected_ans = -1;
  task_load = std::vector<double>();
  global_task_id = std::vector<LDObjid>();

  // Update local load informations
  int n_tasks = my_stats->n_objs;
  for (int i = 0; i < n_tasks; ++i) {
    auto obj_data = my_stats->objData[i];
    double obj_load = obj_data.wallTime;
    my_load += obj_load;

    if (obj_data.migratable && obj_load > 0.001) {
      InfoRecord * info = new InfoRecord {i, obj_load};
      // info->Id = i;
      // info->load = obj_load;
      tasks.push_back(info);
    }
  }

  DefineNeighborhood(); // Updates neighbors
  DefineNeighborhoodLoad(); // Updates expected_neighbor_load
  received_tasks.clear(); // = std::vector<std::pair<int,int> >(); // id, hop_count
  received_from.clear(); // = std::vector<int>();
  // remote_comm_tasks = DefineRemoteCommTasks();
  lb_started = false;
  ending = false;
}

void DistNeighborsLB::DefineNeighborhood() {
  // Neighbors are defined only once.
  if (defined_neighbors && !kRedefineNeighborhood) return;

  // Reset dataset
  neighbors = std::set<int>();

  // Neighborhood is defined based on task communication.
  LDCommData &comm_data = my_stats->commData;
  int max_iter = my_stats->n_comm;

  auto start_time = CmiWallTimer();
  // For every registered communication
  for (int i = 0; i < max_iter; ++i) {
    // Considering only communication between objects
    if (comm_data[i].recv_type() == LD_OBJ_MSG) {

      const LDCommDesc& to = comm_data[i].receiver;
      int comm_pe = to.destObjProc;

      // Case local communication
      if (my_pe == comm_pe) {
        // If the current PE is the receiver, we'll do nothing.
        continue;
      } else {
        if (DIST_LBDBG_ON > 1) {
          CkPrintf("[%d] Detected communication to: %d\n", my_pe, comm_pe);
        }
        // This is no local communication, so we'll add comm_pe to our neighborhood.
        neighbors.emplace(comm_pe, -1);

        #if AFFINITY_ON
        // Get CommHash to assign remote_comm_tasks
        int t_id = my_stats->getHash(comm_data[i].sender);
        if (DIST_LBDBG_ON > 1)
          CkPrintf("[%d] Communicating task is: %d", my_pe, t_id);
        remote_comm_tasks.emplace(t_id, comm_pe);
        #endif //AFFINITY
      }
    }
  }
  auto end_time = CmiWallTimer();
  auto time_to_define = end_time - start_time;

  // Debug information on defined neighborhoods.
  if (DIST_LBDBG_ON) {
    std::string db_neighbors();
    for (auto e : neighbors) {
      db_neighbors << e << ", ";
    }
    db_neighbors << std::endl;
    CkPrintf("[%d] Defined neighborhood: %s\n", my_pe, db_neighbors);
  }
  CkPrintf("[%d] Time elapsed: %lf", my_pe, time_to_define);
  defined_neighbors = true;
  return;
}

void DistNeighborsLB::DefineNeighborhoodLoad() {
  expected_ans = 0;
  for (n : neighbors) {
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
  neighbors[source].second = load;
  expected_ans--;

  // UpdateMinNeighbor(source, load);
  if (expected_ans == 0) {
    // Contribute for average load reduction
    CkCallback cb (CkReductionTarget(DistNeighborsLB, AvgLoadReduction), thisProxy);
    contribute(sizeof(double), &my_load, CkReduction::sum_double, cb);
  }
}

void DistNeighborsLB::ShareNeighborLoad(int source, double load) {
  // Not an expected ans
  if (!neighbors.count(source)) {
    thisProxy[source].ReturnLoad(int source, double load);
    neighbors.emplace(source, load);
  } else {
    expected_ans--;
    neighbors[source].second = load;
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
  for (n : neighbors) {
    avg_nbr_load += n.second;
  }
  avg_nbr_load /= neighbors.size();
  if (DIST_LBDBG_ON > 1) {
    CkPrintf("[%d] I had a total of %d missing neighbors.\n", )
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

int DistNeighborsLB::ChooseReceiver() {
  int rec;
  srand(CmiWallTimer()*neighbor_priority);

  // If the neighborhood has an OL factor higher than 5%
  // Migrate out.
  if (neighbor_priority > 1.05) {
    do {
      rec = rand()%CkNumPes();
    } while (neighbors.count(rec) || rec == my_pe);

  // If neighborhood has an UL factor higher than 5%
  // Migrate in.
  } else if (neighbor_priority < 0.95) {
    int it_count = rand()%neighbors.size();
    auto iterator = neighbors.begin();
    for (int i = 0; i > it_count; ++i) {
      iterator++;
    }
    rec = ((std::pair<int,double>)(*iterator)).first;

  // If near balance neighborhood, pick a PE at random (not self)
  // To increase convergence rate.
  } else {
    do {
      rec = rand()%CkNumPes();
    } while (rec == my_pe);
  }

  return rec;
}

std::pair<int, double> ChooseLeavingTask(int receiver) {
  std::pair<int, double> l_task;


  return l_task;
}

void DistNeighborsLB::LoadBalance() {
  // Start LB
  if (!lb_started) lb_started = true;

  // Determine if PE is overloaded.
  if (my_load > ub(avg_sys_load, kImbalanceFactor)) {
    while (my_load > ub(avg_sys_load, kImbalanceFactor)) {
      // Choose who to send.
      int receiver = ChooseReceiver();

      // Choose what to send. Removes task from data structures.
      std::pair<int, double> task = ChooseLeavingTask(receiver);

      // Commit migration.
      AddToMigrationMessage(task.first, task.second, receiver);

      // Push migration.
      thisProxy[receiver].IrradiateLoad(my_pe, task.second, 0);

      // Update local information.
      my_load -= task.second;
      total_migrates++;
      if (neighbors.count(receiver)) {
        neighbors[receiver].second += task.second;
      }
    }
  }
  // Contribute for final reduction
  if (!ending && !total_migrates) {
    ending = true;
    contribute(CkCallback(
      CkReductionTarget(DistNeighborsLB, DoneIrradiating), thisProxy)
    );
  }
}

void DistNeighborsLB::IrradiateLoad(int from_pe, int remote_id, double pe_load, double load, int hop_count) {
  // Update number of expected received migrations for base class (DistLB)
  migrations_expected++;

  // Not a neighbor
  if (!neighbors.count(from_pe)) {
    if (DIST_LBDBG_ON > 1) {
      CkPrintf("[%d] I am receiveing a remote migration!\n", my_pe);
    }
    remotes.emplace(from_pe, pe_load);
  } else {
    neighbors[from_pe].second = pe_load;
  }

  // If task is to be rejected, give it back
  if (my_load+load > ub(avg_sys_load, kImbalanceFactor)) {
    migrations_expected--;
    thisProxy[from_pe].DenyLoad(remote_id, load, true);
    LoadBalance();
  } else {
  // Accepted task
    received_from.push_back(from_pe);
    received_tasks.push_back(load);
    my_load += load;
    thisProxy[from_pe].DenyLoad(remote_id, load, false);
    LoadBalance();
  }
}

void DistNeighborsLB::DenyLoad(int task_id, double task_load, bool deny) {
  if (deny) {
    my_load += task_load;

  }
}

void DistNeighborsLB::DoneIrradiating() {

}


#include "DistNeighborsLB.def.h"
