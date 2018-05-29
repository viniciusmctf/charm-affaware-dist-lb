

#ifndef DIST_NEIGH_LB
#define DIST_NEIGH_LB

#include "DistBaseLB.h"
#include "DistNeighborsLB.decl.h"
#include ""
#include <vector>
#include <set>
#include <map>
#include <unordered_map>

void CreateDistNeighborsLB();

class DistNeighborsLB : public CBase_DistNeighborsLB {
 public:
   DistNeighborsLB(const CkLBOptions &);
   DistNeighborsLB(CkMigrateMessage *m);
   void turnOff();
   void turnOn();
   void CommunicateNeighborhood(int hwloc_id, int charm_id);
   void ShareNeighborLoad(int source, double load);
   void ReturnLoad(int source, double load);
   void AvgLoadReduction(double x);
   void IrradiateLoad(int from_pe, int remote_id, double pe_load, double load, int hop_count);
   void DoneIrradiating();

 protected:
   CProxy_DistNeighborsLB thisProxy;

   // Constants
   bool kRedefineNeighborhood;
   double kImbalanceFactor;
   double kCommunicationFactor;

   bool lb_started;
   bool defined_neighbors;
   bool ending;
   int my_pe;
   int total_migrates;
   // Use migrates_expected to count # of receiving tasks
   std::map<int,double> neighbors;
   std::map<int,double> remotes;
   std::vector<std::pair<int,int> > received_tasks;
   std::vector<int> received_from;
   std::map<int,int> remote_comm_tasks; // Prime targets for migration (task,pe)
   double my_load;
   double avg_sys_load;
   double neighbor_priority;
   int expected_ans;
   std::pair<int,double> min_neighbor_load;
   std::vector<InfoRecord*> tasks;

   const DistBaseLB::LDStats* my_stats;
   std::vector<MigrateInfo*> migrateInfo;
   LBMigrateMsg* msg;

   int ChooseReceiver();
   std::pair<int,double> ChooseLeavingTask(int rec);

   void InitLB(const CkLBOptions &);
   void DefineNeighborhood();
   void DefineRemoteCommTasks();
   void UpdateMinNeighbor(int source, double load);
   void LocalRemote(int i, const LDObjid& from, const LDObjid& to);
   void LoadBalance();
   void GlobalReschedule();
   void Strategy(const DistBaseLB::LDStats* const stats);
   bool QueryBalanceNow(int step) { return true; };
};

#endif //DIST_NEIGH_LB
