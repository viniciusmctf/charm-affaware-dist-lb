#ifndef DIST_NEIGH_LB
#define DIST_NEIGH_LB

#include "DistBaseLB.h"
#include "DistNeighborsLB.decl.h"
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
   void ShareNeighborLoad(int source, double load);
   void ReturnLoad(int source, double load);
   void AvgLoadReduction(double x);
   void IrradiateLoad(int from_pe, int remote_id, double pe_load, double load, int hop_count);
   void DoneIrradiating();
   void MigrateAfterBarrier();

 protected:
   CProxy_DistNeighborsLB thisProxy;

   // Constants
   bool kRedefineNeighborhood;
   double kImbalanceFactor;
   double kCommunicationFactor;

   // Local Variables
   bool lb_started; // Used by base class to finish LB.
   bool defined_neighbors; // Determines if neighborhood has been defined.
   bool ending; // Determines if chare has already commited for LB end.
   int my_pe; // CkMyPe().
   int expected_ans; // Number of expected neighbor answers.
   int total_migrates; // Total migration attempts.
   int mig_acks; // Total migration responses.
   double my_load;
   double avg_sys_load;
   double neighbor_priority;

   std::map<int,double> neighbors;
   std::map<int,double> remotes;
   std::vector<std::pair<int,int> > received_tasks;
   std::vector<int> received_from;
   std::map<int,int> remote_comm_tasks; // Prime targets for migration (task,pe)
   std::pair<int,double> min_neighbor_load;
   std::unordered_map<int, double> tasks; // id, load

   const DistBaseLB::LDStats* my_stats;
   std::vector<MigrateInfo*> migrateInfo;
   LBMigrateMsg* msg;

   std::pair<int, bool> ChooseReceiver(); // Chosen receiver, remote stat 
   std::pair<int,double> ChooseLeavingTask(int rec, bool remote_comm);

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
