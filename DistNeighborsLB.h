#ifndef DIST_NEIGH_LB
#define DIST_NEIGH_LB

#include "DistBaseLB.h"
#include "DistNeighborsLB.decl.h"
#include "archtopo.h"
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
   void DenyLoad(int, double, int, double, bool);
   void DoneIrradiating();
   void MigrateAfterBarrier();

 protected:
   CProxy_DistNeighborsLB thisProxy;

   // Constants
   int kMaxHops;
   bool kRedefineNeighborhood;
   bool kDynamicNeighborhood;
   double kImbalanceFactor;
   double kCommunicationFactor;

   // Topology
   archtopo::comm_topo topo;

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

   // Emulate BaseLB::LDStats::*Hash functions
   int* obj_hash;
   int hash_size;

   std::map<int,double> neighbors;
   std::map<int,double> remotes;
   std::vector<std::pair<int,double> > received_tasks; // Id, load
   std::vector<int> received_from;
   std::map<int,int> remote_comm_tasks; // Prime targets for migration (task,pe)
   std::pair<int,double> min_neighbor_load;
   std::map<int, double> tasks; // id, load
   std::unordered_map<int,int> sent_tasks;

   const DistBaseLB::LDStats* my_stats;
   std::vector<MigrateInfo*> migrateInfo;
   LBMigrateMsg* msg;

   std::pair<int, bool> ChooseReceiver(); // Chosen receiver, remote stat
   std::pair<int,double> ChooseLeavingTask(int rec, bool remote_comm);

   void InitLB(const CkLBOptions &);
   void DefineNeighborhood();
   void DefineNeighborhoodLoad();
   void MakeCommHash();
   void DeleteCommHash();
   int GetObjHash(const LDObjid &oid, const LDOMid &mid);
   int GetObjHash(const LDObjKey &key);
   int GetSendHash(const LDCommData &c_data);
   int GetRecvHash(const LDCommData &c_data);
   void DefineRemoteCommTasks();
   void UpdateMinNeighbor(int source, double load);
   void AddToMigrationMessage(int, double, int);
   void LocalRemote(int i, const LDObjid& from, const LDObjid& to);
   void GlobalReschedule();
   void LoadBalance();
   void SendLoad(int hop);
   void DesperateLoadBalance();
   void Strategy(const DistBaseLB::LDStats* const stats);
   bool QueryBalanceNow(int step) { return true; };
};

#endif //DIST_NEIGH_LB
