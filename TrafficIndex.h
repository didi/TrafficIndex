#include <libpq-fe.h>
#include "mysql.h"
#include "LogUlits.h"
#include "hiredis.h"
#include <pthread.h>
#include "curl/curl.h"
#include "md5.h"
#include "traffic_pb.pb.h"
#include <map>
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "hiredis.h"
#include <cmath>
#include <signal.h>

/*
 高速路：00
 快速路：01
 主干路：02 03
 次干路：04
 支路：06 08 09 0a 0b
 */

using namespace std;

typedef struct{
    double dfSpeed;
    double dfLength;
    string strELinkID;
    string strBatchTime;
    time_t nBatchTime;
    unsigned int nConfidence;
}trafficInfo;

typedef struct{
    double dfTTIFenmu;
    double dfSpdFenzi;
}TTIFenzi;

struct LinkArray{
    long long* pLinkSet;
    long long llSize;
    
    LinkArray(){
        memset(this,0,sizeof(LinkArray));
    }
};

struct RealtimeTraffic{
    vector<trafficInfo>* pVecElement;
    pthread_mutex_t trafficDataMutex;
    
    RealtimeTraffic(){
        memset(this,0,sizeof(RealtimeTraffic));
    }
};

struct WeightAndFreeflow{
    double* pValues;
    pthread_mutex_t weightAndFreeflowMutex;
    
    WeightAndFreeflow(){
        memset(this,0,sizeof(WeightAndFreeflow));
    }
};
struct ThreadParameter{
    const char* pszCityCode;
    time_t nCurrentTime;
    const char* pszTrafficPublicURL;
    class TrafficIndex* pTrafficIndex;
    
    ThreadParameter(){
        memset(this,0,sizeof(ThreadParameter));
    }
};

class TrafficIndex{
public:
    TrafficIndex();
    ~TrafficIndex();
    
public:
    bool connectDatabase();
    
    MYSQL* getMySQLConn();
    
    PGconn* getPGConn();
    
    redisContext* getRedisConn();
    
    void setProvinceList(vector<string>& vecProvince);
    
    void setTrafficPublicURL(string& strTrafficPublicURL);
    
    string getTrafficPublicURL();
    
    void getProvinceList(vector<string>& vecProvince);
    
    void setMySQLInfo(DBInfo& mySQLInfo);
    
    void setPGInfo(DBInfo& pgInfo);
    
    void setRedisInfo(DBInfo& redisInfo);
    
    void setTrafficDataMutex(pthread_mutex_t& traffic_data_mutex);
    
    void set_tti_obj_mutex(pthread_mutex_t& tti_obj_mutex);
    
    void setUpdateWeightAndFreeflowMutex(pthread_mutex_t& update_weight_freeflow_mutex);
    
    bool isGetLock(int nObj_id,time_t nBatch_time);
    
    void computeTTI(time_t nBatch_time);
    
    void updateLink();
    
    void updateWeightAndFreeflow();
    
    void aggreTTT(int nType,time_t nCurrentTime);
    
    bool initJob();
    
    void add_tti_object(string& strObjID);
    
    void remove_tti_object(string& strJobID);
    
    void consumeRedis();
    
    static void *getProvinceRealtimeTrafficThread(void *pParam);
    
    static void deleteRealtimeCache(const leveldb::Slice& key, void* value);
    
    static void deleteFreeflowAndWeightCache(const leveldb::Slice& key, void* value);
    
    leveldb::Cache* getRealtimeTrafficDataCache();
    
    void getTTILinks();
private:
    long long* queryELinkID(PGconn* pConn,string& strWKT,string strFilter,long long& llSize);
        
    void computeLinkSetTTI(time_t nBatchTime,int nObj_id,LinkArray* pLinkSet,double& dfTTI,double& dfSpd);
    
    void releaseResource();
    
    void getFreeflowAndWeight(string& strELink,double& dfFreeflow,double& dfWeight);
    
    string getFeatureVersion(string strFeatureType,PGconn* pConn);
    
    void getWeightAndFreeflow();
private:
    MYSQL* m_pComputeMySQL;
    MYSQL* m_pUpdateLinkMySQL;
    MYSQL* m_pSubscribeMySQL;
    MYSQL* m_pAggreMySQL;
    MYSQL* m_pInitLinksMySQL;

    PGconn* m_pUpdateLinkPG;
    PGconn* m_pWeightAndFreeflowPG;
    PGconn* m_pSubscribePG;
    PGconn* m_pInitLinksPG;

    redisContext* m_pMQRedisConn;
    redisContext* m_pLockRedisConn;

    DBInfo m_mySQLInfo;
    DBInfo m_pgInfo;
    DBInfo m_redisInfo;
    vector<string> m_vecProvince;
    string m_strTrafficPublicURL;
    string m_strLinkVersion;
    string m_strWeightAndFreeflowVersion;
    
    pthread_mutex_t m_tti_obj_mutex;
    pthread_mutex_t m_UpdatWeightAndFreeflowMutex;
    map<int,LinkArray*> m_TTIList;

    leveldb::Cache* m_pFreeflowAndWeightCache;
    
public:
    pthread_mutex_t m_TrafficDataMutex;
    leveldb::Cache* m_pRealtimeTrafficDataCache;
};
