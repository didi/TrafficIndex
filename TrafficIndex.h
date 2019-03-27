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

typedef struct
{
    double dfSpeed;
    double dfLength;
    string strELinkID;
    string strBatchTime;
    time_t nBatchTime;
}trafficInfo;

typedef struct
{
    double dfTTIFenmu;
    double dfSpdFenzi;
}TTIFenzi;


class TrafficIndex
{
public:
    TrafficIndex();
    ~TrafficIndex();
    
public:
    bool connectDatabase();
    
    MYSQL* getMySQLConn();
    
    PGconn* getPGConn();
    
    redisContext* getRedisConn();
    
    int httpGet(string & strURL, std::string & strResponse,int nTimeout ,const char * pCaPath = NULL);
    
    void getTrafficDataFromServer(const char* pszCityCode,time_t nCurrentTime);
    
    void setProvinceList(vector<string>& vecProvince);
    
    void setTrafficPublicURL(string& strTrafficPublicURL);
    
    void getProvinceList(vector<string>& vecProvince);
    
    void setMySQLInfo(DBInfo& mySQLInfo);
    
    void setPGInfo(DBInfo& pgInfo);
    
    void setRedisInfo(DBInfo& redisInfo);
    
    void setLogUlits(LogUlits* pLogUlits);
    
    LogUlits* getLogUlits();
    
    void setTrafficDataMutex(pthread_mutex_t& traffic_data_mutex);
    
    void setPredefineObjMutex(pthread_mutex_t& predefine_obj_mutex);
    
    void setUpdateWeightAndFreeflowMutex(pthread_mutex_t& update_weight_freeflow_mutex);
    
    void setPublishURLMutex(pthread_mutex_t& pulish_url_mutex);
    
    void setCustomObjMutex(pthread_mutex_t& custom_obj_mutex);
    
    void computeTTI(string strBatch_time,int nType);
    
    void updateLink();
    
    void updateWeightAndFreeflow();
    
    void aggreTTT(int nType,time_t nCurrentTime);
    
    bool initJob();
    
    void add_tti_object(string& strObjID);
    
    void remove_tti_object(string& strJobID);
    
    void setNodeType(string strNodeType);
    
    void setHostName(string& strHostName);
    
    void consumeRedis();
private:
    void queryELinkIDs(PGconn* pConn,string &strWKB,vector<string>& vecELinkIDs,int nObjType);
    
    void queryELinkID(PGconn* pConn,string strWKT,vector<string>& vecELinkIDs,string strFilter);
        
    void computeLinkSetTTI(time_t nBatchTime,int nObj_id,vector<string>& vecLinkSet,double& dfTTI,double& dfSpd);
    
    void releaseResource();
    
    void getFreeflowAndWeight(string& strELink,double& dfFreeflow,double& dfWeight);
    
    void convertToELinkIDs(map<string,string>& mapDirection, string strLinkID, vector<string> &vecELinkIDs);
    
    void getTTILinks();
    
    int getConfigNodeCount(MYSQL* pMySQL);
    
    bool jobIsRunning(redisContext* pRedisConn,int nObj_ID);
    
    void getNodeList(redisContext* pRedisConn,vector<string>& vecNodeList);
    
    void getObjCountPerNode(redisContext* pRedisConn,vector<string>& vecNodeList,map<string,int>& mapObjCountPerNode);
    
    bool resetObjStatus();
    
    bool isSmallestNode(redisContext* pRedisConn,int& nCurrentNodeCount);
    
    string getFeatureVersion(string strFeatureType,PGconn* pConn);
    
    void getWeightAndFreeflow();
    
    int objectCountPerNode(MYSQL* pMySQL);
    
    int getTTIObjectCount(MYSQL* pMySQL);
    
    void computeObjTTI(MYSQL* pMySQL, string strBatch_time,map<int,vector<string> >& mapObjList,int nType);
    
private:
    MYSQL* m_pUpdateLinkMySQL;
    MYSQL* m_pComputePredefineMySQL;
    MYSQL* m_pComputeCustomMySQL;
    MYSQL* m_pSubscribeMySQL;
    
    PGconn* m_pPGConnSyncPredefine;
    PGconn* m_pUpdateLinkPG;
    PGconn* m_pPQWeightAndFreeflow;
    PGconn* m_pSubscribePG;
    
    redisContext* m_pRedisConn;
    redisContext* m_pMQRedisConn;
    
    LogUlits* m_pLogUlits;
    DBInfo m_mySQLInfo;
    DBInfo m_pgInfo;
    DBInfo m_redisInfo;
    vector<string> m_vecProvince;
    string m_strTrafficPublicURL;
    pthread_mutex_t m_traffic_data_mutex;
    pthread_mutex_t m_predefine_obj_mutex;
    pthread_mutex_t m_update_weight_freeflow_mutex;
    pthread_mutex_t m_pulish_url_mutex;
    map<string,vector<trafficInfo> >m_mapTrafficData;
    map<int,vector<string> >m_mapPredefineObjList;
    string m_strLinkVersion;
    string m_strWeightAndFreeflowVersion;
    map<string,vector<double> >m_mapWeightAndFreeflow;
    
    map<int,vector<string> >m_mapCustomObjList;
    pthread_mutex_t m_custom_obj_mutex;
    pthread_mutex_t m_percent_mutex;
    
    leveldb::Cache* m_pFreeflowAndWeightCache;
    leveldb::Cache* m_pRealtimeTrafficDataCache;

    string m_strNodeType;
    string m_strHostName;
};

typedef struct
{
    const char* pszCityCode;
    time_t nCurrentTime;
    TrafficIndex* pTrafficIndex;
}ThreadParameter;
