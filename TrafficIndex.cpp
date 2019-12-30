#include "TrafficIndex.h"

TrafficIndex::TrafficIndex(){
    m_strLinkVersion = "";
    m_strWeightAndFreeflowVersion = "";
    
    m_pFreeflowAndWeightCache = leveldb::NewLRUCache(100000000);
    m_pRealtimeTrafficDataCache = leveldb::NewLRUCache(100000000);
    
    m_pUpdateLinkMySQL = NULL;
    m_pComputeMySQL = NULL;
    m_pSubscribeMySQL = NULL;
    m_pAggreMySQL = NULL;
    m_pInitLinksMySQL = NULL;
    
    m_pSubscribePG = NULL;
    m_pUpdateLinkPG = NULL;
    m_pWeightAndFreeflowPG = NULL;
    m_pInitLinksPG = NULL;
    
    m_pMQRedisConn = NULL;
    m_pLockRedisConn = NULL;
}
TrafficIndex::~TrafficIndex(){
    releaseResource();
}
void TrafficIndex::releaseResource(){
    if (m_pComputeMySQL != NULL){
        mysql_close(m_pComputeMySQL);
        m_pComputeMySQL = NULL;
    }
    
    if (m_pAggreMySQL != NULL){
        mysql_close(m_pAggreMySQL);
        m_pAggreMySQL = NULL;
    }
    
    if (m_pUpdateLinkMySQL != NULL){
        mysql_close(m_pUpdateLinkMySQL);
        m_pUpdateLinkMySQL = NULL;
    }
    
    if (m_pSubscribeMySQL != NULL){
        mysql_close(m_pSubscribeMySQL);
        m_pSubscribeMySQL = NULL;
    }
    
    if (m_pInitLinksMySQL != NULL){
        mysql_close(m_pInitLinksMySQL);
        m_pInitLinksMySQL = NULL;
    }
    
    if (m_pWeightAndFreeflowPG != NULL) {
        PQfinish(m_pWeightAndFreeflowPG);
        m_pWeightAndFreeflowPG = NULL;
    }
    
    if (m_pUpdateLinkPG != NULL) {
        PQfinish(m_pUpdateLinkPG);
        m_pUpdateLinkPG = NULL;
    }
    
    if (m_pSubscribePG != NULL) {
        PQfinish(m_pSubscribePG);
        m_pSubscribePG = NULL;
    }
    
    if (m_pInitLinksPG != NULL) {
        PQfinish(m_pInitLinksPG);
        m_pInitLinksPG = NULL;
    }
    
    if (m_pMQRedisConn != NULL) {
        redisFree(m_pMQRedisConn);
        m_pMQRedisConn = NULL;
    }
    
    if (m_pLockRedisConn != NULL) {
        redisFree(m_pLockRedisConn);
        m_pLockRedisConn = NULL;
    }

    m_vecProvince.clear();
    
    if (m_pFreeflowAndWeightCache != NULL) {
        delete m_pFreeflowAndWeightCache;
        m_pFreeflowAndWeightCache = NULL;
    }
    
    if (m_pRealtimeTrafficDataCache != NULL) {
        delete m_pRealtimeTrafficDataCache;
        m_pRealtimeTrafficDataCache = NULL;
    }
    
    map<int,LinkArray*>::iterator iter = m_TTIList.begin();
    for (; iter != m_TTIList.end(); iter++) {
        LinkArray* pLinkArray = iter->second;
        if (pLinkArray != NULL) {
            long long* pLinkSet = pLinkArray->pLinkSet;
            if (pLinkSet != NULL) {
                delete []pLinkSet;
                pLinkSet = NULL;
            }
            delete pLinkArray;
            pLinkArray = NULL;
        }
    }
}

MYSQL* TrafficIndex::getMySQLConn(){
    MYSQL* pMySQL = mysql_init(NULL);
    
    if (pMySQL == NULL){
        return NULL;
    }
    
    char value = 1;
    mysql_options(pMySQL, MYSQL_OPT_RECONNECT, &value);
    
    if (mysql_real_connect(pMySQL, m_mySQLInfo.strHost.c_str(), m_mySQLInfo.strUser.c_str(),m_mySQLInfo.strPassword.c_str(),
                           m_mySQLInfo.strDBName.c_str(), atoi(m_mySQLInfo.strPort.c_str()), NULL, NULL) == NULL){
        return NULL;
    }
    
    mysql_query(pMySQL, "SET NAMES UTF8");
    
    return pMySQL;
}

PGconn *TrafficIndex::getPGConn(){
    char szConnectInfo[1024] = {0};
    sprintf(szConnectInfo, "host=%s port=%s dbname=%s user=%s password=%s",m_pgInfo.strHost.c_str(),m_pgInfo.strPort.c_str(),m_pgInfo.strDBName.c_str(),m_pgInfo.strUser.c_str(),m_pgInfo.strPassword.c_str());
    
    PGconn *pPGConn = PQconnectdb(szConnectInfo);
    
    ConnStatusType connStatus = PQstatus(pPGConn);
    
    if (connStatus != CONNECTION_OK){
        string strErr = "连接Postgresql失败，错误信息：";
        strErr += PQerrorMessage(pPGConn);
        LogUlits::appendMsg(strErr.c_str());
        PQfinish(pPGConn);
        return NULL;
    }
    
    return pPGConn;
}

redisContext* TrafficIndex::getRedisConn(){
    struct timeval timeout = {5, 500000}; // 5.5 seconds
    redisContext* pRedisConn = redisConnectWithTimeout(m_redisInfo.strHost.c_str(), atoi(m_redisInfo.strPort.c_str()), timeout);
    
    if(pRedisConn == NULL || pRedisConn->err ){
        if (pRedisConn != NULL){
            string strErr = "连接Redis失败，错误信息：";
            strErr += pRedisConn->errstr;
            LogUlits::appendMsg(strErr.c_str());
            redisFree(pRedisConn);
            pRedisConn = NULL;
        }else{
            string strErr = "初始化Redis失败";
            LogUlits::appendMsg(strErr.c_str());
        }
        
        return NULL;
    }
    
    redisReply* pAuthReply = (redisReply *)redisCommand(pRedisConn, "auth %s", m_redisInfo.strPassword.c_str());
    if (pAuthReply->type == REDIS_REPLY_ERROR){
        freeReplyObject(pAuthReply);
        redisFree(pRedisConn);
        LogUlits::appendMsg("认证redis失败");
        return NULL;
    }
    
    freeReplyObject(pAuthReply);
    
    return pRedisConn;
}

bool TrafficIndex::connectDatabase(){
    m_pUpdateLinkMySQL = getMySQLConn();
    m_pComputeMySQL = getMySQLConn();
    m_pAggreMySQL = getMySQLConn();
    m_pSubscribeMySQL = getMySQLConn();
    m_pInitLinksMySQL = getMySQLConn();
    
    m_pWeightAndFreeflowPG = getPGConn();
    m_pUpdateLinkPG = getPGConn();
    m_pSubscribePG = getPGConn();
    m_pInitLinksPG = getPGConn();
    
    m_pMQRedisConn = getRedisConn();
    m_pLockRedisConn = getRedisConn();
    
    if (m_pUpdateLinkMySQL == NULL || m_pComputeMySQL == NULL || m_pAggreMySQL == NULL || m_pSubscribeMySQL == NULL
        || m_pWeightAndFreeflowPG == NULL || m_pUpdateLinkPG == NULL || m_pSubscribePG == NULL
        || m_pMQRedisConn == NULL || m_pLockRedisConn == NULL || m_pInitLinksMySQL == NULL || m_pInitLinksPG == NULL){
        LogUlits::appendMsg("初始化数据库连接对象失败");
        return false;
    }
    
    return true;
}

void TrafficIndex::getTTILinks(){
    LogUlits::appendMsg("开始获取link集合");
//    int nRes = mysql_query(m_pInitLinksMySQL, "select obj_id,astext(geom),attibute_filter from traffic_obj_info where city = '济南市' and obj_type in (1,2);");
    int nRes = mysql_query(m_pInitLinksMySQL, "select obj_id,astext(geom),attibute_filter from traffic_obj_info");
    if (nRes == 0) {
        MYSQL_RES* pRes = mysql_store_result(m_pInitLinksMySQL);
        if (pRes != NULL) {
            MYSQL_ROW ppRecord;
            while ((ppRecord = mysql_fetch_row(pRes))){
                if (ppRecord[0] != NULL && ppRecord[1] != NULL){
                    const char* pszObjID = ppRecord[0];
                    const char* pszGeom = ppRecord[1];
                    const char* pszFilter = ppRecord[2];
                    
                    if (pszObjID != NULL && pszGeom != NULL && pszFilter != NULL) {
                        string strWKT = pszGeom;
                        long long llSize = 0;
                        long long* pLinkSet = queryELinkID(m_pInitLinksPG,strWKT,pszFilter,llSize);
                        if (pLinkSet != NULL) {
                            LinkArray* linkArray = new LinkArray;
                            
                            linkArray->pLinkSet = pLinkSet;
                            linkArray->llSize = llSize;
                            
                            if (llSize != 0) {
                                pthread_mutex_lock(&m_tti_obj_mutex);
                                m_TTIList[atoi(pszObjID)] = linkArray;
                                pthread_mutex_unlock(&m_tti_obj_mutex);
                                char szMsg[128] = {0};
                                sprintf(szMsg, "obj_id:%s,link总数:%lld",pszObjID,llSize);
                                LogUlits::appendMsg(szMsg);
                            }
                        }else{
                            char szMsg[512] = {0};
                            sprintf(szMsg, "未获取到links,obj_id=%s",pszObjID);
                            LogUlits::appendMsg(szMsg);
                        }
                    }
                }
            }
            mysql_free_result(pRes);
        }
    }
    LogUlits::appendMsg("获取link集合结束");
}

long long* TrafficIndex::queryELinkID(PGconn* pConn,string& strWKT,string strFilter,long long& llSize){
    if (pConn == NULL || m_strLinkVersion == "") {
        return NULL;
    }
    
    string strSQL = "";
    string::size_type idx = strWKT.find("LINE");
    
    if (idx != string::npos) {
        strSQL = "select link_id,direction from ";
        strSQL += m_strLinkVersion;
        strSQL += "_link where st_contains(st_buffer(st_geomfromtext('";
        strSQL += strWKT;
        strSQL += "'),0.00001),geom);";
    } else {
        strSQL = "select link_id,direction from ";
        strSQL += m_strLinkVersion;
        strSQL += "_link where st_intersects";
        strSQL += "(geom,st_geomfromtext('";
        strSQL += strWKT;
        strSQL += "'))";
    }
    
    if (strFilter != "") {
        strSQL += " and (";
        strSQL += strFilter;
        strSQL += ")";
    }
    
    PGresult* pGRes = PQexec(pConn, strSQL.c_str());
    
    if (PQresultStatus(pGRes) != PGRES_TUPLES_OK){
        string strErr = "SQL查询错误,错误信息:";
        strErr += PQerrorMessage(pConn);
        strErr += ",SQL:";
        strErr += strSQL;
        LogUlits::appendMsg(strErr.c_str());
        PQclear(pGRes);
        return NULL;
    }
    
    int nRow = PQntuples(pGRes);
    if (nRow == 0) {
        PQclear(pGRes);
        return NULL;
    }
    long long* pLinkArray = new long long[nRow * 2];
    memset(pLinkArray, 0, sizeof(long long) * nRow * 2);
    
    for(int i = 0; i < nRow;i++){
        const char* pszID = PQgetvalue(pGRes,i,0);
        const char* pszDirection = PQgetvalue(pGRes,i,1);
        if (pszID != NULL && pszDirection != NULL) {
            int nDirection = atoi(pszDirection);
            switch (nDirection){
                case 0:{
                    char szLinkID_0[128] = {0};
                    char szLinkID_1[128] = {0};
                    sprintf(szLinkID_0, "%s0",pszID);
                    sprintf(szLinkID_1, "%s1",pszID);
                    pLinkArray[llSize++] = atoll(szLinkID_0);
                    pLinkArray[llSize++] = atoll(szLinkID_1);
                }
                    break;
                case 1:{
                    char szLinkID_0[128] = {0};
                    char szLinkID_1[128] = {0};
                    sprintf(szLinkID_0, "%s0",pszID);
                    sprintf(szLinkID_1, "%s1",pszID);
                    pLinkArray[llSize++] = atoll(szLinkID_0);
                    pLinkArray[llSize++] = atoll(szLinkID_1);
                }
                    break;
                case 2:{
                    char szLinkID_0[128] = {0};
                    sprintf(szLinkID_0, "%s0",pszID);
                    pLinkArray[llSize++] = atoll(szLinkID_0);
                }
                    break;
                case 3:{
                    char szLinkID_1[128] = {0};
                    sprintf(szLinkID_1, "%s1",pszID);
                    pLinkArray[llSize++] = atoll(szLinkID_1);
                }
                    break;
                default:
                    break;
            }
        }
    }
    PQclear(pGRes);
    return pLinkArray;
}
void TrafficIndex::computeLinkSetTTI(time_t nBatchTime,int nObj_id,LinkArray* pLinkSet,double& dfTTI,double& dfSpd){
    if (pLinkSet == NULL) {
        return;
    }
    double dfTTIFenzi = 0;
    double dfTTIFenmu = 0;
    
    double dfSpdFenzi = 0;
    double dfSpdFenmu = 0;
    
    for (size_t i = 0; i < pLinkSet->llSize; i++){
        string strELinkID = CommonTools::toString((long long)pLinkSet->pLinkSet[i]);
        
        double dfWeight;
        double dfFreeflow;
        getFreeflowAndWeight(strELinkID, dfFreeflow, dfWeight);
        
        if (dfFreeflow == 0 || dfWeight == 0){
            continue;
        }
        
        leveldb::Cache::Handle* pLookupHandle = m_pRealtimeTrafficDataCache->Lookup(strELinkID);
        if (pLookupHandle == NULL){
            continue;
        }
        
        pthread_mutex_lock(&m_TrafficDataMutex);
        RealtimeTraffic* pRealtimeData = (RealtimeTraffic*)m_pRealtimeTrafficDataCache->Value(pLookupHandle);
        if (pRealtimeData != NULL) {
            vector<trafficInfo>* pVecElement = pRealtimeData->pVecElement;
            if (pVecElement != NULL) {
                int nBatchCount = 0;
                for (int j = 0; j < (*pVecElement).size(); j++){
                    trafficInfo currentTraffic = (*pVecElement)[j];
                    
                    time_t nDiff = nBatchTime - currentTraffic.nBatchTime;
                    
                    if (nDiff >= 600 || nDiff < 0){
                        continue;
                    }
                    
                    if (currentTraffic.dfSpeed == 0) {
                        continue;
                    }
                    
                    double dfRealTime = currentTraffic.dfLength / currentTraffic.dfSpeed;
                    
                    dfTTIFenzi += dfRealTime * dfWeight;
                    //dfSpdFenmu += dfRealTime * dfWeight;
                    dfSpdFenmu = dfTTIFenzi;
                    
                    double dfFreeflowTime = currentTraffic.dfLength / dfFreeflow;
                    dfTTIFenmu += dfFreeflowTime * dfWeight;
                    dfSpdFenzi += currentTraffic.dfLength * dfWeight;
                    if (++nBatchCount == 5) {
                        break;
                    }
                }
            }
        }
        m_pRealtimeTrafficDataCache->Release(pLookupHandle);
        pthread_mutex_unlock(&m_TrafficDataMutex);
    }
    
    if (dfTTIFenmu != 0 && dfTTIFenzi != 0){
        dfTTI = dfTTIFenzi / dfTTIFenmu;
    }
    
    if (dfSpdFenzi != 0 && dfSpdFenmu != 0){
        dfSpd = dfSpdFenzi / dfSpdFenmu;
    }
    dfTTI = dfTTI > 40 ? 40 : dfTTI;
}
void TrafficIndex::setProvinceList(vector<string>& vecProvince) {
    m_vecProvince = vecProvince;
}

void TrafficIndex::setTrafficPublicURL(string& strTrafficPublicURL) { 
    m_strTrafficPublicURL = strTrafficPublicURL;
}
string TrafficIndex::getTrafficPublicURL(){
    return m_strTrafficPublicURL;
}
void TrafficIndex::getProvinceList(vector<string>& vecProvince)
{
    vecProvince.clear();
    vecProvince = m_vecProvince;
}

void TrafficIndex::setMySQLInfo(DBInfo& mySQLInfo) {
    m_mySQLInfo = mySQLInfo;
}

void TrafficIndex::setPGInfo(DBInfo& pgInfo) {
    m_pgInfo = pgInfo;
}

void TrafficIndex::setRedisInfo(DBInfo& redisInfo) {
    m_redisInfo = redisInfo;
}

void TrafficIndex::setTrafficDataMutex(pthread_mutex_t& traffic_data_mutex) { 
    m_TrafficDataMutex = traffic_data_mutex;
}

void TrafficIndex::set_tti_obj_mutex(pthread_mutex_t& tti_obj_mutex) {
    m_tti_obj_mutex = tti_obj_mutex;
}

void TrafficIndex::getFreeflowAndWeight(string& strELink,double& dfFreeflow,double& dfWeight){
    dfFreeflow = 0;
    dfWeight = 0;
    
    if (m_pFreeflowAndWeightCache == NULL) {
        return;
    }
    
    leveldb::Cache::Handle* pHandle = m_pFreeflowAndWeightCache->Lookup(strELink);
    if (pHandle != NULL) {
        pthread_mutex_lock(&m_UpdatWeightAndFreeflowMutex);
        
        WeightAndFreeflow* pWeightAndFreeflow = (WeightAndFreeflow*)m_pFreeflowAndWeightCache->Value(pHandle);
        if (pWeightAndFreeflow != NULL) {
            double* pValues = pWeightAndFreeflow->pValues;
            if (pValues != NULL) {
                dfWeight = pValues[0];
                dfFreeflow = pValues[1];
            }
        }
        
        m_pFreeflowAndWeightCache->Release(pHandle);
        pthread_mutex_unlock(&m_UpdatWeightAndFreeflowMutex);
    }
}

void TrafficIndex::computeTTI(time_t nBatch_time){
    pthread_mutex_lock(&m_tti_obj_mutex);
    
    char szMsg[128] = {0};
    sprintf(szMsg, "mapObjList.size = %ld",m_TTIList.size());
    LogUlits::appendMsg(szMsg);
    
    if (m_TTIList.empty()){
        pthread_mutex_unlock(&m_tti_obj_mutex);
        return;
    }
    
    string strBatchTime = CommonTools::unix2Standard(nBatch_time);
    string strSQL = "replace into obj_tti_detail (obj_id,batch_time,update_time,tti,avgspd) values ";
    size_t nLength = strSQL.length();
    
    int nInsertCount = 0;
    int nObjCount = 0;
    map<int,LinkArray*>::iterator iter = m_TTIList.begin();
    for (; iter != m_TTIList.end(); iter++){
        if (iter->second == NULL) {
            continue;
        }
        
        int nObjID = iter->first;
        
        if (iter->second->llSize > 0) {
            if (!isGetLock(nObjID,nBatch_time)) {
                continue;
            }
            nObjCount++;
            double dfTTI = 0;
            double dfSpd = 0;
            
            computeLinkSetTTI(nBatch_time,nObjID,iter->second,dfTTI,dfSpd);
            
            if (dfTTI != 0 && dfSpd != 0){
                nInsertCount++;
                
                char szValues[512] = {0};
                sprintf(szValues, "(%d,'%s',now(),%f,%f),",nObjID,strBatchTime.c_str(),dfTTI,dfSpd);
                strSQL += szValues;
                
                if (nInsertCount % 500 == 0){
                    strSQL = strSQL.substr(0,strSQL.length() - 1);
                    
                    mysql_ping(m_pComputeMySQL);
                    int nRes = mysql_query(m_pComputeMySQL, strSQL.c_str());
                    
                    if (nRes != 0){
                        string strMsg = "写入TTI结果失败，失败信息：";
                        strMsg += mysql_error(m_pComputeMySQL);
                        strMsg += ",SQL";
                        strMsg += strSQL;
                        LogUlits::appendMsg(strMsg.c_str());
                    }
                    
                    strSQL = "replace into obj_tti_detail (obj_id,batch_time,update_time,tti,avgspd) values ";
                }
                
                char szRealValus[512] = {0};
                sprintf(szRealValus, "replace into obj_tti_realtime (obj_id,tti,avgspd,batch_time,update_time) values(%d,%f,%f,'%s',now())",nObjID,dfTTI,dfSpd,strBatchTime.c_str());
                

                int nRes = mysql_query(m_pComputeMySQL, szRealValus);
                if (nRes != 0){
                    string strMsg = "写入实时TTI失败，失败信息：";
                    strMsg += mysql_error(m_pComputeMySQL);
                    strMsg += ",SQL";
                    strMsg += szRealValus;
                    LogUlits::appendMsg(strMsg.c_str());
                }
            }
        }
    }
    
    memset(szMsg, 0, sizeof(char) * 128);
    sprintf(szMsg, "计算TTI对象个数:%d",nObjCount);
    LogUlits::appendMsg(szMsg);
    
    if (strSQL.length() > nLength){
        strSQL = strSQL.substr(0,strSQL.length() - 1);
        
        int nPingRes = mysql_ping(m_pComputeMySQL);
        if (nPingRes != 0){
            string strMsg = "重连sql失败，失败信息：";
            strMsg += mysql_error(m_pComputeMySQL);
            LogUlits::appendMsg(strMsg.c_str());
            return;
        }
        
        int nRes = mysql_query(m_pComputeMySQL, strSQL.c_str());
        
        if (nRes != 0){
            string strMsg = "写入TTI结果失败，失败信息：";
            strMsg += mysql_error(m_pComputeMySQL);
            strMsg += ",SQL";
            strMsg += strSQL;
            LogUlits::appendMsg(strMsg.c_str());
        }
    }

    pthread_mutex_unlock(&m_tti_obj_mutex);
}
bool TrafficIndex::isGetLock(int nObj_id,time_t nBatch_time){
    if (m_pLockRedisConn == NULL) {
        return false;
    }
    
    bool bIsOK = false;
    
    //过期时间设置为30分钟
    redisReply* pReply = (redisReply*)redisCommand(m_pLockRedisConn,"set %d_%lld locked ex %d nx",nObj_id,nBatch_time,60 * 30);
    if (pReply != NULL) {
        if (pReply->type == REDIS_REPLY_STATUS) {
            if (pReply->str != NULL) {
                if (strcmp(pReply->str, "OK") == 0) {
                    bIsOK = true;
                }
            }
        }
    }
    
    freeReplyObject(pReply);

    return bIsOK;
}
void TrafficIndex::getWeightAndFreeflow(){
    string strSQL = "select elink_id,weight,freeflow from ";
    strSQL += m_strWeightAndFreeflowVersion;
    PGresult* pGRes = PQexec(m_pWeightAndFreeflowPG, strSQL.c_str());
    
    if (PQresultStatus(pGRes) != PGRES_TUPLES_OK){
        string strErr = "未获取到权重自由流数据，错误信息：";
        strErr += PQerrorMessage(m_pWeightAndFreeflowPG);
        LogUlits::appendMsg(strErr.c_str());
        PQclear(pGRes);
        return;
    }
    
    int nRow = PQntuples(pGRes);
    
    for(int i = 0; i < nRow;i++){
        const char* pszELinkID = PQgetvalue(pGRes,i,0);
        const char* pszWeight = PQgetvalue(pGRes,i,1);
        const char* pszFreeflow = PQgetvalue(pGRes,i,2);
        
        if (pszELinkID == NULL || pszWeight == NULL || pszFreeflow == NULL) {
            continue;
        }
        
        string strELinkID = pszELinkID;
        leveldb::Cache::Handle* pLookupHandle = m_pFreeflowAndWeightCache->Lookup(strELinkID);
        if (pLookupHandle == NULL){
            double* pValues = new double[2];
            pValues[0] = atof(pszWeight);
            pValues[1] = atof(pszFreeflow);
            
            WeightAndFreeflow* pWeightAndFreeflow = new WeightAndFreeflow;
            pWeightAndFreeflow->pValues = pValues;
            pWeightAndFreeflow->weightAndFreeflowMutex = m_UpdatWeightAndFreeflowMutex;
            
            leveldb::Cache::Handle* pInsertHandle = m_pFreeflowAndWeightCache->Insert(strELinkID, pWeightAndFreeflow, 1, &TrafficIndex::deleteFreeflowAndWeightCache);
            m_pFreeflowAndWeightCache->Release(pInsertHandle);
        }else{
            pthread_mutex_lock(&m_UpdatWeightAndFreeflowMutex);
            
            WeightAndFreeflow* pWeightAndFreeflow = (WeightAndFreeflow*)m_pFreeflowAndWeightCache->Value(pLookupHandle);
            if (pWeightAndFreeflow != NULL) {
                double* pValues = pWeightAndFreeflow->pValues;
                if (pValues != NULL) {
                    pValues[0] = atof(pszWeight);
                    pValues[1] = atof(pszFreeflow);
                }
            }

            m_pFreeflowAndWeightCache->Release(pLookupHandle);
            pthread_mutex_unlock(&m_UpdatWeightAndFreeflowMutex);
        }
    }
    
    PQclear(pGRes);
}

void TrafficIndex::updateWeightAndFreeflow(){
    string strWeightAndFreeflowVersion = getFeatureVersion("weight_freeflow",m_pWeightAndFreeflowPG);
    if (strWeightAndFreeflowVersion == "") {
        return;
    }
    
    if (strWeightAndFreeflowVersion != m_strWeightAndFreeflowVersion) {
        char szMsg[512] = {0};
        sprintf(szMsg, "更新自由流,旧版本:%s,新版本:%s",m_strWeightAndFreeflowVersion.c_str(),strWeightAndFreeflowVersion.c_str());
        LogUlits::appendMsg(szMsg);
        
        m_strWeightAndFreeflowVersion = strWeightAndFreeflowVersion;
        
        getWeightAndFreeflow();
        
        LogUlits::appendMsg("更新自由流结束");
    }
}
void TrafficIndex::updateLink(){
    string strLinkVersion = getFeatureVersion("link",m_pUpdateLinkPG);
    if (strLinkVersion == "") {
        return;
    }
    
    if (strLinkVersion != m_strLinkVersion) {
        char szMsg[512] = {0};
        sprintf(szMsg, "更新路网数据,旧版本:%s,新版本:%s",m_strLinkVersion.c_str(),strLinkVersion.c_str());
        LogUlits::appendMsg(szMsg);
        
        m_strLinkVersion = strLinkVersion;
        
        int nRes = mysql_query(m_pUpdateLinkMySQL, "select obj_id,astext(geom),attibute_filter from traffic_obj_info order by obj_id asc;");
        
        if (nRes == 0) {
            MYSQL_RES* pRes = mysql_store_result(m_pUpdateLinkMySQL);
            if (pRes != NULL) {
                MYSQL_ROW ppRecord;
                while ((ppRecord = mysql_fetch_row(pRes))){
                    if (ppRecord[0] != NULL && ppRecord[1] != NULL){
                        const char* pszObjID = ppRecord[0];
                        const char* pszGeom = ppRecord[1];
                        const char* pszFilter = ppRecord[2];
                        
                        if (pszObjID != NULL && pszGeom != NULL && pszFilter != NULL) {
                            string strWKT = pszGeom;
                            pthread_mutex_lock(&m_tti_obj_mutex);
                            map<int,LinkArray*>::iterator iter = m_TTIList.find(atoi(pszObjID));
                            if (iter != m_TTIList.end()) {
                                long long llSize = 0;
                                long long* pLinkSet = queryELinkID(m_pUpdateLinkPG,strWKT,pszFilter,llSize);
                                if (pLinkSet != NULL) {
                                    LinkArray* pOldLinkArray = iter->second;
                                    if (pOldLinkArray != NULL) {
                                        long long* pLinkSet = pOldLinkArray->pLinkSet;
                                        if (pLinkSet != NULL) {
                                            delete []pLinkSet;
                                            pLinkSet = NULL;
                                        }
                                        pOldLinkArray->llSize = 0;
                                        delete pOldLinkArray;
                                        pOldLinkArray = NULL;
                                    }
                                    
                                    LinkArray* pNewLinkArray = new LinkArray;
                                    
                                    pNewLinkArray->pLinkSet = pLinkSet;
                                    pNewLinkArray->llSize = llSize;
                                    m_TTIList[atoi(pszObjID)] = pNewLinkArray;
                                }else{
                                    char szMsg[512] = {0};
                                    sprintf(szMsg, "未获取到links,obj_id=%s",pszObjID);
                                    LogUlits::appendMsg(szMsg);
                                }
                            }
                            pthread_mutex_unlock(&m_tti_obj_mutex);
                        }
                    }
                }
                mysql_free_result(pRes);
            }
        }
        LogUlits::appendMsg("更新路网结束");
    }
}

void TrafficIndex::setUpdateWeightAndFreeflowMutex(pthread_mutex_t &update_weight_freeflow_mutex) { 
    m_UpdatWeightAndFreeflowMutex = update_weight_freeflow_mutex;
}

void TrafficIndex::aggreTTT(int nType,time_t nCurrentTime){
    nCurrentTime -= 900;
    vector<string> vecFailedSQL;
    
    if (nType == 1){
        time_t nStart = nCurrentTime - 60 * 60;
        string strStart = CommonTools::unix2Standard(nStart);
        string strEnd = CommonTools::unix2Standard(nCurrentTime);

        char szInsertSQL[1024] = {0};
        sprintf(szInsertSQL, "replace into obj_tti_hourly (obj_id,tti,avgspd,batch_time) select obj_id,avg(tti),avg(avgspd),'%s' from obj_tti_detail where batch_time >= '%s' and batch_time < '%s' group by obj_id;",strEnd.c_str(),strStart.c_str(),strEnd.c_str());
        
        mysql_ping(m_pAggreMySQL);
        int nRes = mysql_query(m_pAggreMySQL, szInsertSQL);
        
        if (nRes != 0){
            string strErr = "写入小时表失败，错误信息：";
            strErr += mysql_error(m_pAggreMySQL);
            strErr += ",sql:";
            strErr += szInsertSQL;
            LogUlits::appendMsg(strErr.c_str());
            vecFailedSQL.push_back(szInsertSQL);
        }
    }else{
        string strRealtime = CommonTools::unix2Standard(nCurrentTime - 1);
        vector<string> vecSplit;
        CommonTools::split(strRealtime, " ", vecSplit);
        
        string strBatchTime = vecSplit[0];
        
        char szStart_1[512] = {0};
        sprintf(szStart_1, "%s 07:00:00",strBatchTime.c_str());
        
        char szEnd_1[512] = {0};
        sprintf(szEnd_1, "%s 19:00:00",strBatchTime.c_str());
        
        char szInsertSQL_1[1024] = {0};
        sprintf(szInsertSQL_1, "replace into obj_tti_daily (obj_id,tti,avgspd,batch_time,aggre_type) select obj_id,avg(tti),avg(avgspd),'%s 00:00:00',1 from obj_tti_detail where batch_time >= '%s' and batch_time < '%s' group by obj_id;",strBatchTime.c_str(),szStart_1,szEnd_1);
        
        mysql_ping(m_pAggreMySQL);
        int nRes = mysql_query(m_pAggreMySQL, szInsertSQL_1);
        
        if (nRes != 0){
            string strErr = "写入天表失败，错误信息：";
            strErr += mysql_error(m_pAggreMySQL);
            strErr += ",sql:";
            strErr += szInsertSQL_1;
            LogUlits::appendMsg(strErr.c_str());
            vecFailedSQL.push_back(szInsertSQL_1);
        }
        
        //////////
        char szStart_2_1[512] = {0};
        sprintf(szStart_2_1, "%s 19:00:00",strBatchTime.c_str());
        char szEnd_2_1[512] = {0};
        sprintf(szEnd_2_1, "%s 23:59:59",strBatchTime.c_str());
        
        
        char szStart_2_2[512] = {0};
        sprintf(szStart_2_2, "%s 00:00:00",strBatchTime.c_str());
        char szEnd_2_2[512] = {0};
        sprintf(szEnd_2_2, "%s 07:00:00",strBatchTime.c_str());
        
        char szInsertSQL_2[1024] = {0};
        sprintf(szInsertSQL_2, "replace into obj_tti_daily (obj_id,tti,avgspd,batch_time,aggre_type) select obj_id,avg(tti),avg(avgspd),'%s 00:00:00',2 from obj_tti_detail where (batch_time >= '%s' and batch_time <= '%s') or (batch_time >= '%s' and batch_time < '%s') group by obj_id;",strBatchTime.c_str(),szStart_2_1,szEnd_2_1,szStart_2_2,szEnd_2_2);
        
        mysql_ping(m_pAggreMySQL);
        nRes = mysql_query(m_pAggreMySQL, szInsertSQL_2);
        
        if (nRes != 0){
            string strErr = "写入天表失败，错误信息：";
            strErr += mysql_error(m_pAggreMySQL);
            strErr += ",sql:";
            strErr += szInsertSQL_2;
            LogUlits::appendMsg(strErr.c_str());
            vecFailedSQL.push_back(szInsertSQL_2);
        }
        
        ///////////
        char szStart_3[512] = {0};
        sprintf(szStart_3, "%s 07:30:00",strBatchTime.c_str());
        
        char szEnd_3[512] = {0};
        sprintf(szEnd_3, "%s 09:30:00",strBatchTime.c_str());
        
        char szInsertSQL_3[1024] = {0};
        sprintf(szInsertSQL_3, "replace into obj_tti_daily (obj_id,tti,avgspd,batch_time,aggre_type) select obj_id,avg(tti),avg(avgspd),'%s 00:00:00',3 from obj_tti_detail where batch_time >= '%s' and batch_time < '%s' group by obj_id;",strBatchTime.c_str(),szStart_3,szEnd_3);
        
        mysql_ping(m_pAggreMySQL);
        nRes = mysql_query(m_pAggreMySQL, szInsertSQL_3);
        
        if (nRes != 0){
            string strErr = "写入天表失败，错误信息：";
            strErr += mysql_error(m_pAggreMySQL);
            strErr += ",sql:";
            strErr += szInsertSQL_3;
            LogUlits::appendMsg(strErr.c_str());
            vecFailedSQL.push_back(szInsertSQL_3);
        }
        
        ///////////
        char szStart_4[512] = {0};
        sprintf(szStart_4, "%s 17:30:00",strBatchTime.c_str());
        
        char szEnd_4[512] = {0};
        sprintf(szEnd_4, "%s 19:30:00",strBatchTime.c_str());
        
        char szInsertSQL_4[1024] = {0};
        sprintf(szInsertSQL_4, "replace into obj_tti_daily (obj_id,tti,avgspd,batch_time,aggre_type) select obj_id,avg(tti),avg(avgspd),'%s 00:00:00',4 from obj_tti_detail where batch_time >= '%s' and batch_time < '%s' group by obj_id;",strBatchTime.c_str(),szStart_4,szEnd_4);
        
        mysql_ping(m_pAggreMySQL);
        nRes = mysql_query(m_pAggreMySQL, szInsertSQL_4);
        
        if (nRes != 0){
            string strErr = "写入天表失败，错误信息：";
            strErr += mysql_error(m_pAggreMySQL);
            strErr += ",sql:";
            strErr += szInsertSQL_4;
            LogUlits::appendMsg(strErr.c_str());
            vecFailedSQL.push_back(szInsertSQL_4);
        }
        
        ///////////
        char szStart_5[512] = {0};
        sprintf(szStart_5, "%s 11:00:00",strBatchTime.c_str());
        
        char szEnd_5[512] = {0};
        sprintf(szEnd_5, "%s 16:00:00",strBatchTime.c_str());
        
        char szInsertSQL_5[1024] = {0};
        sprintf(szInsertSQL_5, "replace into obj_tti_daily (obj_id,tti,avgspd,batch_time,aggre_type) select obj_id,avg(tti),avg(avgspd),'%s 00:00:00',5 from obj_tti_detail where batch_time >= '%s' and batch_time < '%s' group by obj_id;",strBatchTime.c_str(),szStart_5,szEnd_5);
        
        mysql_ping(m_pAggreMySQL);
        mysql_query(m_pAggreMySQL, szInsertSQL_5);
        
        if (nRes != 0){
            string strErr = "写入天表失败，错误信息：";
            strErr += mysql_error(m_pAggreMySQL);
            strErr += ",sql:";
            strErr += szInsertSQL_5;
            LogUlits::appendMsg(strErr.c_str());
            vecFailedSQL.push_back(szInsertSQL_5);
        }
        
        ///////////
        char szStart_6[512] = {0};
        sprintf(szStart_6, "%s 00:00:00",strBatchTime.c_str());
        
        char szEnd_6[512] = {0};
        sprintf(szEnd_6, "%s 23:59:59",strBatchTime.c_str());
        
        char szInsertSQL_6[1024] = {0};
        sprintf(szInsertSQL_6, "replace into obj_tti_daily (obj_id,tti,avgspd,batch_time,aggre_type) select obj_id,avg(tti),avg(avgspd),'%s 00:00:00',6 from obj_tti_detail where batch_time >= '%s' and batch_time <='%s' group by obj_id;",strBatchTime.c_str(),szStart_6,szEnd_6);
        
        mysql_ping(m_pAggreMySQL);
        nRes = mysql_query(m_pAggreMySQL, szInsertSQL_6);
        
        if (nRes != 0){
            string strErr = "写入天表失败，错误信息：";
            strErr += mysql_error(m_pAggreMySQL);
            strErr += ",sql:";
            strErr += szInsertSQL_6;
            LogUlits::appendMsg(strErr.c_str());
            vecFailedSQL.push_back(szInsertSQL_6);
        }
    }
    
    //对执行失败的SQL重新执行
    int nRetryCount = 0;
    while (true) {
        if (vecFailedSQL.empty()) {
            break;
        }
        
        nRetryCount++;
        
        vector<string>::iterator iter;
        for (iter = vecFailedSQL.begin(); iter != vecFailedSQL.end(); ) {
            mysql_ping(m_pAggreMySQL);
            int nRetryRes = mysql_query(m_pAggreMySQL, (*iter).c_str());
            if (nRetryRes == 0) {
                iter = vecFailedSQL.erase(iter);
            } else {
                ++iter;
            }
        }
        
        if (nRetryCount == 3) {
            break;
        }
    }
    
    for (size_t i = 0; i < vecFailedSQL.size(); i++) {
        string strMsg = "重试写数据库失败，SQL:";
        strMsg += vecFailedSQL[i];
        LogUlits::appendMsg(strMsg.c_str());
    }
}
string TrafficIndex::getFeatureVersion(string strFeatureType,PGconn* pConn){
    string strFeatureVersion = "";
    if (pConn == NULL) {
        return strFeatureVersion;
    }
    
    PGresult* pGResult = PQexec(pConn, "select feature_type,feature_version from link_config where feature_type in ('link','weight_freeflow'); ");
    
    if (PQresultStatus(pGResult) != PGRES_TUPLES_OK)
    {
        string strErr = "未获取路网版本，错误信息：";
        strErr += PQerrorMessage(pConn);
        LogUlits::appendMsg(strErr.c_str());
        PQclear(pGResult);
        return strFeatureVersion;
    }
    
    int nRow = PQntuples(pGResult);
    
    for(int i = 0; i < nRow;i++)
    {
        string strType = PQgetvalue(pGResult,i,0);
        string strVersion = PQgetvalue(pGResult,i,1);
        
        if (strType == strFeatureType) {
            strFeatureVersion = strVersion;
            break;
        }
    }
    
    PQclear(pGResult);
    
    return strFeatureVersion;
}
bool TrafficIndex::initJob(){
    m_strLinkVersion = getFeatureVersion("link",m_pUpdateLinkPG);
    m_strWeightAndFreeflowVersion = getFeatureVersion("weight_freeflow",m_pUpdateLinkPG);
    
    if (m_strLinkVersion == "" || m_strWeightAndFreeflowVersion == "") {
        LogUlits::appendMsg("获取路网或自由流版本失败");
        return false;
    }
    char szMsg[512] = {0};
    sprintf(szMsg, "路网版本:%s,自由流版本:%s",m_strLinkVersion.c_str(),m_strWeightAndFreeflowVersion.c_str());
    LogUlits::appendMsg(szMsg);
    
    LogUlits::appendMsg("获取权重和自由流");
    getWeightAndFreeflow();
    
    return true;
}
void TrafficIndex::remove_tti_object(string& strJobID){
    string strDelSQL = "select job_id,obj_id from custom_job where job_id in (";
    strDelSQL += strJobID;
    strDelSQL += ");";
    int nRes = mysql_query(m_pSubscribeMySQL, strDelSQL.c_str());
    
    if (nRes == 0) {
        MYSQL_RES* pRes = mysql_store_result(m_pSubscribeMySQL);
        if (pRes != NULL) {
            MYSQL_ROW ppRecord;
            while ((ppRecord = mysql_fetch_row(pRes))){
                if (ppRecord[0] != NULL && ppRecord[1] != NULL){
                    int nJobID = atoi(ppRecord[0]);
                    int nObjID = atoi(ppRecord[1]);
                    
                    pthread_mutex_lock(&m_tti_obj_mutex);

                    map<int,LinkArray*>::iterator iter = m_TTIList.find(nObjID);
                    if (iter != m_TTIList.end()){
                        LinkArray* pLinkArray = iter->second;
                        if (pLinkArray != NULL) {
                            long long* pLinkSet = pLinkArray->pLinkSet;
                            if (pLinkSet != NULL) {
                                delete []pLinkSet;
                                pLinkSet = NULL;
                            }
                            delete pLinkArray;
                            pLinkArray = NULL;
                        }
                        m_TTIList.erase(iter);
                        
                        char szMsg[512] = {0};
                        sprintf(szMsg, "移除计算对象,job_id=%d,obj_id=%d",nJobID,nObjID);
                        LogUlits::appendMsg(szMsg);
                    }
                    pthread_mutex_unlock(&m_tti_obj_mutex);
                }
            }
            mysql_free_result(pRes);
        }
    }
}
void TrafficIndex::add_tti_object(string& strObjID){
    char szSQL[1024] = {0};
    sprintf(szSQL, "select obj_id,astext(geom),attibute_filter from traffic_obj_info where obj_id = %s;",strObjID.c_str());
    int nRes = mysql_query(m_pSubscribeMySQL, szSQL);
    
    if (nRes == 0) {
        MYSQL_RES* pRes = mysql_store_result(m_pSubscribeMySQL);
        if (pRes != NULL) {
            MYSQL_ROW ppRecord;
            while ((ppRecord = mysql_fetch_row(pRes))){
                if (ppRecord[0] != NULL && ppRecord[1] != NULL){
                    const char* pszObjID = ppRecord[0];
                    const char* pszGeom = ppRecord[1];
                    const char* pszFilter = ppRecord[2];
                    
                    if (pszObjID != NULL && pszGeom != NULL && pszFilter != NULL) {
                        string strWKT = pszGeom;
                        long long llSize = 0;
                        long long* pLinkSet = queryELinkID(m_pSubscribePG,strWKT,pszFilter,llSize);
                        if (pLinkSet != NULL) {
                            LinkArray* pLinkArray = new LinkArray;
                            
                            pLinkArray->pLinkSet = pLinkSet;
                            pLinkArray->llSize = llSize;
                            pthread_mutex_lock(&m_tti_obj_mutex);
                            m_TTIList[atoi(pszObjID)] = pLinkArray;
                            pthread_mutex_unlock(&m_tti_obj_mutex);
                            
                            char szMsg[512] = {0};
                            sprintf(szMsg, "增加计算TTI计算对象,obj_id=%s",pszObjID);
                            LogUlits::appendMsg(szMsg);
                        }else{
                            char szMsg[512] = {0};
                            sprintf(szMsg, "未获取到links,obj_id=%s",pszObjID);
                            LogUlits::appendMsg(szMsg);
                        }
                    }
                }
            }
            mysql_free_result(pRes);
        }
    }
}

void TrafficIndex::consumeRedis(){
    if (m_pMQRedisConn == NULL) {
        return;
    }
    
    redisReply *reply = (redisReply *)redisCommand(m_pMQRedisConn, "subscribe added_obj_set del_obj_set");
    freeReplyObject(reply);
    
    while (redisGetReply(m_pMQRedisConn, (void **)&reply) == REDIS_OK){
        if ( reply->type == REDIS_REPLY_ARRAY && reply->elements == 3 ) {
            string s = reply->element[0]->str;
            if ( strcmp( reply->element[0]->str, "subscribe" ) != 0 ) {
                string strChannel = reply->element[1]->str;
                
                char szMsg[512] = {0};
                sprintf(szMsg,"channel:%s,value:%s",reply->element[1]->str,reply->element[2]->str);
                LogUlits::appendMsg(szMsg);
                
                if (strChannel == "del_obj_set" || strChannel == "added_obj_set") {
                    string strID = reply->element[2]->str;
                    
                    if (strChannel == "del_obj_set") {
                        remove_tti_object(strID);
                    }else{
                        add_tti_object(strID);
                    }
                }
            }
        }
        freeReplyObject(reply);
    }
}
void *TrafficIndex::getProvinceRealtimeTrafficThread(void *pParam){
    ThreadParameter threadPara = *(ThreadParameter*)pParam;
    
    const char* pszCityCode = threadPara.pszCityCode;
    time_t nCurrentTime = threadPara.nCurrentTime;
    TrafficIndex* pTrafficIndex = threadPara.pTrafficIndex;
    leveldb::Cache* pRealtimeTrafficDataCache = pTrafficIndex->m_pRealtimeTrafficDataCache;
    
    char szUser_pwd[128] = { 0 };
    string strPassword = "TTI-Calc";
    sprintf(szUser_pwd, "%sDLR%ld", strPassword.c_str(), nCurrentTime);
    
    char szTime[100] = {0};
    sprintf(szTime, "%ld",nCurrentTime);
    
    MD5 md5;
    md5.reset();
    md5.update(szUser_pwd, strlen(szUser_pwd));
    string strMD5 = md5.toString();
    
#ifdef __APPLE__
    string strURL = "http://traffic.map.xiaojukeji.com/traffic_publish?vid=";
#else
    string strURL = threadPara.pszTrafficPublicURL;
    strURL += "/traffic-dlr/getData?vid=";
#endif
    
    strURL += strMD5;
    strURL += "&citycode=";
    strURL += pszCityCode;
    strURL += "&ts=";
    strURL += szTime;
    strURL += "&username=Calc-TTI";
    
    memoryStruct chunk;
    chunk.memory = (char*)malloc(1);
    chunk.size = 0;
    CommonTools::httpGet(strURL.c_str(), &chunk);
    
    uint32_t nDstLength = 1024 * 1024 * 60;
    unsigned char* szDstBuffer = new unsigned char[nDstLength];
    
    CommonTools::decoderGz((uint8_t*)chunk.memory,(uint32_t)chunk.size,szDstBuffer,nDstLength);
    
    free(chunk.memory);
    
    its::service::TrafficData trafficData;
    if (!trafficData.ParseFromArray(szDstBuffer, nDstLength)){
        string strMsg = "解析pb失败,请求地址:";
        strMsg += strURL;
        LogUlits::appendMsg(strMsg.c_str());
        delete []szDstBuffer;
        return NULL;
    }
    
    int nLinkSize = trafficData.flow().link_arr_size();
    
    if (nLinkSize == 0){
        string strMsg = "link_arr_size=0,请求地址:";
        strMsg += strURL;
        LogUlits::appendMsg(strMsg.c_str());
        delete []szDstBuffer;
        return NULL;
    }
    
    ::google::protobuf::uint32 nBatch_time = trafficData.meta().batch_time();
    
#ifdef __APPLE__
    string strBatchTime = CommonTools::unix2Standard(nBatch_time);
#endif
    
    for (int i = 0; i < nLinkSize; i++){
        unsigned int nConfidence = trafficData.flow().confidence_arr(i);
        
        if (nConfidence >= 20){
            long long lLinkID = trafficData.flow().link_arr().Get(i);
            string strELinkID = CommonTools::toString(lLinkID);
            
            unsigned int nLength = trafficData.flow().length_arr(i);
            
            unsigned int nSpeed = trafficData.flow().speed_without_light_arr(i);
            double dfSpeed = (double)nSpeed / 10.0;
            
            if (dfSpeed == 0) {
                dfSpeed = 0.001;
            }
            
            trafficInfo info;
            info.dfLength = (double)nLength / 1000.0;
            info.strELinkID = strELinkID;
//            info.nConfidence = nConfidence;
            
#ifdef __APPLE__
            info.strBatchTime = strBatchTime;
#endif
            info.dfSpeed = dfSpeed;
            info.nBatchTime = nBatch_time;
            
            leveldb::Cache::Handle* pLookupHandle = pRealtimeTrafficDataCache->Lookup(strELinkID);
            if (pLookupHandle == NULL){
                vector<trafficInfo>* pVecElement = new vector<trafficInfo>();
                pVecElement->push_back(info);
                
                RealtimeTraffic* pRealtimeData = new RealtimeTraffic;
                pRealtimeData->pVecElement = pVecElement;
                pRealtimeData->trafficDataMutex = pTrafficIndex->m_TrafficDataMutex;
                
                leveldb::Cache::Handle* pInsertHandle = pRealtimeTrafficDataCache->Insert(strELinkID, pRealtimeData, 1, &TrafficIndex::deleteRealtimeCache);
                pRealtimeTrafficDataCache->Release(pInsertHandle);
            }else{
                pthread_mutex_lock(&(pTrafficIndex->m_TrafficDataMutex));
                
                RealtimeTraffic* pRealtimeData = (RealtimeTraffic*)pRealtimeTrafficDataCache->Value(pLookupHandle);
                if (pRealtimeData != NULL) {
                    vector<trafficInfo>* pVecElement = pRealtimeData->pVecElement;
                    if (pVecElement != NULL) {
                        if (pVecElement->size() == 8){
                            vector<trafficInfo>::iterator iter = pVecElement->begin();
                            pVecElement->erase(iter);
                        }
                        pVecElement->push_back(info);
                    }
                }

                pRealtimeTrafficDataCache->Release(pLookupHandle);
                pthread_mutex_unlock(&(pTrafficIndex->m_TrafficDataMutex));
            }
        }
    }
    
    delete []szDstBuffer;
    return NULL;
}
void TrafficIndex::deleteRealtimeCache(const leveldb::Slice& key, void* value){
    RealtimeTraffic* pRealtimeData = (RealtimeTraffic*)value;
    if (pRealtimeData != NULL) {
        pthread_mutex_lock(&(pRealtimeData->trafficDataMutex));
        vector<trafficInfo>* pVecElement = pRealtimeData->pVecElement;
        if (pVecElement != NULL) {
            delete pVecElement;
            pVecElement = NULL;
        }
        pthread_mutex_unlock(&(pRealtimeData->trafficDataMutex));
        delete pRealtimeData;
        pRealtimeData = NULL;
    }
}
void TrafficIndex::deleteFreeflowAndWeightCache(const leveldb::Slice& key, void* value){
    WeightAndFreeflow* pWeightAndFreeflow = (WeightAndFreeflow*)value;
    if (pWeightAndFreeflow != NULL) {
        pthread_mutex_lock(&(pWeightAndFreeflow->weightAndFreeflowMutex));
        double* pValues = pWeightAndFreeflow->pValues;
        if (pValues != NULL){
            delete []pValues;
            pValues = NULL;
        }
        pthread_mutex_unlock(&(pWeightAndFreeflow->weightAndFreeflowMutex));
        delete pWeightAndFreeflow;
        pWeightAndFreeflow = NULL;
    }
}
leveldb::Cache* TrafficIndex::getRealtimeTrafficDataCache(){
    return m_pRealtimeTrafficDataCache;
}

