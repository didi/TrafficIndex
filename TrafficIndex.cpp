#include "TrafficIndex.h"

TrafficIndex::TrafficIndex()
{
    m_pPGConnSyncPredefine = NULL;

    m_pPQWeightAndFreeflow = NULL;
    m_pLogUlits = NULL;
    m_strLinkVersion = "";
    m_strWeightAndFreeflowVersion = "";
    
    m_pFreeflowAndWeightCache = leveldb::NewLRUCache(100000000);
    m_pRealtimeTrafficDataCache = leveldb::NewLRUCache(100000000);
    
    m_strNodeType = "";
    m_pUpdateLinkMySQL = NULL;
    m_pUpdateLinkPG = NULL;
    m_pComputePredefineMySQL = NULL;
    m_pComputeCustomMySQL = NULL;
    m_pSubscribePG = NULL;

    m_strHostName = "";
    
    m_pRedisConn = NULL;
    m_pMQRedisConn = NULL;
    m_pSubscribeMySQL = NULL;
}


TrafficIndex::~TrafficIndex()
{
    releaseResource();
}

void TrafficIndex::releaseResource()
{
    if (m_pComputePredefineMySQL != NULL){
        mysql_close(m_pComputePredefineMySQL);
        m_pComputePredefineMySQL = NULL;
    }
    
    if (m_pComputeCustomMySQL != NULL){
        mysql_close(m_pComputeCustomMySQL);
        m_pComputeCustomMySQL = NULL;
    }
    
    if (m_pUpdateLinkMySQL != NULL){
        mysql_close(m_pUpdateLinkMySQL);
        m_pUpdateLinkMySQL = NULL;
    }

    
    if (m_pSubscribeMySQL != NULL){
        mysql_close(m_pSubscribeMySQL);
        m_pSubscribeMySQL = NULL;
    }
    
    if (m_pPQWeightAndFreeflow != NULL) {
        PQfinish(m_pPQWeightAndFreeflow);
        m_pPQWeightAndFreeflow = NULL;
    }
    
    if (m_pUpdateLinkPG != NULL) {
        PQfinish(m_pUpdateLinkPG);
        m_pUpdateLinkPG = NULL;
    }
    
    if (m_pSubscribePG != NULL) {
        PQfinish(m_pSubscribePG);
        m_pSubscribePG = NULL;
    }
    
    
    if (m_pRedisConn != NULL) {
        redisFree(m_pRedisConn);
        m_pRedisConn = NULL;
    }
    
    if (m_pMQRedisConn != NULL) {
        redisFree(m_pMQRedisConn);
        m_pMQRedisConn = NULL;
    }

    m_vecProvince.clear();
    m_mapPredefineObjList.clear();
    m_mapTrafficData.clear();
    
    if (m_pFreeflowAndWeightCache != NULL) {
        delete m_pFreeflowAndWeightCache;
        m_pFreeflowAndWeightCache = NULL;
    }
    
    if (m_pRealtimeTrafficDataCache != NULL) {
        delete m_pRealtimeTrafficDataCache;
        m_pRealtimeTrafficDataCache = NULL;
    }
    
}

static size_t writeData(void *pData, size_t nSize, size_t nMemSize, void *pStream)
{
    string *pAccepString = (string*)pStream;
    (*pAccepString).append((char*)pData, nSize * nMemSize);
    return nSize * nMemSize;
}

MYSQL* TrafficIndex::getMySQLConn()
{
    MYSQL* pMySQL = mysql_init(NULL);
    
    if (pMySQL == NULL)
    {
        return NULL;
    }
    
    char value = 1;
    mysql_options(pMySQL, MYSQL_OPT_RECONNECT, &value);
    
    if (mysql_real_connect(pMySQL, m_mySQLInfo.strHost.c_str(), m_mySQLInfo.strUser.c_str(),m_mySQLInfo.strPassword.c_str(),
                           m_mySQLInfo.strDBName.c_str(), atoi(m_mySQLInfo.strPort.c_str()), NULL, NULL) == NULL)
    {
        return NULL;
    }
    
    mysql_query(pMySQL, "SET NAMES UTF8");
    
    return pMySQL;
}

PGconn *TrafficIndex::getPGConn()
{
    char szConnectInfo[1024] = {0};
    sprintf(szConnectInfo, "host=%s port=%s dbname=%s user=%s password=%s",m_pgInfo.strHost.c_str(),m_pgInfo.strPort.c_str(),m_pgInfo.strDBName.c_str(),m_pgInfo.strUser.c_str(),m_pgInfo.strPassword.c_str());
    
    PGconn *pPGConn = PQconnectdb(szConnectInfo);
    
    ConnStatusType connStatus = PQstatus(pPGConn);
    
    if (connStatus != CONNECTION_OK)
    {
        string strErr = "连接Postgresql失败，错误信息：";
        strErr += PQerrorMessage(pPGConn);
        m_pLogUlits->AppendMsg(strErr.c_str());
        PQfinish(pPGConn);
        return NULL;
    }
    
    return pPGConn;
}

redisContext* TrafficIndex::getRedisConn()
{
    struct timeval timeout = {5, 500000}; // 5.5 seconds
    redisContext* pRedisConn = redisConnectWithTimeout(m_redisInfo.strHost.c_str(), atoi(m_redisInfo.strPort.c_str()), timeout);
    
    if(pRedisConn == NULL || pRedisConn->err )
    {
        if (pRedisConn != NULL)
        {
            string strErr = "连接Redis失败，错误信息：";
            strErr += pRedisConn->errstr;
            m_pLogUlits->AppendMsg(strErr.c_str());
            redisFree(pRedisConn);
            pRedisConn = NULL;
        }
        else
        {
            string strErr = "初始化Redis失败";
            m_pLogUlits->AppendMsg(strErr.c_str());
        }
        
        return NULL;
    }
    
    redisReply* pAuthReply = (redisReply *)redisCommand(pRedisConn, "auth %s", m_redisInfo.strPassword.c_str());
    if (pAuthReply->type == REDIS_REPLY_ERROR){
        freeReplyObject(pAuthReply);
        redisFree(pRedisConn);
        m_pLogUlits->AppendMsg("认证redis失败");
        return NULL;
    }
    
    freeReplyObject(pAuthReply);
    
    return pRedisConn;
}

bool TrafficIndex::connectDatabase()
{
    m_pUpdateLinkMySQL = getMySQLConn();
    m_pComputePredefineMySQL = getMySQLConn();
    m_pComputeCustomMySQL = getMySQLConn();
    m_pSubscribeMySQL = getMySQLConn();
//    m_pAddCustomMySQL = getMySQLConn();
//    m_pDelCustomMySQL = getMySQLConn();
    
    m_pPQWeightAndFreeflow = getPGConn();
    m_pUpdateLinkPG = getPGConn();
    m_pSubscribePG = getPGConn();
    
    m_pRedisConn = getRedisConn();
    
    m_pMQRedisConn = getRedisConn();
    
    if (m_pUpdateLinkMySQL == NULL || m_pComputePredefineMySQL == NULL || m_pComputeCustomMySQL == NULL || m_pSubscribeMySQL == NULL
        || m_pPQWeightAndFreeflow == NULL || m_pUpdateLinkPG == NULL || m_pSubscribePG == NULL || m_pRedisConn == NULL || m_pMQRedisConn == NULL)
    {
        m_pLogUlits->AppendMsg("初始化数据库连接对象失败");
        return false;
    }
    
    return true;
}

void TrafficIndex::getTTILinks(){
    double dfCount = objectCountPerNode(m_pUpdateLinkMySQL);
    
    if (dfCount == 0) {
        m_pLogUlits->AppendMsg("获取计算节点的对象总数失败");
        return;
    }
    
    if (m_strNodeType == "master") {
        int nRes = mysql_query(m_pUpdateLinkMySQL, "select obj_id,astext(geom),attibute_filter from traffic_obj_info where obj_type in (1,2);");
//        int nRes = mysql_query(m_pUpdateLinkMySQL, "select obj_id,astext(geom) from traffic_obj_info where obj_type in (1,2) and obj_id = 2 order by obj_id asc;");
        
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
                            vector<string> vecELinkIDs;
                            queryELinkID(m_pUpdateLinkPG,pszGeom,vecELinkIDs,pszFilter);
                            
                            if (!vecELinkIDs.empty()) {
                                m_mapPredefineObjList[atoi(pszObjID)] = vecELinkIDs;
                            }else{
                                char szMsg[512] = {0};
                                sprintf(szMsg, "未获取到links,obj_id=%s",pszObjID);
                                m_pLogUlits->AppendMsg(szMsg);
                            }
                        }
                    }
                }
                mysql_free_result(pRes);
            }
        }
    }
    
    //自定义对象
    int nRes = mysql_query(m_pUpdateLinkMySQL, "select obj_id,astext(geom),attibute_filter from traffic_obj_info where obj_type <> 1 and obj_type <> 2;");
//    int nRes = mysql_query(m_pUpdateLinkMySQL, "select obj_id,astext(geom) from traffic_obj_info where obj_type = 4 and city = '深圳市' order by obj_id asc;");
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
                        vector<string> vecELinkIDs;
                        queryELinkID(m_pUpdateLinkPG,pszGeom,vecELinkIDs,pszFilter);
                        
                        if (!vecELinkIDs.empty()) {
                            if (jobIsRunning(m_pRedisConn, atoi(pszObjID))){
                                continue;
                            }
                            
                            m_mapCustomObjList[atoi(pszObjID)] = vecELinkIDs;
                            
                            if (m_mapCustomObjList.size() >= (int)dfCount) {
                                break;
                            }
                        }else{
                            char szMsg[512] = {0};
                            sprintf(szMsg, "未获取到links,obj_id=%s",pszObjID);
                            m_pLogUlits->AppendMsg(szMsg);
                        }
                    }
                }
            }
            mysql_free_result(pRes);
        }
    }
}

void TrafficIndex::queryELinkID(PGconn* pConn,string strWKT,vector<string>& vecELinkIDs,string strFilter)
{
    if (pConn == NULL || m_strLinkVersion == "") {
        return;
    }
    
    string strSQL = "";
    string::size_type idx_1 = strWKT.find("LINE");
    
    if (idx_1 != string::npos) {
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
    
    if (PQresultStatus(pGRes) != PGRES_TUPLES_OK)
    {
        string strErr = "未获取到link数据，错误信息：";
        strErr += PQerrorMessage(pConn);
        m_pLogUlits->AppendMsg(strErr.c_str());
        PQclear(pGRes);
        return;
    }
    
    int nRow = PQntuples(pGRes);
    
    for(int i = 0; i < nRow;i++){
        const char* pszID = PQgetvalue(pGRes,i,0);
        const char* pszDirection = PQgetvalue(pGRes,i,1);
        
        string strID = pszID;
        int nDirection = atoi(pszDirection);
        switch (nDirection)
        {
            case 0:
                strID.insert(strID.length(), "0");
                vecELinkIDs.push_back(strID);
                
                strID = pszID;
                strID.insert(strID.length(), "1");
                vecELinkIDs.push_back(strID);
                break;
            case 1:
                strID.insert(strID.length(), "0");
                vecELinkIDs.push_back(strID);
                
                strID = pszID;
                strID.insert(strID.length(), "1");
                vecELinkIDs.push_back(strID);
                break;
            case 2:
                strID.insert(strID.length(), "0");
                vecELinkIDs.push_back(strID);
                break;
            case 3:
                strID.insert(strID.length(), "1");
                vecELinkIDs.push_back(strID);
                break;
            default:
                break;
        }
    }
    
    PQclear(pGRes);
}

void TrafficIndex::queryELinkIDs(PGconn* pConn,string &strWKB,vector<string>& vecELinkIDs,int nObjType)
{
    if (pConn == NULL || m_strLinkVersion == "") {
        return;
    }
    
    string strSQL = "select link_id,direction from ";
    strSQL += m_strLinkVersion;
    strSQL += "_link where st_intersects";
    strSQL += "(geom,'";
    strSQL += strWKB;
    strSQL += "')";
    
    if (nObjType == 5)
    {
        strSQL = "select link_id,direction from ";
        strSQL += m_strLinkVersion;
        strSQL += "_link where st_contains(st_buffer('";
        strSQL += strWKB;
        strSQL += "',0.00001),geom);";
    }
    
    PGresult* pGRes = PQexec(pConn, strSQL.c_str());
    
    if (PQresultStatus(pGRes) != PGRES_TUPLES_OK)
    {
        string strErr = "未获取到link数据，错误信息：";
        strErr += PQerrorMessage(pConn);
        m_pLogUlits->AppendMsg(strErr.c_str());
        PQclear(pGRes);
        return;
    }
    
    int nRow = PQntuples(pGRes);

    for(int i = 0; i < nRow;i++)
    {
        const char* pszID = PQgetvalue(pGRes,i,0);
        const char* pszDirection = PQgetvalue(pGRes,i,1);
        
        string strID = pszID;
        int nDirection = atoi(pszDirection);
        switch (nDirection)
        {
            case 0:
                strID.insert(strID.length(), "0");
                vecELinkIDs.push_back(strID);
                
                strID = pszID;
                strID.insert(strID.length(), "1");
                vecELinkIDs.push_back(strID);
                break;
            case 1:
                strID.insert(strID.length(), "0");
                vecELinkIDs.push_back(strID);
                
                strID = pszID;
                strID.insert(strID.length(), "1");
                vecELinkIDs.push_back(strID);
                break;
            case 2:
                strID.insert(strID.length(), "0");
                vecELinkIDs.push_back(strID);
                break;
            case 3:
                strID.insert(strID.length(), "1");
                vecELinkIDs.push_back(strID);
                break;
            default:
                break;
        }
    }
    
    PQclear(pGRes);
}
void TrafficIndex::computeLinkSetTTI(time_t nBatchTime,int nObj_id,vector<string>& vecLinkSet,double& dfTTI,double& dfSpd){
    double dfTTIFenzi = 0;
    double dfTTIFenmu = 0;
    
    double dfSpdFenzi = 0;
    double dfSpdFenmu = 0;
    
//    double dfLength_1 = 0;
//    double dfLength_2 = 0;
    
    for (size_t i = 0; i < vecLinkSet.size(); i++){
        string strELinkID = vecLinkSet[i];
        
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

        pthread_mutex_lock(&m_traffic_data_mutex);
        
        vector<trafficInfo> vecElement = *(vector<trafficInfo>*)m_pRealtimeTrafficDataCache->Value(pLookupHandle);
    
        for (int j = 0; j < vecElement.size(); j++){
            trafficInfo currentTraffic = vecElement[j];
            
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
        }
        m_pRealtimeTrafficDataCache->Release(pLookupHandle);
        
        pthread_mutex_unlock(&m_traffic_data_mutex);
    }
    
    if (dfTTIFenmu != 0 && dfTTIFenzi != 0){
        dfTTI = dfTTIFenzi / dfTTIFenmu;
    }
    
    if (dfSpdFenzi != 0 && dfSpdFenmu != 0){
        dfSpd = dfSpdFenzi / dfSpdFenmu;
    }
}

int TrafficIndex::httpGet(string & strURL, std::string & strResponse,int nTimeout ,const char * pCaPath)
{
    strResponse = "";
    CURL* pCurl = curl_easy_init();
    
    if (pCurl == NULL) {
        return CURLE_FAILED_INIT;
    }
    
    curl_easy_setopt(pCurl, CURLOPT_URL, strURL.c_str());
    curl_easy_setopt(pCurl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(pCurl, CURLOPT_TIMEOUT, nTimeout);
    curl_easy_setopt(pCurl, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(pCurl, CURLOPT_WRITEFUNCTION, writeData);
    curl_easy_setopt(pCurl, CURLOPT_WRITEDATA, (void*)&strResponse);
    CURLcode res = curl_easy_perform(pCurl);
    curl_easy_cleanup(pCurl);
    return res;
}

void deleteRealtimeCache(const leveldb::Slice& key, void* value)
{
    vector<trafficInfo>* pVecElement = (vector<trafficInfo>*)value;
    if (pVecElement != NULL)
    {
        delete pVecElement;
        pVecElement = NULL;
    }
}

void TrafficIndex::getTrafficDataFromServer(const char* pszCityCode,time_t nCurrentTime)
{
    if (pszCityCode == NULL) {
        return;
    }
    
    char szUser_pwd[128] = { 0 };
    string strPassword = "your password";
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
    pthread_mutex_lock(&m_pulish_url_mutex);
    string strURL = m_strTrafficPublicURL;
    pthread_mutex_unlock(&m_pulish_url_mutex);
    strURL += "/traffic-dlr/getData?vid=";
#endif
    
    strURL += strMD5;
    strURL += "&citycode=";
    strURL += pszCityCode;
    strURL += "&ts=";
    strURL += szTime;
    strURL += "&username=Calc-TTI";
    
    string strResponse;
    int nRes = httpGet(strURL, strResponse,300);
    size_t nSrcLength = strResponse.length();
    
    if (nSrcLength == 0){
        string strMsg = "未获取到路况数据，请求地址:";
        strMsg += strURL;
        strMsg += ",错误码:";
        strMsg += CommonTools::toString(nRes);
        m_pLogUlits->AppendMsg(strMsg.c_str());
        return;
    }

    unsigned char *szSrcBuffer = new unsigned char[nSrcLength];
    for (int i = 0; i< nSrcLength; i++){
        szSrcBuffer[i] = strResponse[i];
    }

    uint32_t nDstLength = 1024 * 1024 * 60;
    unsigned char* szDstBuffer = new unsigned char[nDstLength];
    
    CommonTools::decoderGz(szSrcBuffer,(uint32_t)nSrcLength,szDstBuffer,nDstLength);
    
    its::service::TrafficData trafficData;
    
    if (!trafficData.ParseFromArray(szDstBuffer, nDstLength)){
        string strMsg = "解析pb失败,请求地址:";
        strMsg += strURL;
        m_pLogUlits->AppendMsg(strMsg.c_str());
        delete []szSrcBuffer;
        delete []szDstBuffer;
        return;
    }
    
    int nLinkSize = trafficData.flow().link_arr_size();
    
    if (nLinkSize == 0){
        string strMsg = "link_arr_size=0,请求地址:";
        strMsg += strURL;
        m_pLogUlits->AppendMsg(strMsg.c_str());
        delete []szSrcBuffer;
        delete []szDstBuffer;
        return;
    }
    
    ::google::protobuf::uint32 nBatch_time = trafficData.meta().batch_time();
    string strBatchTime = CommonTools::unix2Standard(nBatch_time);

    for (int i = 0; i < nLinkSize; i++){
        unsigned int nConfidence = trafficData.flow().confidence_arr(i);
        
        if (nConfidence >= 20){
            long long lLinkID = trafficData.flow().link_arr().Get(i);
            string strELinkID = CommonTools::toString(lLinkID);
    
            unsigned int nLength = trafficData.flow().length_arr(i);
            
            unsigned int nSpeed = trafficData.flow().speed_without_light_arr(i);
//            unsigned int nSpeed = trafficData.flow().speed_with_light_arr(i);
            double dfSpeed = (double)nSpeed / 10.0;
            
            if (dfSpeed == 0) {
                dfSpeed = 0.001;
            }
            
            trafficInfo info;
            info.dfLength = (double)nLength / 1000.0;
            info.strELinkID = strELinkID;
            
#ifdef __APPLE__
            info.strBatchTime = strBatchTime;
#endif
            info.dfSpeed = dfSpeed;
            info.nBatchTime = nBatch_time;
            
            leveldb::Cache::Handle* pLookupHandle = m_pRealtimeTrafficDataCache->Lookup(strELinkID);
            if (pLookupHandle == NULL){
                vector<trafficInfo>* pVecElement = new vector<trafficInfo>();
                pVecElement->push_back(info);
                
                leveldb::Cache::Handle* pInsertHandle = m_pRealtimeTrafficDataCache->Insert(strELinkID, pVecElement, 1, deleteRealtimeCache);
                m_pRealtimeTrafficDataCache->Release(pInsertHandle);
            }else{
                pthread_mutex_lock(&m_traffic_data_mutex);
                
                vector<trafficInfo>* pVecElement = (vector<trafficInfo>*)m_pRealtimeTrafficDataCache->Value(pLookupHandle);
                
                size_t nSize = pVecElement->size();
                
                if (nSize == 12){
                    vector<trafficInfo>::iterator iter = pVecElement->begin();
                    pVecElement->erase(iter);
                }
                
                pVecElement->push_back(info);
 
                m_pRealtimeTrafficDataCache->Release(pLookupHandle);
                pthread_mutex_unlock(&m_traffic_data_mutex);
            }
        }
    }

    delete []szSrcBuffer;
    delete []szDstBuffer;
}

void TrafficIndex::setProvinceList(vector<string>& vecProvince) {
    m_vecProvince = vecProvince;
}

void TrafficIndex::setTrafficPublicURL(string& strTrafficPublicURL) { 
    m_strTrafficPublicURL = strTrafficPublicURL;
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

void TrafficIndex::setLogUlits(LogUlits* pLogUlits){
    m_pLogUlits = pLogUlits;
}

LogUlits *TrafficIndex::getLogUlits() { 
    return m_pLogUlits;
}

void TrafficIndex::setTrafficDataMutex(pthread_mutex_t& traffic_data_mutex) { 
    m_traffic_data_mutex = traffic_data_mutex;
}

void TrafficIndex::setPredefineObjMutex(pthread_mutex_t& predefine_obj_mutex) {
    m_predefine_obj_mutex = predefine_obj_mutex;
}

void TrafficIndex::getFreeflowAndWeight(string& strELink,double& dfFreeflow,double& dfWeight)
{
    dfFreeflow = 0;
    dfWeight = 0;
    
    if (m_pFreeflowAndWeightCache == NULL) {
        return;
    }
    
    leveldb::Cache::Handle* pHandle = m_pFreeflowAndWeightCache->Lookup(strELink);
    
    if (pHandle == NULL) {
        return;
    }
    double* pValues = (double*)m_pFreeflowAndWeightCache->Value(pHandle);
    dfWeight = pValues[0];
    dfFreeflow = pValues[1];
    m_pFreeflowAndWeightCache->Release(pHandle);
}

void TrafficIndex::computeTTI(string strBatch_time,int nType){
    if (nType == 1){
        pthread_mutex_lock(&m_predefine_obj_mutex);
        computeObjTTI(m_pComputePredefineMySQL,strBatch_time,m_mapPredefineObjList,nType);
        pthread_mutex_unlock(&m_predefine_obj_mutex);
    }else{
        pthread_mutex_lock(&m_custom_obj_mutex);
        computeObjTTI(m_pComputeCustomMySQL,strBatch_time,m_mapCustomObjList,nType);
        pthread_mutex_unlock(&m_custom_obj_mutex);
    }
}

void TrafficIndex::computeObjTTI(MYSQL* pMySQL,string strBatch_time,map<int,vector<string> >& mapObjList,int nType)
{
    char szMsg[128] = {0};
    sprintf(szMsg, "%d mapObjList.size = %ld",nType,mapObjList.size());
    m_pLogUlits->AppendMsg(szMsg);
    
    if (mapObjList.empty())
    {
        return;
    }
    
    time_t nBatch_time = CommonTools::standard2Unix(strBatch_time.c_str());
    
    string strSQL = "replace into obj_tti_detail (obj_id,batch_time,update_time,tti,avgspd) values ";
    
    size_t nLength = strSQL.length();
    
    int nInsertCount = 0;
    
    map<int,vector<string> >::iterator iter = mapObjList.begin();
    for (; iter != mapObjList.end(); iter++){
        int nObjID = iter->first;
        
        if (iter->second.empty()){
            continue;
        }
        
        double dfTTI = 0;
        double dfSpd = 0;
        
        computeLinkSetTTI(nBatch_time,nObjID,iter->second,dfTTI,dfSpd);
        
        if (dfTTI != 0 && dfSpd != 0){
            nInsertCount++;
            
            char szValues[512] = {0};
            sprintf(szValues, "(%d,'%s',now(),%f,%f),",nObjID,strBatch_time.c_str(),dfTTI,dfSpd);
            strSQL += szValues;
            
            if (nInsertCount % 500 == 0){
                strSQL = strSQL.substr(0,strSQL.length() - 1);
                
                int nPingRes = mysql_ping(pMySQL);
                if (nPingRes != 0){
                    string strMsg = "重连sql失败，失败信息：";
                    strMsg += mysql_error(pMySQL);
                    m_pLogUlits->AppendMsg(strMsg.c_str());
                    
                    strSQL = "replace into obj_tti_detail (obj_id,batch_time,update_time,tti,avgspd) values ";
                    continue;
                }
                
                int nRes = mysql_query(pMySQL, strSQL.c_str());
                
                if (nRes != 0){
                    string strMsg = "写入TTI结果失败，失败信息：";
                    strMsg += mysql_error(pMySQL);
                    strMsg += ",SQL";
                    strMsg += strSQL;
                    m_pLogUlits->AppendMsg(strMsg.c_str());
                }
                
                strSQL = "replace into obj_tti_detail (obj_id,batch_time,update_time,tti,avgspd) values ";
            }
            
            char szRealValus[512] = {0};
            sprintf(szRealValus, "replace into obj_tti_realtime (obj_id,tti,avgspd,batch_time,update_time) values(%d,%f,%f,'%s',now())",nObjID,dfTTI,dfSpd,strBatch_time.c_str());
            
            mysql_ping(pMySQL);
            int nRes = mysql_query(pMySQL, szRealValus);
            
            if (nRes != 0){
                string strMsg = "写入实时TTI失败，失败信息：";
                strMsg += mysql_error(pMySQL);
                strMsg += ",SQL";
                strMsg += szRealValus;
                m_pLogUlits->AppendMsg(strMsg.c_str());
            }
        }
    }
    
    if (strSQL.length() > nLength){
        strSQL = strSQL.substr(0,strSQL.length() - 1);
        
        int nPingRes = mysql_ping(pMySQL);
        if (nPingRes != 0){
            string strMsg = "重连sql失败，失败信息：";
            strMsg += mysql_error(pMySQL);
            m_pLogUlits->AppendMsg(strMsg.c_str());
            return;
        }
        
        int nRes = mysql_query(pMySQL, strSQL.c_str());
        
        if (nRes != 0){
            string strMsg = "写入TTI结果失败，失败信息：";
            strMsg += mysql_error(pMySQL);
            strMsg += ",SQL";
            strMsg += strSQL;
            m_pLogUlits->AppendMsg(strMsg.c_str());
        }
    }
}

void deleteFreeflowAndWeightCache(const leveldb::Slice& key, void* value)
{
    double* pValues = (double*)value;
    if (pValues != NULL)
    {
        delete []pValues;
        pValues = NULL;
    }
}

void TrafficIndex::getWeightAndFreeflow(){
    string strSQL = "select elink_id,weight,freeflow from ";
    strSQL += m_strWeightAndFreeflowVersion;
    PGresult* pGRes = PQexec(m_pPQWeightAndFreeflow, strSQL.c_str());
    
    if (PQresultStatus(pGRes) != PGRES_TUPLES_OK){
        string strErr = "未获取到权重自由流数据，错误信息：";
        strErr += PQerrorMessage(m_pPQWeightAndFreeflow);
        m_pLogUlits->AppendMsg(strErr.c_str());
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
            leveldb::Cache::Handle* pInsertHandle = m_pFreeflowAndWeightCache->Insert(strELinkID, pValues, 1, deleteFreeflowAndWeightCache);
            m_pFreeflowAndWeightCache->Release(pInsertHandle);
        }else{
            pthread_mutex_lock(&m_update_weight_freeflow_mutex);
            
            double* pValues = (double*)m_pFreeflowAndWeightCache->Value(pLookupHandle);
            pValues[0] = atof(pszWeight);
            pValues[1] = atof(pszFreeflow);
            m_pFreeflowAndWeightCache->Release(pLookupHandle);
            
            pthread_mutex_unlock(&m_update_weight_freeflow_mutex);
        }
    }
    
    PQclear(pGRes);
}

void TrafficIndex::updateWeightAndFreeflow(){
    string strWeightAndFreeflowVersion = getFeatureVersion("weight_freeflow",m_pPQWeightAndFreeflow);
    if (strWeightAndFreeflowVersion == "") {
        return;
    }
    
    if (strWeightAndFreeflowVersion != m_strWeightAndFreeflowVersion) {
        char szMsg[512] = {0};
        sprintf(szMsg, "更新自由流,旧版本:%s,新版本:%s",m_strWeightAndFreeflowVersion.c_str(),strWeightAndFreeflowVersion.c_str());
        m_pLogUlits->AppendMsg(szMsg);
        
        getWeightAndFreeflow();
        
        m_strWeightAndFreeflowVersion = strWeightAndFreeflowVersion;
        m_pLogUlits->AppendMsg("更新自由流结束");
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
        m_pLogUlits->AppendMsg(szMsg);
        
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
                            int nObjID = atoi(pszObjID);
                            
                            pthread_mutex_lock(&m_predefine_obj_mutex);
                            map<int,vector<string> >::iterator iter = m_mapPredefineObjList.find(nObjID);
                            if (iter != m_mapPredefineObjList.end()) {
                                vector<string> vecELinkIDs;
                                queryELinkID(m_pUpdateLinkPG,pszGeom,vecELinkIDs,pszFilter);
                                if (!vecELinkIDs.empty()) {
                                    m_mapPredefineObjList[nObjID] = vecELinkIDs;
                                }else{
                                    char szMsg[512] = {0};
                                    sprintf(szMsg, "未获取到links,obj_id=%s",pszObjID);
                                    m_pLogUlits->AppendMsg(szMsg);
                                }
                            }
                            pthread_mutex_unlock(&m_predefine_obj_mutex);
                            
                            pthread_mutex_lock(&m_custom_obj_mutex);
                            map<int,vector<string> >::iterator iter_2 = m_mapCustomObjList.find(nObjID);
                            if (iter_2 != m_mapCustomObjList.end()) {
                                vector<string> vecELinkIDs;
                                queryELinkID(m_pUpdateLinkPG,pszGeom,vecELinkIDs,pszFilter);
                                if (!vecELinkIDs.empty()) {
                                    m_mapCustomObjList[nObjID] = vecELinkIDs;
                                }else{
                                    char szMsg[512] = {0};
                                    sprintf(szMsg, "未获取到links,obj_id=%s",pszObjID);
                                    m_pLogUlits->AppendMsg(szMsg);
                                }
                            }
                            pthread_mutex_unlock(&m_custom_obj_mutex);
                        }
                    }
                }
                mysql_free_result(pRes);
            }
        }
        
        m_strLinkVersion = strLinkVersion;
        m_pLogUlits->AppendMsg("更新路网结束");
    }
}


void TrafficIndex::convertToELinkIDs(map<string,string>& mapDirection, string strLinkID, vector<string> &vecELinkIDs)
{
    map<string,string>::iterator iter = mapDirection.find(strLinkID);
    if (iter == mapDirection.end()) {
        return;
    }
    
    string strID = strLinkID;
    int nDirection = atoi(iter->second.c_str());
    switch (nDirection)
    {
        case 0:
            strID.insert(strID.length(), "0");
            vecELinkIDs.push_back(strID);
            
            strID = strLinkID;
            strID.insert(strID.length(), "1");
            vecELinkIDs.push_back(strID);
            break;
        case 1:
            strID.insert(strID.length(), "0");
            vecELinkIDs.push_back(strID);
            
            strID = strLinkID;
            strID.insert(strID.length(), "1");
            vecELinkIDs.push_back(strID);
            break;
        case 2:
            strID.insert(strID.length(), "0");
            vecELinkIDs.push_back(strID);
            break;
        case 3:
            strID.insert(strID.length(), "1");
            vecELinkIDs.push_back(strID);
            break;
        default:
            break;
    }
}

void TrafficIndex::setUpdateWeightAndFreeflowMutex(pthread_mutex_t &update_weight_freeflow_mutex) { 
    m_update_weight_freeflow_mutex = update_weight_freeflow_mutex;
}

void TrafficIndex::setPublishURLMutex(pthread_mutex_t& pulish_url_mutex){
    m_pulish_url_mutex = pulish_url_mutex;
}

void TrafficIndex::setCustomObjMutex(pthread_mutex_t& custom_obj_mutex){
    m_custom_obj_mutex = custom_obj_mutex;
}

void TrafficIndex::aggreTTT(int nType,time_t nCurrentTime){
    MYSQL* pMySQL = mysql_init(NULL);
    
    if (pMySQL == NULL){
        m_pLogUlits->AppendMsg("初始化聚合mysql对象失败");
        return;
    }
    
    char value = 1;
    mysql_options(pMySQL, MYSQL_OPT_RECONNECT, &value);
    
    if (mysql_real_connect(pMySQL, m_mySQLInfo.strHost.c_str(), m_mySQLInfo.strUser.c_str(),m_mySQLInfo.strPassword.c_str(),
                           m_mySQLInfo.strDBName.c_str(), atoi(m_mySQLInfo.strPort.c_str()), NULL, NULL) == NULL){
        string strErr = "连接聚合MySQL失败，错误信息：";
        strErr += mysql_error(pMySQL);
        m_pLogUlits->AppendMsg(strErr.c_str());
        mysql_close(pMySQL);
        return;
    }
    
    mysql_query(pMySQL, "SET NAMES UTF8");

    nCurrentTime -= 900;
    
    vector<string> vecFailedSQL;
    
    if (nType == 1){
        time_t nStart = nCurrentTime - 60 * 60;
        string strStart = CommonTools::unix2Standard(nStart);
        string strEnd = CommonTools::unix2Standard(nCurrentTime);

        char szInsertSQL[1024] = {0};
        sprintf(szInsertSQL, "replace into obj_tti_hourly (obj_id,tti,avgspd,batch_time) select obj_id,avg(tti),avg(avgspd),'%s' from obj_tti_detail where batch_time >= '%s' and batch_time < '%s' group by obj_id;",strEnd.c_str(),strStart.c_str(),strEnd.c_str());
        
        mysql_ping(pMySQL);
        int nRes = mysql_query(pMySQL, szInsertSQL);
        
        if (nRes != 0){
            string strErr = "写入小时表失败，错误信息：";
            strErr += mysql_error(pMySQL);
            strErr += ",sql:";
            strErr += szInsertSQL;
            m_pLogUlits->AppendMsg(strErr.c_str());
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
        
        mysql_ping(pMySQL);
        int nRes = mysql_query(pMySQL, szInsertSQL_1);
        
        if (nRes != 0){
            string strErr = "写入天表失败，错误信息：";
            strErr += mysql_error(pMySQL);
            strErr += ",sql:";
            strErr += szInsertSQL_1;
            m_pLogUlits->AppendMsg(strErr.c_str());
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
        
        mysql_ping(pMySQL);
        nRes = mysql_query(pMySQL, szInsertSQL_2);
        
        if (nRes != 0){
            string strErr = "写入天表失败，错误信息：";
            strErr += mysql_error(pMySQL);
            strErr += ",sql:";
            strErr += szInsertSQL_2;
            m_pLogUlits->AppendMsg(strErr.c_str());
            vecFailedSQL.push_back(szInsertSQL_2);
        }
        
        ///////////
        char szStart_3[512] = {0};
        sprintf(szStart_3, "%s 07:30:00",strBatchTime.c_str());
        
        char szEnd_3[512] = {0};
        sprintf(szEnd_3, "%s 09:30:00",strBatchTime.c_str());
        
        char szInsertSQL_3[1024] = {0};
        sprintf(szInsertSQL_3, "replace into obj_tti_daily (obj_id,tti,avgspd,batch_time,aggre_type) select obj_id,avg(tti),avg(avgspd),'%s 00:00:00',3 from obj_tti_detail where batch_time >= '%s' and batch_time < '%s' group by obj_id;",strBatchTime.c_str(),szStart_3,szEnd_3);
        
        mysql_ping(pMySQL);
        nRes = mysql_query(pMySQL, szInsertSQL_3);
        
        if (nRes != 0){
            string strErr = "写入天表失败，错误信息：";
            strErr += mysql_error(pMySQL);
            strErr += ",sql:";
            strErr += szInsertSQL_3;
            m_pLogUlits->AppendMsg(strErr.c_str());
            vecFailedSQL.push_back(szInsertSQL_3);
        }
        
        ///////////
        char szStart_4[512] = {0};
        sprintf(szStart_4, "%s 17:30:00",strBatchTime.c_str());
        
        char szEnd_4[512] = {0};
        sprintf(szEnd_4, "%s 19:30:00",strBatchTime.c_str());
        
        char szInsertSQL_4[1024] = {0};
        sprintf(szInsertSQL_4, "replace into obj_tti_daily (obj_id,tti,avgspd,batch_time,aggre_type) select obj_id,avg(tti),avg(avgspd),'%s 00:00:00',4 from obj_tti_detail where batch_time >= '%s' and batch_time < '%s' group by obj_id;",strBatchTime.c_str(),szStart_4,szEnd_4);
        
        mysql_ping(pMySQL);
        nRes = mysql_query(pMySQL, szInsertSQL_4);
        
        if (nRes != 0){
            string strErr = "写入天表失败，错误信息：";
            strErr += mysql_error(pMySQL);
            strErr += ",sql:";
            strErr += szInsertSQL_4;
            m_pLogUlits->AppendMsg(strErr.c_str());
            vecFailedSQL.push_back(szInsertSQL_4);
        }
        
        ///////////
        char szStart_5[512] = {0};
        sprintf(szStart_5, "%s 11:00:00",strBatchTime.c_str());
        
        char szEnd_5[512] = {0};
        sprintf(szEnd_5, "%s 16:00:00",strBatchTime.c_str());
        
        char szInsertSQL_5[1024] = {0};
        sprintf(szInsertSQL_5, "replace into obj_tti_daily (obj_id,tti,avgspd,batch_time,aggre_type) select obj_id,avg(tti),avg(avgspd),'%s 00:00:00',5 from obj_tti_detail where batch_time >= '%s' and batch_time < '%s' group by obj_id;",strBatchTime.c_str(),szStart_5,szEnd_5);
        
        mysql_ping(pMySQL);
        mysql_query(pMySQL, szInsertSQL_5);
        
        if (nRes != 0){
            string strErr = "写入天表失败，错误信息：";
            strErr += mysql_error(pMySQL);
            strErr += ",sql:";
            strErr += szInsertSQL_5;
            m_pLogUlits->AppendMsg(strErr.c_str());
            vecFailedSQL.push_back(szInsertSQL_5);
        }
        
        ///////////
        char szStart_6[512] = {0};
        sprintf(szStart_6, "%s 00:00:00",strBatchTime.c_str());
        
        char szEnd_6[512] = {0};
        sprintf(szEnd_6, "%s 23:59:59",strBatchTime.c_str());
        
        char szInsertSQL_6[1024] = {0};
        sprintf(szInsertSQL_6, "replace into obj_tti_daily (obj_id,tti,avgspd,batch_time,aggre_type) select obj_id,avg(tti),avg(avgspd),'%s 00:00:00',6 from obj_tti_detail where batch_time >= '%s' and batch_time <='%s' group by obj_id;",strBatchTime.c_str(),szStart_6,szEnd_6);
        
        mysql_ping(pMySQL);
        nRes = mysql_query(pMySQL, szInsertSQL_6);
        
        if (nRes != 0){
            string strErr = "写入天表失败，错误信息：";
            strErr += mysql_error(pMySQL);
            strErr += ",sql:";
            strErr += szInsertSQL_6;
            m_pLogUlits->AppendMsg(strErr.c_str());
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
            mysql_ping(pMySQL);
            int nRetryRes = mysql_query(pMySQL, (*iter).c_str());
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
        m_pLogUlits->AppendMsg(strMsg.c_str());
    }
    
    mysql_close(pMySQL);
}
void TrafficIndex::setNodeType(string strNodeType){
    m_strNodeType = strNodeType;
}

void TrafficIndex::setHostName(string& strHostName){
    m_strHostName = strHostName;
}

int TrafficIndex::getTTIObjectCount(MYSQL* pMySQL){
    int nResult = 0;
    
    if (pMySQL == NULL) {
        return nResult;
    }
    
    int nRes = mysql_query(pMySQL, "select count(*) from traffic_obj_info where obj_type <> 1 and obj_type <> 2;");
//    int nRes = mysql_query(pMySQL, "select count(*) from traffic_obj_info where obj_type = 4 and city = '深圳市'");
    
    if (nRes == 0) {
        MYSQL_RES* pRes = mysql_store_result(pMySQL);
        if (pRes != NULL) {
            MYSQL_ROW ppRecord;
            while ((ppRecord = mysql_fetch_row(pRes))){
                if (ppRecord[0] != NULL){
                    nResult = atoi(ppRecord[0]);
                }
            }
            mysql_free_result(pRes);
        }
    }

    return nResult;
}
    
int TrafficIndex::getConfigNodeCount(MYSQL* pMySQL){
    int nNodeCount = 0;
    if (pMySQL == NULL) {
        return nNodeCount;
    }
    
    int nRes = mysql_query(pMySQL, "select node_count from tti_config");
    
    if (nRes != 0) {
        return nNodeCount;
    }
    
    MYSQL_RES* pRes = mysql_store_result(pMySQL);
    
    if (pRes == NULL) {
        return nNodeCount;
    }
    
    MYSQL_ROW ppRecord;
    while ((ppRecord = mysql_fetch_row(pRes))){
        if (ppRecord[0] != NULL){
            nNodeCount = atoi(ppRecord[0]);
        }
    }
    mysql_free_result(pRes);
    
    return nNodeCount;
}

bool TrafficIndex::jobIsRunning(redisContext* pRedisConn,int nObj_ID)
{
    if (pRedisConn == NULL) {
        return true;
    }
    
    bool bFind = false;
    
    char szCommand[128] = {0};
    sprintf(szCommand, "setnx lock_obj_%d running",nObj_ID);
    redisReply* pReply = (redisReply*)redisCommand(pRedisConn,szCommand);
    
    if (pReply == NULL) {
        return true;
    }
    
    if (pReply->type == REDIS_REPLY_ERROR)
    {
        freeReplyObject(pReply);
        return true;
    }
    
    long long llResult = pReply->integer;
    
    if (llResult == 0)
    {
        bFind = true;
    }
    
    freeReplyObject(pReply);
    return bFind;
}

bool TrafficIndex::resetObjStatus(){
    if (m_pRedisConn == NULL || m_pUpdateLinkMySQL == NULL) {
        return false;
    }
    
    redisReply* pReply = (redisReply*)redisCommand(m_pRedisConn,"smembers node_list");
    if (pReply != NULL) {
        for (int i = 0; i < pReply->elements; i++) {
            const char* pszNodeName = pReply->element[i]->str;
            if (pszNodeName != NULL) {
                char szCommand[512] = {0};
                sprintf(szCommand, "del %s",pszNodeName);
                redisReply* pReply_1 = (redisReply*)redisCommand(m_pRedisConn,szCommand);
                freeReplyObject(pReply_1);
            }
        }
        freeReplyObject(pReply);
    }
    
    redisReply* pReply_2 = (redisReply*)redisCommand(m_pRedisConn,"del node_list");
    freeReplyObject(pReply_2);
    
    redisReply* pReply_5 = (redisReply*)redisCommand(m_pRedisConn,"del lock_redis");
    freeReplyObject(pReply_5);
    
    int nRes = mysql_query(m_pUpdateLinkMySQL, "select obj_id from traffic_obj_info where obj_type <> 1 and obj_type <> 2;");
    
    if (nRes == 0) {
        MYSQL_RES* pRes = mysql_store_result(m_pUpdateLinkMySQL);
        
        if (pRes != NULL) {
            MYSQL_ROW ppRecord;
            while ((ppRecord = mysql_fetch_row(pRes))){
                if (ppRecord[0] != NULL ){
                    char szCommand[512] = {0};
                    sprintf(szCommand, "del lock_obj_%s",ppRecord[0]);
                    
                    redisReply* pReply = (redisReply*)redisCommand(m_pRedisConn,szCommand);
                    freeReplyObject(pReply);
                }
            }
            mysql_free_result(pRes);
        }
    }
    
    return true;
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
        m_pLogUlits->AppendMsg(strErr.c_str());
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

int TrafficIndex::objectCountPerNode(MYSQL* pMySQL){
    double dfNodeCount = (double)getConfigNodeCount(pMySQL);
    
    if (dfNodeCount == 0) {
        return 0;
    }
    double dfTotalCount = (double)getTTIObjectCount(pMySQL);

    double dfObjNumsPerNode = ceil(dfTotalCount / dfNodeCount);
    
    char szMsg[512] = {0};
    sprintf(szMsg, "节点名称:%s,总量:%d,节点个数:%d,节点分配计算个数:%d",m_strHostName.c_str(),(int)dfTotalCount,(int)dfNodeCount,(int)dfObjNumsPerNode);
    m_pLogUlits->AppendMsg(szMsg);
    
    return dfObjNumsPerNode;
}

bool TrafficIndex::initJob(){
    m_strLinkVersion = getFeatureVersion("link",m_pUpdateLinkPG);
    m_strWeightAndFreeflowVersion = getFeatureVersion("weight_freeflow",m_pUpdateLinkPG);
    
    if (m_strLinkVersion == "" || m_strWeightAndFreeflowVersion == "") {
        m_pLogUlits->AppendMsg("获取路网或自由流版本失败");
        return false;
    }
    
    if (m_strNodeType == "master") {
        if (!resetObjStatus()) {
            m_pLogUlits->AppendMsg("重置obj状态失败");
            return false;
        }
    }
    
    char szMsg_1[512] = {0};
    sprintf(szMsg_1, "路网版本:%s，自由流版本:%s",m_strLinkVersion.c_str(),m_strWeightAndFreeflowVersion.c_str());
    m_pLogUlits->AppendMsg(szMsg_1);
    
    m_pLogUlits->AppendMsg("获取城市和辖区的link集合");
    getTTILinks();
    
    m_pLogUlits->AppendMsg("获取权重和自由流");
    getWeightAndFreeflow();
    
    char szCommand_1[512] = {0};
    sprintf(szCommand_1, "set %s %ld",m_strHostName.c_str(),m_mapPredefineObjList.size() + m_mapCustomObjList.size());
    redisReply* pReply = (redisReply*)redisCommand(m_pRedisConn,szCommand_1);
    freeReplyObject(pReply);
    
    char szCommand[512] = {0};
    sprintf(szCommand, "sadd node_list %s",m_strHostName.c_str());
    pReply = (redisReply*)redisCommand(m_pRedisConn,szCommand);
    freeReplyObject(pReply);
    
    m_pLogUlits->AppendMsg("启动计算线程");
    
    return true;
}

void TrafficIndex::getNodeList(redisContext* pRedisConn,vector<string>& vecNodeList){
    if (pRedisConn == NULL) {
        return;
    }
    
    char szCommand[512] = {0};
    sprintf(szCommand, "SMEMBERS node_list");
    redisReply* pReply = (redisReply*)redisCommand(pRedisConn,szCommand);
    
    if (pReply == NULL) {
        return;
    }
    for(size_t i = 0 ; i < pReply->elements;i++){
        vecNodeList.push_back(pReply->element[i]->str);
    }
    freeReplyObject(pReply);
}

void TrafficIndex::getObjCountPerNode(redisContext* pRedisConn,vector<string>& vecNodeList,map<string,int>& mapObjCountPerNode){
    if (pRedisConn == NULL || vecNodeList.empty()) {
        return;
    }
    
    for (size_t i = 0; i < vecNodeList.size(); i++) {
        char szCommand[512] = {0};
        sprintf(szCommand, "get %s",vecNodeList[i].c_str());
        redisReply* pReply = (redisReply*)redisCommand(pRedisConn,szCommand);
        
        if (pReply != NULL) {
            mapObjCountPerNode[vecNodeList[i]] = atoi(pReply->str);
            freeReplyObject(pReply);
        }
    }
}

bool TrafficIndex::isSmallestNode(redisContext* pRedisConn,int& nCurrentNodeCount){
    if (pRedisConn == NULL) {
        return false;
    }
    
    nCurrentNodeCount = -1;
    int nMinNodeCount = 1000000000;
    
    char szCommand[512] = {0};
    sprintf(szCommand, "SMEMBERS node_list");
    redisReply* pReply = (redisReply*)redisCommand(pRedisConn,szCommand);
    
    if (pReply != NULL) {
        for(size_t i = 0 ; i < pReply->elements;i++){
            char szCommand_1[512] = {0};
            sprintf(szCommand_1, "get %s",pReply->element[i]->str);
            redisReply* pReply_1 = (redisReply*)redisCommand(pRedisConn,szCommand_1);
            if (pReply_1 != NULL) {
                int nTempNodeCount = atoi(pReply_1->str);
                nMinNodeCount = nMinNodeCount > nTempNodeCount ? nTempNodeCount : nMinNodeCount;
                if (strcmp(m_strHostName.c_str(), pReply->element[i]->str) == 0) {
                    nCurrentNodeCount = nTempNodeCount;
                }
                freeReplyObject(pReply_1);
            }
        }
        freeReplyObject(pReply);
    }

    return nCurrentNodeCount == nMinNodeCount;
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
                    pthread_mutex_lock(&m_custom_obj_mutex);
                    int nJobID = atoi(ppRecord[0]);
                    int nObjID = atoi(ppRecord[1]);
                    map<int,vector<string> >::iterator iter_find = m_mapCustomObjList.find(nObjID);
                    if (iter_find != m_mapCustomObjList.end()){
                        m_mapCustomObjList.erase(iter_find);
                        
                        char szCommand_1[512] = {0};
                        sprintf(szCommand_1, "del lock_obj_%d",nObjID);
                        redisReply* pReply_1 = (redisReply*)redisCommand(m_pRedisConn,szCommand_1);
                        
                        char szCommand_2[512] = {0};
                        sprintf(szCommand_2, "get %s",m_strHostName.c_str());
                        redisReply* pReply_2 = (redisReply*)redisCommand(m_pRedisConn,szCommand_2);
                        
                        char szCommand_3[512] = {0};
                        sprintf(szCommand_3, "set %s %d",m_strHostName.c_str(),atoi(pReply_2->str) - 1);
                        redisReply* pReply_3 = (redisReply*)redisCommand(m_pRedisConn,szCommand_3);
                        
                        freeReplyObject(pReply_1);
                        freeReplyObject(pReply_2);
                        freeReplyObject(pReply_3);
                        
                        char szMsg[512] = {0};
                        sprintf(szMsg, "移除计算对象,job_id=%d,obj_id=%d",nJobID,nObjID);
                        m_pLogUlits->AppendMsg(szMsg);
                    }
                    
                    pthread_mutex_unlock(&m_custom_obj_mutex);
                }
            }
            mysql_free_result(pRes);
        }
    }
}
void TrafficIndex::add_tti_object(string& strObjID){
    int nCurrentNodeCount = -1;
    if (!isSmallestNode(m_pRedisConn,nCurrentNodeCount)) {
        return;
    }
    
    
    if (jobIsRunning(m_pRedisConn, atoi(strObjID.c_str()))){
        return;
    }
    
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
                        vector<string> vecELinkIDs;
                        queryELinkID(m_pSubscribePG,pszGeom,vecELinkIDs,pszFilter);
                        
                        if (!vecELinkIDs.empty()) {
                            pthread_mutex_lock(&m_custom_obj_mutex);
                            m_mapCustomObjList[atoi(pszObjID)] = vecELinkIDs;
                            pthread_mutex_unlock(&m_custom_obj_mutex);
                            
                            char szCommand_1[512] = {0};
                            sprintf(szCommand_1, "set %s %d",m_strHostName.c_str(),nCurrentNodeCount + 1);
                            redisReply* pReply_1 = (redisReply*)redisCommand(m_pRedisConn,szCommand_1);
                            freeReplyObject(pReply_1);
                            
                            char szMsg[512] = {0};
                            sprintf(szMsg, "增加计算对象,obj_id=%s",pszObjID);
                            m_pLogUlits->AppendMsg(szMsg);
                        }else{
                            char szMsg[512] = {0};
                            sprintf(szMsg, "未获取到link,obj_id=%s",pszObjID);
                            m_pLogUlits->AppendMsg(szMsg);
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
                m_pLogUlits->AppendMsg(szMsg);
                
                if (strChannel == "del_obj_set" || strChannel == "added_obj_set") {
                    string strObjID = reply->element[2]->str;
                    
                    if (strChannel == "del_obj_set") {
                        remove_tti_object(strObjID);
                    }else{
                        add_tti_object(strObjID);
                    }
                }
            }
        }
        
        freeReplyObject(reply);
    }
}
