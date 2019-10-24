//
//  CommonTools.cpp
//  TrafficIndex
//
//  Created by didi on 2017/12/28.
//  Copyright © 2017年 didi. All rights reserved.
//

#include "CommonTools.h"

CommonTools::CommonTools()
{
    
}

CommonTools::~CommonTools()
{
    
}

string CommonTools::getCurrentPath(){
    char *szPath = getcwd(NULL, 0);
    
    string strCurrentPath = szPath;
    
    free(szPath);
    return strCurrentPath;
}

string CommonTools::getUUID(){
    uuid_t uuid_obj;
    
    char szUUID[36];
    uuid_generate( uuid_obj );
    uuid_unparse_lower(uuid_obj, szUUID);
    
    string strUUID = szUUID;
    
    strUUID.erase(std::remove(strUUID.begin(), strUUID.end(), '-'), strUUID.end());
    return strUUID;
}

string CommonTools::unix2Standard(time_t nUnix){
    struct tm newtime;
    localtime_r(&nUnix, &newtime);
    
    char szTime[100] = { 0 };
    strftime(szTime, sizeof(szTime) - 1, "%Y-%m-%d %H:%M:%S", &newtime);
    
    string strTime = szTime;
    return strTime;
}
void CommonTools::split(const std::string &strSrc, const std::string &strPattern,std::vector<std::string>& resVec){
    resVec.clear();
    if (strSrc == "" ){
        return;
    }
    std::string strs = strSrc + strPattern;
    
    size_t nPos = strs.find(strPattern);
    size_t nSize = strs.size();
    
    while (nPos != std::string::npos){
        std::string x = strs.substr(0, nPos);
        resVec.push_back(x);
        strs = strs.substr(nPos + 1, nSize);
        nPos = strs.find(strPattern);
    }
}


bool CommonTools::parseConf(string strConfPath, DBInfo &mySQLInfo, DBInfo &pgInfo,DBInfo& redisInfo,string& strTrafficPublicURL,vector<string>& vecProvince,string& strNodeType){
    string strConfigPath = strConfPath;
    
    strConfigPath += "/config.conf";
    
    std::ifstream configFile;
    configFile.open(strConfigPath.c_str());
    
    if (!configFile.is_open()){
        return false;
    }
    
    vector<string> vecRecord;
    
    std::string strRecord;
    while (std::getline(configFile, strRecord)){
        if (strRecord == "") {
            continue;
        }
        
        char szFirstChar = strRecord.at(0);
        
        if (szFirstChar == '#') {
            continue;
        }
        vecRecord.push_back(strRecord);
    }
    
    configFile.close();
    
    if (vecRecord.size() != 16){
        return false;
    }
    
    vector<string>::iterator iter = vecRecord.begin();
    for (; iter != vecRecord.end(); iter++){
        vector<string> vecSplit;
        split(*iter, "=", vecSplit);
        if(vecSplit.size() == 2){
            if (vecSplit[0] == "pg_host") {
                pgInfo.strHost = vecSplit[1];
            }else if(vecSplit[0] == "pg_port"){
                pgInfo.strPort = vecSplit[1];
            }else if(vecSplit[0] == "pg_user"){
                pgInfo.strUser = vecSplit[1];
            }else if(vecSplit[0] == "pg_password"){
                pgInfo.strPassword = vecSplit[1];
            }else if(vecSplit[0] == "pg_database"){
                pgInfo.strDBName = vecSplit[1];
            }else if (vecSplit[0] == "mysql_host") {
                mySQLInfo.strHost = vecSplit[1];
            }else if(vecSplit[0] == "mysql_port"){
                mySQLInfo.strPort = vecSplit[1];
            }else if(vecSplit[0] == "mysql_user"){
                mySQLInfo.strUser = vecSplit[1];
            }else if(vecSplit[0] == "mysql_password"){
                mySQLInfo.strPassword = vecSplit[1];
            }else if(vecSplit[0] == "mysql_database"){
                mySQLInfo.strDBName = vecSplit[1];
            }else if (vecSplit[0] == "redis_host") {
                redisInfo.strHost = vecSplit[1];
            }else if(vecSplit[0] == "redis_port"){
                redisInfo.strPort = vecSplit[1];
            }else if(vecSplit[0] == "redis_password"){
                redisInfo.strPassword = vecSplit[1];
            }else if(vecSplit[0] == "traffic_publish_url"){
                strTrafficPublicURL = vecSplit[1];
            }else if(vecSplit[0] == "province_list"){
                split(vecSplit[1], ",", vecProvince);
            }else if(vecSplit[0] == "node_type"){
                strNodeType = vecSplit[1];
            }
        }
    }
    
    return true;;
}

bool CommonTools::decoderGz(uint8_t *src_data, uint32_t src_length, uint8_t *dest_data, uint32_t &dest_length){
    int err = 0;
    z_stream d_stream = {0};
    static char dummy_head[2] ={
        0x8 + 0x7 * 0x10,
        (((0x8 + 0x7 * 0x10) * 0x100 + 30) / 31 * 31) & 0xFF,
    };
    d_stream.zalloc = (alloc_func)0;
    d_stream.zfree = (free_func)0;
    d_stream.opaque = (voidpf)0;
    d_stream.next_in = src_data;
    d_stream.avail_in = 0;
    d_stream.next_out = dest_data;
    if(inflateInit2(&d_stream,31) != Z_OK)
        return false;
    while (d_stream.total_out < dest_length && d_stream.total_in < src_length) {
        d_stream.avail_in = d_stream.avail_out = 1; /* force small buffers */
        if((err = inflate(&d_stream, Z_NO_FLUSH)) == Z_STREAM_END) break;
        if(err != Z_OK ) {
            if(err == Z_DATA_ERROR) {
                d_stream.next_in = (Bytef*) dummy_head;
                d_stream.avail_in = sizeof(dummy_head);
                if((err = inflate(&d_stream, Z_NO_FLUSH)) != Z_OK)  {
                    return false;
                }
            } else {
                return false;
            }
        }
    }
    if(inflateEnd(&d_stream) != Z_OK)
        return false;
    dest_length = (uint32_t)d_stream.total_out;
    return true;
}

time_t CommonTools::getCurrentTime(){
    time_t nCurrentTime;
    time(&nCurrentTime);
    return nCurrentTime;
}

string CommonTools::getCurrentTime_s(){
    time_t nCurrentTime = getCurrentTime();
    string strCurrentTime = unix2Standard(nCurrentTime);
    return strCurrentTime;
}

int CommonTools::getHour(time_t nCurrentTime){
    struct tm newtime;
    localtime_r(&nCurrentTime, &newtime);
    char szTime[100] = { 0 };
    strftime(szTime, sizeof(szTime) - 1, "%H", &newtime);
    
    return atoi(szTime);
}

int CommonTools::getMinute(time_t nCurrentTime){
    struct tm newtime;
    localtime_r(&nCurrentTime, &newtime);
    char szTime[100] = { 0 };
    strftime(szTime, sizeof(szTime) - 1, "%M", &newtime);
    
    return atoi(szTime);
}

int CommonTools::getSecond(time_t nCurrentTime){
    struct tm newtime;
    localtime_r(&nCurrentTime, &newtime);
    char szTime[100] = { 0 };
    strftime(szTime, sizeof(szTime) - 1, "%S", &newtime);
    
    return atoi(szTime);
}
bool CommonTools::isLaunchProcess(time_t nCurrentTime,int nLaunchMinute){
    int nMinute = getMinute(nCurrentTime);
    int nSecond = getSecond(nCurrentTime);
    
    if (nMinute % nLaunchMinute == 0 && nSecond == 0) {
        return true;
    }
    
    return false;
}

bool CommonTools::isLaunchProcessEx(time_t nCurrentTime,int nLaunchMinute){
    int nMinute = getMinute(nCurrentTime);
    int nSecond = getSecond(nCurrentTime);
    
    if (nMinute % nLaunchMinute == 0 && nMinute % 10 != 0 && nSecond == 0) {
        return true;
    }
    
    return false;
}
time_t CommonTools::standard2Unix(const char* szTimestamp){
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    
    sscanf(szTimestamp, "%d-%d-%d %d:%d:%d", &tm.tm_year, &tm.tm_mon, &tm.tm_mday, &tm.tm_hour, &tm.tm_min, &tm.tm_sec);
    
    tm.tm_year -= 1900;
    tm.tm_mon--;
    
    return mktime(&tm);
}

std::string CommonTools::toString(int nValue){
    char szValue[128] = {0};
    sprintf(szValue, "%d",nValue);
    return szValue;
}

std::string CommonTools::toString(double dfValue){
    char szValue[128] = {0};
    sprintf(szValue, "%f",dfValue);
    return szValue;
}

std::string CommonTools::toString(long long llValue){
    char szValue[128] = {0};
    sprintf(szValue, "%lld",llValue);
    return szValue;
}
std::string CommonTools::getIP() {
    string strIP = "";
    struct ifaddrs * ifAddrStruct=NULL;
    void * tmpAddrPtr=NULL;
    
    getifaddrs(&ifAddrStruct);
    
    while (ifAddrStruct!=NULL) {
        if (ifAddrStruct->ifa_addr->sa_family==AF_INET) { // check it is IP4
            // is a valid IP4 Address
            tmpAddrPtr=&((struct sockaddr_in *)ifAddrStruct->ifa_addr)->sin_addr;
            char addressBuffer[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
            if (strcmp(addressBuffer, "127.0.0.1") != 0) {
                strIP = addressBuffer;
                break;
            }
        }
        ifAddrStruct=ifAddrStruct->ifa_next;
    }
    return strIP;
}

size_t CommonTools::writeMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp) { 
    size_t realsize = size * nmemb;
    memoryStruct *mem = (memoryStruct *)userp;
    
    char *ptr = (char*)realloc(mem->memory, mem->size + realsize + 1);
    if(ptr == NULL) {
        /* out of memory! */
        printf("not enough memory (realloc returned NULL)\n");
        return 0;
    }
    
    mem->memory = ptr;
    memcpy(&(mem->memory[mem->size]), contents, realsize);
    mem->size += realsize;
    mem->memory[mem->size] = 0;
    
    return realsize;
}

void CommonTools::httpGet(const char *pszURL, memoryStruct *pMemData) { 
    CURL* pCurl = curl_easy_init();
    
    if (pCurl != NULL) {
        curl_easy_setopt(pCurl, CURLOPT_URL, pszURL);
        curl_easy_setopt(pCurl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(pCurl, CURLOPT_TIMEOUT, 300);
        curl_easy_setopt(pCurl, CURLOPT_NOPROGRESS, 1L);
        curl_easy_setopt(pCurl, CURLOPT_WRITEFUNCTION, writeMemoryCallback);
        curl_easy_setopt(pCurl, CURLOPT_WRITEDATA, (void*)pMemData);
        curl_easy_perform(pCurl);
        curl_easy_cleanup(pCurl);
    }
}








