//
//  main.cpp
//  TrafficIndex
//
//  Created by 张深圳 on 2017/12/28.
//  Copyright © 2017年 didi. All rights reserved.
//

#include "LogUlits.h"
#include "traffic_pb.pb.h"
#include <iostream>
#include "TrafficIndex.h"
#include <pthread.h>
#include <execinfo.h>


//日志互斥锁
pthread_mutex_t g_log_mutex;
//路况锁
pthread_mutex_t g_traffic_data_mutex;
//更新obj_list锁
pthread_mutex_t g_predefine_obj_mutex;

pthread_mutex_t g_update_weight_freeflow_mutex;

pthread_mutex_t g_pulish_url_mutex;

pthread_mutex_t g_custom_obj_mutex;

pthread_mutex_t g_crash_mutex;

void *computePredefineTTIThread(void *pParam){
    TrafficIndex* pTrafficIndex = (TrafficIndex*)pParam;
    while (true){
        time_t nCurrentTime = CommonTools::getCurrentTime();
        
        bool bIsLaunch = CommonTools::isLaunchProcessEx(nCurrentTime,5);
        
        if (!bIsLaunch){
            continue;
        }
        
        nCurrentTime -= 300;
        
        string strBatchTime = CommonTools::unix2Standard(nCurrentTime);
        
        pTrafficIndex->getLogUlits()->AppendMsg("开始计算predefine_TTI");
        pTrafficIndex->computeTTI(strBatchTime,1);
        pTrafficIndex->getLogUlits()->AppendMsg("结束计算predefine_TTI");
        
        sleep(1);
    }
    
    return NULL;
}

void *computeCustomTTIThread(void *pParam){
    TrafficIndex* pTrafficIndex = (TrafficIndex*)pParam;
    while (true){
        time_t nCurrentTime = CommonTools::getCurrentTime();
        
        bool bIsLaunch = CommonTools::isLaunchProcessEx(nCurrentTime,5);
        
        if (!bIsLaunch){
            continue;
        }
        
        nCurrentTime -= 300;
        
        string strBatchTime = CommonTools::unix2Standard(nCurrentTime);
        
        pTrafficIndex->getLogUlits()->AppendMsg("开始计算custom_TTI");
        pTrafficIndex->computeTTI(strBatchTime,2);
        pTrafficIndex->getLogUlits()->AppendMsg("结束计算custom_TTI");
        
        sleep(1);
    }

    return NULL;
}

void *getTrafficChildThread(void *pParam){
    ThreadParameter threadPara = *(ThreadParameter*)pParam;

    threadPara.pTrafficIndex->getTrafficDataFromServer(threadPara.pszCityCode,threadPara.nCurrentTime);

    return NULL;
}

void *getRealtimeTrafficThread(void *pParam){
    TrafficIndex* pTrafficIndex = (TrafficIndex*)pParam;
    
    vector<string> vecProvince;
    pTrafficIndex->getProvinceList(vecProvince);
    
    while (true){
        time_t nCurrentTime = CommonTools::getCurrentTime();
        int nMinute = CommonTools::getMinute(nCurrentTime);
        int nSecond = CommonTools::getSecond(nCurrentTime);
        
        if (nMinute % 2 == 0 && nSecond == 0) {
            ThreadParameter* pThreadParaArray = new ThreadParameter[vecProvince.size()];
            pthread_t* pThreadArray = new pthread_t[vecProvince.size()];
            
            for (size_t i = 0 ; i < vecProvince.size(); i++){
                ThreadParameter childPara;
                childPara.nCurrentTime = nCurrentTime;
                childPara.pszCityCode = vecProvince[i].c_str();
                childPara.pTrafficIndex = pTrafficIndex;
                
                pThreadParaArray[i] = childPara;
                pthread_t pth;
                pthread_create(&pth, NULL, getTrafficChildThread,(void*)&pThreadParaArray[i]);
                pThreadArray[i] = pth;
            }
            
            for (size_t i = 0 ; i < vecProvince.size(); i++){
                pthread_join(pThreadArray[i], NULL);
            }
            
            delete []pThreadParaArray;
            delete []pThreadArray;
            
            sleep(1);
        }
    }
    return NULL;
}

void *updateLinkThread(void *pParam){
    TrafficIndex* pTrafficIndex = (TrafficIndex*)pParam;
    while (true){
        time_t nCurrentTime = CommonTools::getCurrentTime();
        
        bool bIsLaunch = CommonTools::isLaunchProcess(nCurrentTime,5);
        
        if (!bIsLaunch){
            continue;
        }

        pTrafficIndex->updateLink();
        
        sleep(1);
    }
    
    return NULL;
}

void *consumeThread(void *pParam){
    TrafficIndex* pTrafficIndex = (TrafficIndex*)pParam;

    pTrafficIndex->consumeRedis();
    return NULL;
}

void *updateWeightAndFreeflowThread(void *pParam){
    TrafficIndex* pTrafficIndex = (TrafficIndex*)pParam;
    while (true){
        time_t nCurrentTime = CommonTools::getCurrentTime();
        
        bool bIsLaunch = CommonTools::isLaunchProcess(nCurrentTime,5);
        
        if (!bIsLaunch){
            continue;
        }
    
        pTrafficIndex->updateWeightAndFreeflow();
        
        sleep(1);
    }
    
    return NULL;
}

void *aggreTTIThread(void *pParam){
    TrafficIndex* pTrafficIndex = (TrafficIndex*)pParam;
    while (true){
        time_t nCurrentTime = CommonTools::getCurrentTime();
                
        int nHour = CommonTools::getHour(nCurrentTime);
        int nMinute = CommonTools::getMinute(nCurrentTime);
        int nSecond = CommonTools::getSecond(nCurrentTime);
        
        //延后10分钟计算
        //每小时
        if (nMinute == 15 && nSecond == 0){
            pTrafficIndex->aggreTTT(1,nCurrentTime);
            sleep(1);
        }
        
        //每天
        if (nHour == 0 && nMinute == 15 && nSecond == 0){
            pTrafficIndex->aggreTTT(2,nCurrentTime);
            sleep(1);
        }
    }
    
    return NULL;
}
void catchCrash(int nSignal){
    pthread_mutex_lock(&g_crash_mutex);
    
    FILE* pCrashFile = fopen("./crash.log", "a+");
    
    if (pCrashFile == NULL){
        pthread_mutex_unlock(&g_crash_mutex);
        return;
    }
    
    try{
        const int MAX_STACK_FRAMES = 128;
        
        char szLine[512] = {0, };
        time_t t = time(NULL);
        tm* now = localtime(&t);
        sprintf(szLine,"-------------------[%04d-%02d-%02d %02d:%02d:%02d][crash signal code:%d]-------------------\n",now->tm_year + 1900,now->tm_mon + 1,now->tm_mday,now->tm_hour,now->tm_min,now->tm_sec,nSignal);
        fwrite(szLine, 1, strlen(szLine), pCrashFile);
        void* array[MAX_STACK_FRAMES];
        char** ppStrings = NULL;
        signal(nSignal, SIG_DFL);
        int nSize = backtrace(array, MAX_STACK_FRAMES);
        ppStrings = (char**)backtrace_symbols(array, nSize);
        for (int i = 0; i < nSize; ++i){
            char szLine[512] = {0, };
            sprintf(szLine, "%d %s\n", i, ppStrings[i]);
            fwrite(szLine, 1, strlen(szLine), pCrashFile);
            
            std::string strMsg(ppStrings[i]);
            size_t nPos1 = strMsg.find_first_of("[");
            size_t nPos2 = strMsg.find_last_of("]");
            std::string strAddress = strMsg.substr(nPos1 + 1, nPos2 - nPos1 -1);
            char szCmd[128] = {0, };
            sprintf(szCmd, "addr2line -e TrafficIndex.out %s", strAddress.c_str());
            FILE *pFile = popen(szCmd, "r");
            if(pFile != NULL){
                char szBuff[1024];
                memset(szBuff, 0, sizeof(szBuff));
                char* szRet = fgets(szBuff, sizeof(szBuff), pFile);
                pclose(pFile);
                fwrite(szRet, 1, strlen(szRet), pCrashFile);
            }
        }
        free(ppStrings);
    }
    catch (...){
        //
    }
    fflush(pCrashFile);
    fclose(pCrashFile);
    pCrashFile = NULL;
    
    pthread_mutex_unlock(&g_crash_mutex);
}

int main(int argc, const char * argv[])
{
    string strUUID = CommonTools::getUUID();
    
    pthread_mutex_init(&g_log_mutex, NULL);
    pthread_mutex_init(&g_traffic_data_mutex, NULL);
    pthread_mutex_init(&g_predefine_obj_mutex, NULL);
    pthread_mutex_init(&g_update_weight_freeflow_mutex, NULL);
    pthread_mutex_init(&g_pulish_url_mutex, NULL);
    pthread_mutex_init(&g_custom_obj_mutex, NULL);
    pthread_mutex_init(&g_crash_mutex, NULL);
    
    curl_global_init(CURL_GLOBAL_ALL);
    
    LogUlits* pLogUlits = new LogUlits(g_log_mutex,strUUID);
    
    string strCurrentPath = CommonTools::getCurrentPath();
    
    string strLogPath = strCurrentPath;
    strLogPath += "/TrafficIndex.log";
    
    if (!pLogUlits->InitLog(strLogPath))
    {
        printf("%s 创建日志文件失败，程序退出\n",strUUID.c_str());
        delete pLogUlits;
        pthread_mutex_destroy(&g_log_mutex);
        pthread_mutex_destroy(&g_traffic_data_mutex);
        pthread_mutex_destroy(&g_predefine_obj_mutex);
        pthread_mutex_destroy(&g_update_weight_freeflow_mutex);
        pthread_mutex_destroy(&g_pulish_url_mutex);
        pthread_mutex_destroy(&g_custom_obj_mutex);
        pthread_mutex_destroy(&g_crash_mutex);
        curl_global_cleanup();
        google::protobuf::ShutdownProtobufLibrary();
        return 1;
    }
    
    DBInfo mySQLInfo;
    DBInfo pgInfo;
    DBInfo redisInfo;
    
    string strTrafficPublicURL;
    vector<string> vecProvince;
    string strConfPath = strCurrentPath;
    string strNodeType;
    if (!CommonTools::parseConf(strConfPath, mySQLInfo, pgInfo,redisInfo,strTrafficPublicURL,vecProvince,strNodeType))
    {
        pLogUlits->AppendMsg("解析配置文件失败");
        delete pLogUlits;
        pthread_mutex_destroy(&g_log_mutex);
        pthread_mutex_destroy(&g_traffic_data_mutex);
        pthread_mutex_destroy(&g_predefine_obj_mutex);
        pthread_mutex_destroy(&g_update_weight_freeflow_mutex);
        pthread_mutex_destroy(&g_pulish_url_mutex);
        pthread_mutex_destroy(&g_custom_obj_mutex);
        pthread_mutex_destroy(&g_crash_mutex);
        curl_global_cleanup();
        google::protobuf::ShutdownProtobufLibrary();
        return 1;
    }
    
    pLogUlits->AppendMsg("TTI计算服务启动");
    
    // 捕捉崩溃日志
    signal(SIGSEGV,catchCrash);
    signal(SIGFPE,catchCrash);
    signal(SIGABRT,catchCrash);
    
    TrafficIndex* pTrafficIndex = new TrafficIndex();
    
    pTrafficIndex->setProvinceList(vecProvince);
    pTrafficIndex->setTrafficPublicURL(strTrafficPublicURL);
    pTrafficIndex->setMySQLInfo(mySQLInfo);
    pTrafficIndex->setPGInfo(pgInfo);
    pTrafficIndex->setRedisInfo(redisInfo);
    pTrafficIndex->setLogUlits(pLogUlits);
    pTrafficIndex->setTrafficDataMutex(g_traffic_data_mutex);
    pTrafficIndex->setPredefineObjMutex(g_predefine_obj_mutex);
    pTrafficIndex->setUpdateWeightAndFreeflowMutex(g_update_weight_freeflow_mutex);
    pTrafficIndex->setPublishURLMutex(g_pulish_url_mutex);
    pTrafficIndex->setCustomObjMutex(g_custom_obj_mutex);
    pTrafficIndex->setNodeType(strNodeType);
    pTrafficIndex->setHostName(strUUID);
    
    if (!pTrafficIndex->connectDatabase())
    {
        delete pLogUlits;
        delete pTrafficIndex;
        pthread_mutex_destroy(&g_log_mutex);
        pthread_mutex_destroy(&g_traffic_data_mutex);
        pthread_mutex_destroy(&g_predefine_obj_mutex);
        pthread_mutex_destroy(&g_update_weight_freeflow_mutex);
        pthread_mutex_destroy(&g_pulish_url_mutex);
        pthread_mutex_destroy(&g_custom_obj_mutex);
        pthread_mutex_destroy(&g_crash_mutex);
        curl_global_cleanup();
        google::protobuf::ShutdownProtobufLibrary();
        return 1;
    }
    
    if (!pTrafficIndex->initJob()) {
        delete pLogUlits;
        delete pTrafficIndex;
        pthread_mutex_destroy(&g_log_mutex);
        pthread_mutex_destroy(&g_traffic_data_mutex);
        pthread_mutex_destroy(&g_predefine_obj_mutex);
        pthread_mutex_destroy(&g_update_weight_freeflow_mutex);
        pthread_mutex_destroy(&g_pulish_url_mutex);
        pthread_mutex_destroy(&g_custom_obj_mutex);
        pthread_mutex_destroy(&g_crash_mutex);
        curl_global_cleanup();
        google::protobuf::ShutdownProtobufLibrary();
        return 1;
    }
    
    pthread_t trafficThread;
    pthread_create(&trafficThread, NULL, getRealtimeTrafficThread, (void*)pTrafficIndex);

    pthread_t prettiThread;
    pthread_create(&prettiThread, NULL, computePredefineTTIThread, (void*)pTrafficIndex);

    pthread_t customttiThread;
    pthread_create(&customttiThread, NULL, computeCustomTTIThread, (void*)pTrafficIndex);

    pthread_t predefineObjThread;
    pthread_create(&predefineObjThread, NULL, updateLinkThread, (void*)pTrafficIndex);

    pthread_t updateRefThread;
    pthread_create(&updateRefThread, NULL, updateWeightAndFreeflowThread, (void*)pTrafficIndex);
    
    pthread_t mqThread;
    pthread_create(&mqThread, NULL, consumeThread, (void*)pTrafficIndex);

    if (strNodeType == "master") {
        pthread_t aggreThread;
        pthread_create(&aggreThread, NULL, aggreTTIThread, (void*)pTrafficIndex);
        pthread_join(aggreThread, NULL);
    }

    pthread_join(trafficThread, NULL);
    pthread_join(prettiThread, NULL);
    pthread_join(customttiThread, NULL);
    pthread_join(predefineObjThread, NULL);
    pthread_join(updateRefThread, NULL);
    pthread_join(mqThread, NULL);
    
    delete pLogUlits;
    delete pTrafficIndex;
    pthread_mutex_destroy(&g_log_mutex);
    pthread_mutex_destroy(&g_traffic_data_mutex);
    pthread_mutex_destroy(&g_predefine_obj_mutex);
    pthread_mutex_destroy(&g_update_weight_freeflow_mutex);
    pthread_mutex_destroy(&g_pulish_url_mutex);
    pthread_mutex_destroy(&g_custom_obj_mutex);
    pthread_mutex_destroy(&g_crash_mutex);
    curl_global_cleanup();
    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
