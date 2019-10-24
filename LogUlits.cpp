#include "LogUlits.h"

FILE* LogUlits::m_pFile = NULL;
pthread_mutex_t* LogUlits::m_pMutex = NULL;
const char* LogUlits::m_pszUUID = NULL;

LogUlits::LogUlits(){
}


LogUlits::~LogUlits(){
}

bool LogUlits::initLog(const char* pszLogPath,pthread_mutex_t* pMutex,const char* pszUUID){
    m_pMutex = pMutex;
    m_pszUUID = pszUUID;
    m_pFile = fopen(pszLogPath, "a+");
    if (m_pFile == NULL){
        return false;
    }

	return true;
}

void LogUlits::appendMsg(const char* pszMsg){
    if (pszMsg == NULL) {
        return;
    }

    pthread_mutex_lock(m_pMutex);
    if (m_pFile == NULL){
        pthread_mutex_unlock(m_pMutex);
        return;
    }

    time_t currentTime = time(NULL);

    string strCurTime = CommonTools::unix2Standard(currentTime);

    fprintf(m_pFile, "%s %s %s\n", strCurTime.c_str(),m_pszUUID,pszMsg);
    fflush(m_pFile);
    pthread_mutex_unlock(m_pMutex);
}
