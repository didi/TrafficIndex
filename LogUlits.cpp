#include "LogUlits.h"

LogUlits::LogUlits(pthread_mutex_t mutex,string strUUID)
{
    m_mutex = mutex;
	m_pFile = NULL;
    m_strUUID = strUUID;
}


LogUlits::~LogUlits()
{
	if (m_pFile != NULL)
	{
		fclose(m_pFile);
		m_pFile = NULL;
	}

}

bool LogUlits::InitLog(string strLogPath)
{
	m_pFile = fopen(strLogPath.c_str(), "a+");
	if (m_pFile == NULL)
	{
		return false;
	}

	return true;
}

void LogUlits::AppendMsg(const char* pszMsg)
{
    if (pszMsg == NULL) {
        return;
    }
    
    pthread_mutex_lock(&m_mutex);
	if (m_pFile == NULL)
	{
        pthread_mutex_unlock(&m_mutex);
		return;
	}

	time_t currentTime = time(NULL);

    string strCurTime = CommonTools::unix2Standard(currentTime);

	fprintf(m_pFile, "%s %s %s\n", strCurTime.c_str(),m_strUUID.c_str(),pszMsg);
	fflush(m_pFile);
    pthread_mutex_unlock(&m_mutex);
}
