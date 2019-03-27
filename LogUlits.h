#pragma once
#include "stdio.h"
#include "stdlib.h"
#include <string>
#include <pthread.h>
#include "CommonTools.h"
using namespace std;

class LogUlits
{
public:
	LogUlits(pthread_mutex_t mutex,string strUUID);
	~LogUlits();
private:
    FILE* m_pFile;
	pthread_mutex_t m_mutex;

    string m_strUUID;

public:
	bool InitLog(string strLogPath);

	void AppendMsg(const char* pszMsg);
};

