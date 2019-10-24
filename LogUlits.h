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
	LogUlits();
	~LogUlits();
private:
    static FILE* m_pFile;
    static pthread_mutex_t* m_pMutex;
    static const char* m_pszUUID;

public:
	static bool initLog(const char* pszLogPath,pthread_mutex_t* pMutex,const char* pszUUID);

	static void appendMsg(const char* pszMsg);
};

