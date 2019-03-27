#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <vector>
#include <map>
#include <string>
#include <fstream>
#include <uuid/uuid.h>
#include <algorithm>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <stdio.h>
#include <unistd.h>
#include "zlib.h"
#include <stdint.h>
#include <memory.h>
using namespace std;


typedef struct
{
    string strHost;
    string strPort;
    string strUser;
    string strPassword;
    string strDBName;
}DBInfo;

class CommonTools
{
public:
    CommonTools();
    ~CommonTools();
    
public:
    static string getCurrentPath();
    
    static string getUUID();
    
    static string unix2Standard(time_t nUnix);
    
    static bool parseConf(string strConfPath,DBInfo& mySQLInfo,DBInfo& pgInfo,DBInfo& redisInfo,
                          string& strTrafficPublicURL,vector<string>& vecProvince,string& strNodeType);
    
    static void split(const std::string &strSrc, const std::string &strPattern,std::vector<std::string>& resVec);
    
    static bool decoderGz(uint8_t* src_data, uint32_t src_length,  uint8_t* dest_data, uint32_t & dest_length);
    
    static time_t getCurrentTime();
    
    static string getCurrentTime_s();
    
    static bool isOddNumber(int nNumber);
    
    static bool isOddMinute(time_t nCurrentTime);
    
    static bool isLaunchProcess(time_t nCurrentTime,int nLaunchMinute);
    
    static bool isLaunchProcessEx(time_t nCurrentTime,int nLaunchMinute);
    
    static int getHour(time_t nCurrentTime);
    
    static int getMinute(time_t nCurrentTime);
    
    static int getSecond(time_t nCurrentTime);
    
    static time_t standard2Unix(const char* szTimestamp);
    
    static string toString(int nValue);
    
    static string toString(double dfValue);
    
    static string toString(long long llfValue);
    
    static string getHostname();
};

