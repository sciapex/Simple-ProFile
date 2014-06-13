#ifndef __TPROFILEDATA_H
#define __TPROFILEDATA_H

#include "DataInfoDef.h"
#include "TDataBase.h"

class TProFileData {
        public:

        TProFileData(CONF_CONTEXT_INFO *conf, TDataBase *ctx);
        ~TProFileData();

        void Run(const char *path);
        int ReadDir(const char *path);
        int ReadFile(const char *path);


        private:
        char m_cCurrenTime[14 + 1];
        char m_cLastMin[12 + 1];
        int  m_nFilePrefixlen;

        TDataBase *m_pDbctx;
        CONF_CONTEXT_INFO *m_pConf;
};



#endif
