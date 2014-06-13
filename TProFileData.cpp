#include "TProFileData.h"

static int GetCurrenTime(char *cDateTime)
{
        struct tm tTemp = {0};
        time_t tNow = time(NULL);

        localtime_r(&tNow, &tTemp);
        snprintf(cDateTime, 14, "%04d%02d%02d%02d%02d%02d",
                        tTemp.tm_year + 1900, tTemp.tm_mon + 1, tTemp.tm_mday,
                        tTemp.tm_hour, tTemp.tm_min, tTemp.tm_sec);

        return 0;
}

static inline int MiniterRemain(char *cDateTime)
{
        return 60 - atoi(cDateTime + 12);
}

static int RecordSplit(BUSI_INFO &info, const char *src, const int &num)
{
        static char cField[10][128];
        char *ptr = NULL;
        int  iCycle = 0;

        memset(cField, 0x00, sizeof(cField));
        for (; iCycle < num; ++iCycle) {
                ptr = cField[iCycle];
                while (*src != 0 && *src != '|') *ptr++ = *src++;
                if (*src != 0) {
                        *src++;
                } else {
                        break;
                }
        }
        if (iCycle != num) {
                printf("Not match[%d:%d]\n", iCycle, num);
                return -1;
        }

        info.busi_id = atoi(cField[3]);
        info.system_id = atoi(cField[2]);
        strcpy(info.user_id, cField[0]);
        strcpy(info.create_time, cField[6]);
        strcpy(info.ip_address, cField[1]);
        strcpy(info.busi_result, cField[4]);
        strcpy(info.busi_desc, cField[5]);

        return 0;
}

TProFileData::TProFileData(CONF_CONTEXT_INFO *conf, TDataBase *ctx):
        m_nFilePrefixlen(0), m_pConf(conf), m_pDbctx(ctx)
{
        memset(m_cCurrenTime, 0x00, sizeof(m_cCurrenTime));
        memset(m_cLastMin, 0x00, sizeof(m_cLastMin));
}

TProFileData::~TProFileData()
{

}

void TProFileData::Run(const char *path)
{
        while (true) {
                GetCurrenTime(m_cCurrenTime);
                if (memcmp(m_cCurrenTime + 4, m_cLastMin + 4, 8) == 0) {
                        /* sleep:NextMin */
                        sleep(MiniterRemain(m_cCurrenTime));
                        continue;
                } else {
                        memcpy(m_cLastMin, m_cCurrenTime, 12);
                }

                ReadDir(path);
        }

        return;
}

int TProFileData::ReadDir(const char *path)
{
        DIR *pCurrenPath       = NULL;
        struct dirent *pDirEnt = NULL;
        char filePath[256] = {0};
        char newFilePath[256] = {0};

        pCurrenPath = opendir(path);
        if (pCurrenPath == NULL) {
                printf("opendir dir[%s]:%s\n", path, strerror(errno));
                return -1;
        }
        printf("Open dir[%s] success!\n", path);

        while ((pDirEnt = readdir(pCurrenPath)) != NULL) {
                /* ignore father path
                if (strcmp(pDirEnt->d_name, ".") == 0 || strcmp(pDirEnt->d_name, "..") == 0) {
                        continue;
                }
                */

                /* check file */
                if (strncmp(pDirEnt->d_name, "reportBusiInfo", 14) != 0) {
                        continue;
                }

                /* ignore current time */
                if (memcmp(pDirEnt->d_name + m_nFilePrefixlen, m_cLastMin, 12) == 0) {
                        continue;
                }

                snprintf(filePath, sizeof(filePath), "%s/%s", path, pDirEnt->d_name);
                snprintf(newFilePath, sizeof(newFilePath), "%s/work/%s", path, pDirEnt->d_name);
                /* move file */
                if (rename(filePath, newFilePath) < 0) {
                        printf("Rename file[%s]:%s\n", newFilePath, strerror(errno));
                        continue;
                }

                /* read file */
                if (ReadFile(newFilePath) < 0) {
                        printf("ReadFile[%s] error\n", newFilePath);
                        continue;
                }

                /* delete file */
                unlink(newFilePath);
        }

        closedir(pCurrenPath);

        return 0;
}

int TProFileData::ReadFile(const char *path)
{
        FILE *fp = NULL;
        int nReadLine = 0;
        int nDoneLine = 0;
        int nInsertCount = 0;
        char buffer[BUFFER_LENGTH_MAX] = {0};
        BUSI_INFO info = {0};

        fp = fopen(path, "r");
        if (fp == NULL) {
                printf("fopen file[%s]:%s\n", path, strerror(errno));
                return -1;
        }
        printf("Open file[%s] Success!\n", path);

        while (fgets(buffer, sizeof(buffer), fp) != NULL) {
                /* tail /r/n */
                ++nReadLine;
                buffer[strlen(buffer) - 2] = '\0';

                memset(&info, 0x00, sizeof(info));
                if (RecordSplit(info, buffer, 7) < 0) {
                        printf("Split line[%s]\n", buffer);
                        memset(buffer, 0x00, sizeof(buffer));
                        continue;
                }

                /* Insert data */
                if (m_pDbctx->InsertData(info) < 0) {
                        printf("Insert line[%s]\n", buffer);
                        memset(buffer, 0x00, sizeof(buffer));
                        m_pDbctx->Rollback();
                        nInsertCount = 0;
                        continue;
                }

                if (nInsertCount++ >= 1500) {
                        m_pDbctx->Commit();
                        nInsertCount = 0;
                }

                /* Reset */
                memset(buffer, 0x00, sizeof(buffer));
                ++nDoneLine;
        }
        if (nInsertCount > 0) m_pDbctx->Commit();

        fclose(fp);

        printf("Read file[%s] Success! Read:%d, Done:%d\n", path, nReadLine, nDoneLine);
        return 0;
}
