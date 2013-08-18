/*
 * KeyDefine.h
 *
 *  Created on: 2013-8-10
 *      Author: tim
 */

#ifndef _KEY_DEFINE_H_
#define _KEY_DEFINE_H_
#include <stdint.h>

const uint16_t KEY_PROTOCOL_TYPE               = 0;

//ChunkPing
const uint16_t KEY_CHUNK_ID                    = 1;
const uint16_t KEY_CHUNK_IP                    = 2;
const uint16_t KEY_CHUNK_PORT                  = 3;
const uint16_t KEY_CHUNK_DISK_SPACE            = 4;
const uint16_t KEY_CHUNK_DISK_USED             = 5;
//ChunkPingResp
const uint16_t KEY_CHUNK_RSP_CHUNK_ID          = 1;
const uint16_t KEY_CHUNK_RSP_RESULT            = 2;

//FileInfoRep
const uint16_t KEY_FILEINFO_REQ_FID            = 1;
const uint16_t KEY_FILEINFO_REQ_CHUNKPATH      = 2;
//FileInfoResp
const uint16_t KEY_FILEINFO_RSP_RESULT         = 1;
const uint16_t KEY_FILEINFO_RSP_FILE_NAME      = 2;
const uint16_t KEY_FILEINFO_RSP_FILE_SIZE      = 3;
const uint16_t KEY_FILEINFO_RSP_CHUNK_NUM      = 4;
const uint16_t KEY_FILEINFO_RSP_CHUNK_PATH0    = 5;
const uint16_t KEY_FILEINFO_RSP_CHUNK_PATH1    = 6;
const uint16_t KEY_FILEINFO_RSP_CHUNK_PATH2    = 7;
const uint16_t KEY_FILEINFO_RSP_CHUNK_PATH3    = 8;
const uint16_t KEY_FILEINFO_RSP_CHUNK_PATH4    = 9;
//预留
const uint16_t KEY_FILEINFO_RSP_CHUNK_ID       = 20;
const uint16_t KEY_FILEINFO_RSP_CHUNK_IP       = 21;
const uint16_t KEY_FILEINFO_RSP_CHUNK_PORT     = 22;
const uint16_t KEY_FILEINFO_RSP_CHUNK_INDEX    = 23;
const uint16_t KEY_FILEINFO_RSP_CHUNK_OFFSET   = 24;
const uint16_t KEY_FILEINFO_RSP_FID            = 25;

//FileInfoSave
const uint16_t KEY_FILEINFO_SAVE_RESULT        = 1;
const uint16_t KEY_FILEINFO_SAVE_FID           = 2;
const uint16_t KEY_FILEINFO_SAVE_CHUNK_ID      = 3;
const uint16_t KEY_FILEINFO_SAVE_CHUNK_IP      = 4;
const uint16_t KEY_FILEINFO_SAVE_CHUNK_PORT    = 5;
const uint16_t KEY_FILEINFO_SAVE_CHUNK_INDEX   = 6;
const uint16_t KEY_FILEINFO_SAVE_CHUNK_OFFSET  = 7;
const uint16_t KEY_FILEINFO_SAVE_FILE_NAME     = 8;
const uint16_t KEY_FILEINFO_SAVE_FILE_SIZE     = 9;
//FileInfoSaveResult
const uint16_t KEY_FILEINFO_SAVE_RSP_RESULT    = 1;
const uint16_t KEY_FILEINFO_SAVE_RSP_FID       = 2;

//FileData
const uint16_t KEY_FILEDATA_FLAG               = 1;
const uint16_t KEY_FILEDATA_FID                = 2;
const uint16_t KEY_FILEDATA_FILE_NAME          = 3;
const uint16_t KEY_FILEDATA_FILE_SIZE          = 4;
const uint16_t KEY_FILEDATA_INDEX              = 5;
const uint16_t KEY_FILEDATA_OFFSET             = 6;
const uint16_t KEY_FILEDATA_SEG_SIZE           = 7;
const uint16_t KEY_FILEDATA_DATA               = 8;
const uint16_t KEY_FILEDATA_RESULT             = 9;

//FileDataReq
const uint16_t KEY_FILEDATA_REQ_FID            = 1;
const uint16_t KEY_FILEDATA_REQ_INDEX          = 2;
const uint16_t KEY_FILEDATA_REQ_OFFSET         = 3;
const uint16_t KEY_FILEDATA_REQ_SIZE           = 4;
#endif  //_KEY_DEFIE_H_


