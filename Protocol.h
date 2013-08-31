/*
 * Protocol.h
 *
 *  Created on: 2012-12-29
 *      Author: LiuYongJin
 */

#ifndef _PROTOCOL_TYPE_H_
#define _PROTOCOL_TYPE_H_

#include <stdint.h>
#include <string>
#include <vector>
using std::string;
using std::vector;

//协议类型
typedef enum
{
PROTOCOL_BEGIN                  = 0,
//文件信息
PROTOCOL_FILE_INFO_REQ          = 1,    //请求文件信息
PROTOCOL_FILE_INFO              = 2,    //文件信息
PROTOCOL_FILE_INFO_SAVE_RESULT  = 3,    //文件信息保存结果
//文件数据
PROTOCOL_FILE_REQ               = 4,    //请求文件数据
PROTOCOL_FILE                   = 5,    //文件数据
PROTOCOL_FILE_SAVE_RESULT       = 6,    //文件保存状态
//chunk信息
PROTOCOL_CHUNK_PING             = 7,    //chunk请求master保存chunk信息
PROTOCOL_CHUNK_PING_RESP        = 8,    //master回复chunk保存信息结果

PROTOCOL_END                    = 9,
}ProtocolType;


//chunk的ping包信息
class ChunkInfo
{
public:
	string id;               //chunk的id
	string ip;               //chunk的ip
	int32_t port;            //chunk的端口
	uint64_t disk_space;      //chunk的磁盘空间
	uint64_t disk_used;       //chunk的磁盘已用空间
};

//chunk的ping包回复
class ChunkPingRsp
{
public:
	int16_t result;   //0成功,1失败
	string chunk_id;
};

//文件的chunk路径
class ChunkPath
{
public:
	string id;               //chunk的id
	string ip;               //chunk的ip
	uint32_t port;           //chunk的端口
	uint32_t index;          //文件在chunk上的index号
	uint32_t offset;         //文件在chunk上的偏移
};

class FileInfoReq
{
public:
	string fid;                //文件的fid
	int16_t query_chunkpath;   //如果没有文件信息,是否请求分配chunk;0:不请求;1:请求
};

//文件信息
class FileInfo
{
public:
	enum
	{
		RESULT_INVALID=0,   //无效
		RESULT_FAILED,      //失败
		RESULT_CHUNK,       //分配chunk,file_info的chunk_path有效
		RESULT_SAVING,      //正在存储,file_info无
		RESULT_SUCC         //成功,file_info有效
	};

	int16_t result;          //文件信息标志
	string fid;              //文件的fid
	string name;             //文件名
	uint32_t size;           //文件大小

	int GetChunkPathCount(){return chunk_paths.size();}
	void AddChunkPath(ChunkPath &chunk_path){chunk_paths.push_back(chunk_path);}
	ChunkPath& GetChunkPath(int index){return chunk_paths[index];}
private:
	vector<ChunkPath> chunk_paths;
};

class FileInfoSaveResult
{
public:
	enum
	{
		RESULT_SUCC =0,  //保存成功
		RESULT_FAILED    //保存失败
	};

	int16_t result;
	string fid;
};

//请求chunk获取数据
class FileReq
{
public:
	string fid;      //文件fid
	uint32_t index;  //文件index
	uint32_t offset; //文件所在的偏移
	uint32_t size;   //文件大小
};

//文件分片
class FileData
{
public:
	typedef enum
	{
		FLAG_INVALID,        //无效标记
		FLAG_START,          //任务开始
		FLAG_SEG,            //文件分片
		FLAG_END             //任务结束
	}FileFlag;

	int16_t flag;           //文件任务标记
	string fid;              //文件的fid
	string name;             //文件名
	uint32_t filesize;       //文件的大小
	uint32_t offset;         //分片偏移位置
	uint32_t index;          //分片序号
	uint32_t size;           //分片大小
	char *data;        //分片数据
};

class FileSaveResult
{
public:
	typedef enum
	{
		CREATE_FAILED,       //创建失败
		CREATE_SUCC,         //创建成功
		DATA_SAVE_FAILED,    //分片接收成功
		DATA_SAVE_SUCC,      //分片接收失败
	}Status;

	int16_t status;           //状态
	string fid;              //fid
	uint32_t index;          //分片index
};

#endif //_PROTOCOL_TYPE_H_

