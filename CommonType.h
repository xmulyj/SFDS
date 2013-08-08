/*
 * CommonType.h
 *
 *  Created on: 2012-12-29
 *      Author: LiuYongJin
 */

#ifndef _COMMON_TYPE_H_
#define _COMMON_TYPE_H_

#include <stdint.h>
#include <string>
#include <vector>
using std::string;
using std::vector;

//KVData的key
#define KEY_PROTOCOL_TYPE 0

//协议类型
//文件信息
#define PROTOCOL_FILE_INFO_REQ          0    //请求文件信息
#define PROTOCOL_FILE_INFO              1    //文件信息
#define PROTOCOL_FILE_INFO_SAVE_RESULT  2    //文件信息保存结果
//文件数据
#define PROTOCOL_FILE_REQ               3    //请求文件数据
#define PROTOCOL_FILE                   4    //文件数据
#define PROTOCOL_FILE_SAVE_RESULT       5    //文件保存状态
//chunk信息
#define PROTOCOL_CHUNK_PING             6    //chunk请求master保存chunk信息
#define PROTOCOL_CHUNK_PING_RESP        7    //master回复chunk保存信息结果


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

//文件信息
class FileInfo
{
public:
	typedef enum
	{
		RESULT_INVALID,   //无效
		RESULT_FAILED,    //失败
		RESULT_CHUNK,     //分配chunk,file_info的chunk_path有效
		RESULT_SAVING,    //正在存储,file_info无
		RESULT_SUCC       //成功,file_info有效
	}Result;

	Result result;           //文件信息标志
	string fid;              //文件的fid
	string name;             //文件名
	uint32_t size;           //文件大小

	int get_chunkpath_count(){return chunk_paths.size();}
	void add_chunkpath(ChunkPath &chunk_path){chunk_paths.push_back(chunk_path);}
	ChunkPath& get_chunkpath(int index){return chunk_paths[index];}
private:
	vector<ChunkPath> chunk_paths;
};

//chunk信息
class ChunkInfo
{
public:
	string id;               //chunk的id
	string ip;               //chunk的ip
	uint32_t port;           //chunk的端口
	uint64_t disk_space;     //chunk的磁盘空间
	uint64_t disk_used;      //chunk的磁盘已用空间
};

class FileInfoSaveResult
{
public:
	typedef enum
	{
		RESULT_FAILED,  //保存失败
		RESULT_SUCC     //保存成功
	}Result;

	Result result;
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
class FileSeg
{
public:
	typedef enum
	{
		FLAG_INVALID,        //无效标记
		FLAG_START,          //任务开始
		FLAG_SEG,            //文件分片
		FLAG_END             //任务结束
	}FileFlag;
	void set(FileFlag flag, string &fid, string &name, uint32_t filesize=0, uint32_t offset=0, uint32_t index=0, uint32_t seg_size=0)
	{
		this->flag     = flag;
		this->fid      = fid;
		this->name     = name;
		this->filesize = filesize;
		this->offset   = offset;
		this->index    = index;
		this->size     = seg_size;
		this->data     = NULL;
	}

	FileFlag flag;           //文件任务标记
	string fid;              //文件的fid
	string name;             //文件名
	uint32_t filesize;       //文件的大小
	uint32_t offset;         //分片偏移位置
	uint32_t index;          //分片序号
	uint32_t size;           //分片大小
	const char *data;        //分片数据
};

class FileSaveResult
{
public:
	typedef enum
	{
		CREATE_FAILED,       //创建失败
		CREATE_SUCC,         //创建成功
		SEG_FAILED,          //分片接收成功
		SEG_SUCC,            //分片接收失败
	}Status;

	Status status;           //状态
	string fid;              //fid
	uint32_t index;          //分片index
};

#endif //_COMMON_TYPE_H_

