/*
 * Master.h
 *
 *  Created on: 2013-08-08
 *      Author: tim
 */

#ifndef _MASTER_H_
#define _MASTER_H_

#include "IAppInterface.h"
#include "KVData.h"
using namespace easynet;

#include <string>
#include <map>
#include <list>
using namespace std;


#include <mysql++.h>
using namespace mysqlpp;

#include "Protocol.h"


class SavingFid
{
public:
	int insert_time;
	string fid;
};

//默认使用以下组件实例:
//    EventServer     : EventServerEpoll
//    ProtocolFactory : KVDataProtocolFactory
//    TransHandler    : TransHandler
//    ListenHandler   : ListenHandler
//    IMemory         : SystemMemory
class Master:public IAppInterface
{
public:
	Master();
	~Master();

//////////////////////////////////////////////////////////////////
//////////////////////////   接口方法   //////////////////////////
//////////////////////////////////////////////////////////////////
public:
	//启动App实例
	bool Start();

	//获取数据接收的超时时间(单位毫秒).从数据开始接收到收到完整的数据包所允许的时间.
	int32_t GetSocketRecvTimeout();

	//获取连接空闲的超时时间(单位毫秒秒).当连接在该时间内无任何读写事件发生的话,将发生超时事件.
	int32_t GetSocketIdleTimeout();

	//获取允许的最大链接数.
	int32_t GetMaxConnections();


	//处理收到的请求协议
	//  @param fd             : 收到协议的socket
	//  @param context        : 接收到的协议上下文
	//  @param detach_context : 被设置为trues时,由应用层控制context的生存期
	//                          应用层需要在适当的时候调用DeleteProtocolContext释放context实例;
	bool OnReceiveProtocol(int32_t fd, ProtocolContext *context, bool &detach_context);

	//处理发送协议的事件.协议数据完全发送到socket的缓冲区后调本接口
	//  @param fd             : 发送数据的socket
	//  @param context        : 发送成功的数据,应用层需要根据创建方式对齐进行释放
	void OnSendSucc(int32_t fd, ProtocolContext *context);

	//协议数据发送到缓冲区时发生错误后调用本接口
	//  @param fd             : 发送数据的socket
	//  @param context        : 发送失败的数据,应用层需要根据创建方式对齐进行释放
	void OnSendError(int32_t fd, ProtocolContext *context);

	//协议数据超时未完全发送到socket后调用本接口
	//  @param fd             : 发送数据的socket
	//  @param context        : 发送超时的数据,应用层需要根据创建方式对齐进行释放
	void OnSendTimeout(int32_t fd, ProtocolContext *context);

	//socket需要结束时调用本接口
	//  @param fd             : 需要结束的socket
	void OnSocketFinished(int32_t fd);

	//获取ProtocolFactory的实例
	IProtocolFactory* GetProtocolFactory();
private:
	//数据库
	string m_DBIP;
	int32_t m_DBPort;
	string m_DBUser;
	string m_DBPasswd;
	string m_DBName;
	Connection *m_DBConnection;

	int32_t m_SendTimeout;
	IProtocolFactory *m_ProtocolFactory;
private:
	//响应chunk的ping包
	void OnChunkPing(int fd, KVData *kv_data);
	//响应文件信息查询包
	void OnFileInfoReq(int fd, KVData *kv_data);
	//响应chunk发送fileinfo保存包
	void OnFileInfoSave(int fd, KVData *kv_data);
private:
	map<string, ChunkInfo> m_ChunkManager;
	bool AddChunk(ChunkInfo &chunkinfo);
	bool DispatchChunk(ChunkInfo &chunkinfo);
private:
	uint32_t m_SavingTaskTimeoutSec;
	map<string, FileInfo> m_FileInfoCache;
	list<SavingFid> m_SavingFidList;
	map<string, list<SavingFid>::iterator> m_SavingTaskMap;  //正在保存的任务
	bool GetFileInfo(string &fid, FileInfo &fileinfo);
	bool FindSavingTask(string &fid);
	bool AddSavingTask(string &fid);
	bool RemoveSavingTask(string &fid);
	void RemoveSavingTaskTimeout();

	bool DBSaveFileInfo(FileInfo &fileinfo);    //保存到数据库
private:
	DECL_LOGGER(logger);
};

#endif //_MASTER_H_


