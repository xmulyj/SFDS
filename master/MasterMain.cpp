/*
 * MasterMain.cpp
 *
 *  Created on: 2013-08-08
 *      Author: tim
 */

#include "Master.h"

int main(int argc, char *argv[])
{
	INIT_LOGGER("../config/log4cplus.conf");

	Master application;
	application.Start();
	
	return 0;
}

