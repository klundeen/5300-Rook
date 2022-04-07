#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include "SQLParser.h"
#include "sqlhelper.h"

using namespace std;
using namespace hsql;
void sql_to_String(SQLParserResult* sql);
