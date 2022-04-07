#include "sql_to_String.h"

void sql_to_String(SQLParserResult* result){
    for (uint i = 0; i < result->size(); ++i) {
        // Print a statement summary.
        auto st = result->getStatement(i);
        hsql::printStatementInfo(st);
        if (st->type() == kStmtCreate)
            cout << "CREATE";
    }
}