#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include "db_cxx.h"
#include "SQLParser.h"

// CREATE A DIRECTORY IN YOUR HOME DIR ~/cpsc5300/data before running this
const char *HOME = "cpsc5300/data";
const char *EXAMPLE = "example.db";
const unsigned int BLOCK_SZ = 4096;

using namespace std;
using namespace hsql;

string execute(const SQLStatement *stmt);
string printSelect(const SelectStatement *stmt);
string printCreate(const CreateStatement *stmt);
string columnDefinitionToString(const ColumnDefinition *col);
string printExpression(const Expr *expr);
string printTableRefInfo(const TableRef *table);
string printOperatorExpression(const Expr *expr);

int main(void)
{
    std::cout << "Have you created a dir: ~/" << HOME << "? (y/n) " << std::endl;
    std::string ans;
    std::cin >> ans;
    if (ans[0] != 'y')
        return 1;
    const char *home = std::getenv("HOME");
    std::string envdir = std::string(home) + "/" + HOME;

    DbEnv env(0U);
    env.set_message_stream(&std::cout);
    env.set_error_stream(&std::cerr);
    env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

    Db db(&env, 0);
    db.set_message_stream(env.get_message_stream());
    db.set_error_stream(env.get_error_stream());
    db.set_re_len(BLOCK_SZ);                                               // Set record length to 4K
    db.open(NULL, EXAMPLE, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644); // Erases anything already there

    char block[BLOCK_SZ];
    Dbt data(block, sizeof(block));
    int block_number;
    Dbt key(&block_number, sizeof(block_number));
    block_number = 1;
    strcpy(block, "hello!");
    db.put(NULL, &key, &data, 0); // write block #1 to the database

    Dbt rdata;
    db.get(NULL, &key, &rdata, 0); // read block #1 from the database
    std::cout << "Read (block #" << block_number << "): '" << (char *)rdata.get_data() << "'";
    std::cout << " (expect 'hello!')" << std::endl;

    // SQL entry
    while (true)
    {
        cout << "SQL> ";
        string sql;
        getline(cin, sql);

        if (sql == "quit")
            break;
        if (sql.length() < 1)
            continue;

        // Use SQLParser
        SQLParserResult *parser = SQLParser::parseSQLString(sql);
        // hsql::SQLParserResult *parser = hsql::SQLParser::parseSQLString(sql);

        if (!parser->isValid())
        {
            cout << "inValid SQL:" << sql << endl;
            cout << parser->errorMsg() << endl;
            continue;
        }
        else
        {
            for (uint i = 0; i < parser->size(); i++)
            {
                cout << execute(parser->getStatement(i)) << endl;
            }
        }
        free(parser);
    }
    return EXIT_SUCCESS;
}

// Sqlhelper function to handle different SQL
string execute(const SQLStatement *stmt)
{
    string str;
    switch (stmt->type())
    {
    case kStmtSelect:
        str += printSelect((const SelectStatement *)stmt);
        break;
    case kStmtCreate:
        str += printCreate((const CreateStatement *)stmt);
        break;
    /*We don't need these functions now
    case kStmtInsert:
        printInsertStatementInfo((const InsertStatement *)stmt, 0);
        break;
    case kStmtImport:
        printImportStatementInfo((const ImportStatement *)stmt, 0);
        break;
    case kStmtShow:
        inprint("SHOW", 0);
    */
    default:
        cout << "No implemented" << endl;
        break;
    }
    return str;
}

// Sqlhelper with printSelectStatementInfo
string printCreate(const CreateStatement *stmt)
{
    string str;
    str += "CREATE TABLE ";
    str += string(stmt->tableName) + " (";

    bool first = false;
    for (ColumnDefinition *col : *stmt->columns)
    {
        if (first)
        {
            str += ", ";
        }
        else
        {
            first = true;
        }
        str += columnDefinitionToString(col);
    }
    str += ")";
    return str;
}

/**
 * Convert the hyrise ColumnDefinition AST back into the equivalent SQL
 * @param col  column definition to unparse
 * @return     SQL equivalent to *col
 */
string columnDefinitionToString(const ColumnDefinition *col)
{
    string str;
    str += col->name;

    switch (col->type)
    {
    case ColumnDefinition::DOUBLE:
        str += " DOUBLE";
        break;
    case ColumnDefinition::INT:
        str += " INT";
        break;
    case ColumnDefinition::TEXT:
        str += " TEXT";
        break;
    default:
        str += " ...";
        break;
    }
    return str;
}

// Sqlhelper with printSelectStatementInfo
string printSelect(const SelectStatement *stmt)
{
    string str;
    str += "SELECT ";

    bool first = false;
    for (Expr *expr : *stmt->selectList)
    {
        if (first)
        {
            str += ", ";
        }
        else
        {
            first = true;
        }
        str += printExpression(expr);
    }

    str += " FROM " + printTableRefInfo(stmt->fromTable);

    if (stmt->whereClause != NULL)
        str += " WHERE " + printExpression(stmt->whereClause);

    /* This function has bug
    if (stmt->unionSelect != NULL)
        str += "Union:";
        str += printSelect(stmt->unionSelect); */

    if (stmt->order != NULL)
    {
        std::cout << "ORDER BY ";
        str += printExpression(stmt->order->at(0)->expr);
        if (stmt->order->at(0)->type == kOrderAsc)
        {
            std::cout << "ASCENDING ";
        }
        else
        {
            std::cout << "DESCENDING ";
        }
    }
    return str;
}

string printExpression(const Expr *expr)
{
    string str;
    switch (expr->type)
    {
    case kExprStar:
        str += "*";
        break;
    case kExprColumnRef:
        if (expr->table != NULL)
            str += string(expr->table) + ".";
        str += expr->name;
        break;
    // case kExprTableColumnRef: inprint(expr->table, expr->name, numIndent); break;
    case kExprLiteralFloat:
        str += to_string(expr->fval);
        break;
    case kExprLiteralInt:
        str += to_string(expr->ival);
        break;
    case kExprLiteralString:
        str += string(expr->name);
        break;
    case kExprFunctionRef:
        str += string(expr->name);
        str += string(expr->expr->name);
        break;
    case kExprOperator:
        str += printOperatorExpression(expr);
        break;
    default:
        str += "Unrecognized expression type %d\n";
        return str;
    }

    if (expr->alias != NULL)
    {
        str += " AS ";
        str += expr->alias;
    }
    return str;
}

string printTableRefInfo(const TableRef *table)
{
    string str;
    switch (table->type)
    {
    case kTableName:
        str += table->name;
        break;
    case kTableSelect:
        str += printSelect(table->select);
        break;
    case kTableJoin:
        str += printTableRefInfo(table->join->left);

        switch (table->join->type)
        {
        case kJoinInner:
            str += " JOIN ";
            break;
        case kJoinOuter:
            str += " OUTER JOIN ";
            break;
        case kJoinLeft:
            str += " LEFT JOIN ";
            break;
        case kJoinRight:
            str += " RIGHT JOIN ";
            break;
        default:
            cout << "Not yet implemented" << endl;
            /*      kJoinLeftOuter,
                    kJoinRightOuter,
                    kJoinCross,
                    kJoinNatural*/
            break;
        }
        str += printTableRefInfo(table->join->right);
        if (table->join->condition != NULL)
        {
            str += " ON " + printExpression(table->join->condition);
        }
        break;
    case kTableCrossProduct:
        bool first = false;
        for (TableRef *tbl : *table->list)
        {
            if (first)
            {
                str += ", ";
            }
            else
            {
                first = true;
            }
            str += printTableRefInfo(tbl);
        }
        break;
    }
    if (table->alias != NULL)
    {
        str += " AS ";
        str += table->alias;
    }
    return str;
}

string printOperatorExpression(const Expr *expr)
{
    string str;
    if (expr == NULL)
        return "null";
    if (expr->expr != NULL)
        str += printExpression(expr->expr) + " ";

    switch (expr->opType)
    {
    case Expr::SIMPLE_OP:
        str += expr->opChar;
        break;
    case Expr::AND:
        str += "AND";
        break;
    case Expr::OR:
        str += "OR";
        break;
    case Expr::NOT:
        str += "NOT";
        break;
    default:
        str += expr->opType;
        break;
    }
    if (expr->expr2 != NULL)
        str += " " + printExpression(expr->expr2);
    return str;
}