/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2022"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row: *rows)
            delete row;
        delete rows;
    }
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
        SQLExec::indices = new Indices();
    }

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            case kStmtInsert:
                return insert((const InsertStatement *) statement);
            case kStmtDelete:
                return del((const DeleteStatement *) statement);
            case kStmtSelect:
                return select((const SelectStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

// Insert into a table and indices
QueryResult *SQLExec::insert(const InsertStatement *statement) {
    Identifier tableName;
    ColumnNames colNames;
    vector<Value> vals;
    IndexNames indexNames;
    ValueDict row;
    Handle handle;
    uint numIndices;
    
    tableName = statement->tableName;
    DbRelation &table = SQLExec::tables->get_table(tableName);
    
    if (statement->columns != nullptr) {
        for (char* col: *statement->columns) {
            colNames.push_back(col);
        }
    }
    else {
        colNames = table.get_column_names();
    }


    for (uint i = 0; i < colNames.size(); i++) {
        if (i >= statement->values->size())
            goto EXIT;

        Expr* val = statement->values->at(i);

        switch (val->type)
        {
            case ExprType::kExprLiteralInt:
                vals.push_back(Value(val->ival));
                break;
            case ExprType::kExprLiteralString:
                vals.push_back(Value(val->name));
                break;
            default:
                throw SQLExecError("Unsupported data type");
        }
    }

    try {
        indexNames = SQLExec::indices->get_index_names(tableName);

        for (uint i = 0; i < colNames.size(); i++) {
            row[colNames.at(i)] = vals.at(i);
        }

        if (row.size() < 2)
            goto EXIT;

        handle = table.insert(&row);
    }
    catch (...) {
        table.del(handle);
        throw SQLExecError("Error inserting into table");
    }

    numIndices = indexNames.size();

    try {
        for (uint i = 0; i < numIndices; i++) {
            DbIndex &index = SQLExec::indices->get_index(tableName, indexNames.at(i));
            index.insert(handle);
        }
    }
    catch (...) {
        for (uint i = 0; i < numIndices; i++) {
            DbIndex &index = SQLExec::indices->get_index(tableName, indexNames.at(i));
            index.del(handle);
        }
        throw SQLExecError("Error inserting into index");
    }

    return new QueryResult("successfully inserted 1 row into " + tableName +
                           (numIndices == 0 ? "" : (" and " + to_string(numIndices) +
                           (numIndices == 1 ? " index" : " indices"))));

EXIT:
    throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
}

// Delete a table and any indices present
QueryResult *SQLExec::del(const DeleteStatement *statement) {
    Identifier tableName;
    EvalPlan *plan;
    EvalPipeline pipeline;
    IndexNames indexNames;
    Handles *handles;
    uint numRows;
    uint numIndices;
    
    tableName = statement->tableName;
    DbRelation &table = SQLExec::tables->get_table(tableName);
    plan = new EvalPlan(table);

    if (statement->expr != nullptr) {
        plan = new EvalPlan(get_where_conjunction(statement->expr, &table.get_column_names()), plan);
    }
    else {
        plan = plan->optimize();
    }

    pipeline = plan->pipeline();
    
    indexNames = SQLExec::indices->get_index_names(tableName);
    handles = pipeline.second;
    for (auto const& indexName: indexNames) {
        DbIndex &index = SQLExec::indices->get_index(tableName, indexName);
        for (auto const& handle: *handles) {
            index.del(handle);
        }
    }
    
    for (auto const& handle: *handles) {
        pipeline.first->del(handle);
    }
    
    numRows = handles->size();
    numIndices = indexNames.size();

    return new QueryResult("successfully deleted " + to_string(numRows) +
                           (numRows == 1 ? " row" : " rows") + " from " +
                           tableName +
                           (numIndices == 0 ? "" : (" and " + to_string(numIndices) +
                           (numIndices == 1 ? " index" : " indices"))));
}

QueryResult *SQLExec::select(const SelectStatement *statement) { 
    //table and columns
    Identifier table_name = statement->fromTable->name;
    DbRelation &table = SQLExec::tables->get_table(table_name);
    ColumnNames *column_names = new ColumnNames;
    //Evalplan 
    EvalPlan* plan;


    //iterate over select list 
    for(auto const &e : *statement->selectList){
        if(e->type == kExprStar){
            for(auto const column : table.get_column_names()){
                column_names->push_back(column);
            }
        }
        else if(e->type == kExprColumnRef){
            column_names->push_back(e->name);
        }
        else return new QueryResult("Invalid selection");
    }

    //If there is a where clause...
    plan = new EvalPlan(table);
    if(statement->whereClause != nullptr){
        plan = new EvalPlan(get_where_conjunction(statement->whereClause, &table.get_column_names()), plan);
    }
    //projection
    plan = new EvalPlan(column_names, plan);
    //optimize
    EvalPlan *optimize = plan->optimize();
    ValueDicts* rows = optimize->evaluate();
    ColumnAttributes *column_attributes = table.get_column_attributes(*column_names);    
    return new QueryResult(column_names, column_attributes, rows, "successufly returned " + to_string(rows->size()) + " rows"); 
}

// Pull out conjunctions of equality predicates from parse tree
ValueDict *SQLExec::get_where_conjunction(const hsql::Expr *parseWhere, const ColumnNames *columnNames) {
    ValueDict* where;
    ValueDict* whereDict;
    
    whereDict = new ValueDict;
    
    if (parseWhere == nullptr || parseWhere->type != kExprOperator)
        throw SQLExecError("Invalid where clause expression");
    
    switch (parseWhere->opType) {
        case Expr::SIMPLE_OP:
            switch (parseWhere->expr2->type) {
                case ExprType::kExprLiteralInt:
                    whereDict->insert(pair<Identifier, Value>(parseWhere->expr->name, Value(parseWhere->expr2->ival)));
                    break;
                case ExprType::kExprLiteralString:
                    whereDict->insert(pair<Identifier, Value>(parseWhere->expr->name, Value(parseWhere->expr2->name)));
                    break;
                default:
                    throw SQLExecError("Only supports INT and TEXT expression types, not " + parseWhere->expr2->type);
            }
            break;
        case Expr::AND:
            where = get_where_conjunction(parseWhere->expr, columnNames);
            whereDict->insert(where->begin(), where->end());
            where = get_where_conjunction(parseWhere->expr2, columnNames);
            whereDict->insert(where->begin(), where->end());
            whereDict->insert(where->begin(), where->end());
            break;
        default:
            throw SQLExecError("Only supports AND conjunctions, not " + parseWhere->opType);
    }
    
    return whereDict;
}

void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_name = col->name;
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        case ColumnDefinition::DOUBLE:
        default:
            throw SQLExecError("unrecognized data type");
    }
}

// CREATE ...
QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    for (ColumnDefinition *col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Add to schema: _tables and _columns
    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
    try {
        Handles c_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));  // Insert into _columns
            }

            // Finally, actually create the relation
            DbRelation &table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (...) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: c_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }

    } catch (exception &e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(t_handle);
        } catch (...) {}
        throw;
    }
    return new QueryResult("created " + table_name);
}

QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    Identifier index_name = statement->indexName;
    Identifier table_name = statement->tableName;

    // get underlying relation
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // check that given columns exist in table
    const ColumnNames &table_columns = table.get_column_names();
    for (auto const &col_name: *statement->indexColumns)
        if (find(table_columns.begin(), table_columns.end(), col_name) == table_columns.end())
            throw SQLExecError(string("Column '") + col_name + "' does not exist in " + table_name);

    // insert a row for every column in index into _indices
    ValueDict row;
    row["table_name"] = Value(table_name);
    row["index_name"] = Value(index_name);
    row["index_type"] = Value(statement->indexType);
    row["is_unique"] = Value(string(statement->indexType) == "BTREE"); // assume HASH is non-unique --
    int seq = 0;
    Handles i_handles;
    try {
        for (auto const &col_name: *statement->indexColumns) {
            row["seq_in_index"] = Value(++seq);
            row["column_name"] = Value(col_name);
            i_handles.push_back(SQLExec::indices->insert(&row));
        }

        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.create();

    } catch (...) {
        // attempt to remove from _indices
        try {  // if any exception happens in the reversal below, we still want to re-throw the original ex
            for (auto const &handle: i_handles)
                SQLExec::indices->del(handle);
        } catch (...) {}
        throw;  // re-throw the original exception (which should give the client some clue as to why it did
    }
    return new QueryResult("created index " + index_name);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // remove any indices
    for (auto const &index_name: SQLExec::indices->get_index_names(table_name)) {
        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.drop();  // drop the index
    }
    Handles *handles = SQLExec::indices->select(&where);
    for (auto const &handle: *handles)
        SQLExec::indices->del(handle);  // remove all rows from _indices for each index on this table
    delete handles;

    // remove from _columns schema
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    handles = columns.select(&where);
    for (auto const &handle: *handles)
        columns.del(handle);
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    handles = SQLExec::tables->select(&where);
    SQLExec::tables->del(*handles->begin()); // expect only one row from select
    delete handles;

    return new QueryResult(string("dropped ") + table_name);
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;

    // drop index
    DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
    index.drop();

    // remove rows from _indices for this index
    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);
    Handles *handles = SQLExec::indices->select(&where);
    for (auto const &handle: *handles)
        SQLExec::indices->del(handle);
    delete handles;

    return new QueryResult("dropped index " + index_name);
}

// SHOW ...
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    ColumnNames *column_names = new ColumnNames;
    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_names->push_back("table_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("index_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("column_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("seq_in_index");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::INT));

    column_names->push_back("index_type");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("is_unique");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::BOOLEAN));

    ValueDict where;
    where["table_name"] = Value(string(statement->tableName));
    Handles *handles = SQLExec::indices->select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::indices->project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_tables() {
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles *handles = SQLExec::tables->select();
    u_long n = handles->size() - 3;

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
            rows->push_back(row);
        else
            delete row;
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles *handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

// Test Function for Milestone 5
bool test_queries() {
    const int num_queries = 28;
    const string queries[num_queries] = {"show tables",
                                         "create table foo (id int, data text)",
                                         "show tables",
                                         "show columns from foo",
                                         "create index fx on foo (id)",
                                         "create index fz on foo (data)",
                                         "show index from foo",
                                         "insert into foo (id, data) values (1,\"one\")",
                                         "select * from foo",
                                         "insert into foo values (2, \"Two\"); insert into foo values (3, \"Three\"); insert into foo values (99, \"wowzers, Penny!!\")",
                                         "select * from foo",
                                         "select * from foo where id=3",
                                         "select * from foo where id=1 and data=\"one\"",
                                         "select * from foo where id=99 and data=\"nine\"",
                                         "select id from foo",
                                         "select data from foo where id=1",
                                         "delete from foo where id=1",
                                         "select * from foo",
                                         "delete from foo",
                                         "select * from foo",
                                         "insert into foo values (2, \"Two\"); insert into foo values (3, \"Three\"); insert into foo values (99, \"wowzers, Penny!!\")",
                                         "select * from foo",
                                         "drop index fz from foo",
                                         "show index from foo",
                                         "insert into foo (id) VALUES (100)",
                                         "select * from foo",
                                         "drop table foo",
                                         "show tables"};
    bool passed = true;

    for (int i = 0; i < num_queries; i++) {
        cout << "SQL> " << queries[i] << endl;
        SQLParserResult *result = SQLParser::parseSQLString(queries[i]);
        if(result->isValid()) {
            //if result is valid, pass result to our own execute function
            for(long unsigned int i = 0; i < result->size(); ++i) {
                const SQLStatement *statement = result->getStatement(i);
                try {
                    QueryResult *query_result = SQLExec::execute(statement);
                    cout << *query_result << endl;
                    delete query_result;
                } 
                catch (SQLExecError &e) {
                    cout << "Error: " << e.what() << endl;
                }
            }
        }
        else {
            passed = false;
            cout << "Invalid SQL" << endl;
        }
        delete result;
    }
    
    return passed;
}
