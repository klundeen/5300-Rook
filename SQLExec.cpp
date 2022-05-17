/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2022"
 * start of this code provided by Professor Lundeen for milestone 3
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) 
{
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

QueryResult::~QueryResult() 
{
    if (column_names != nullptr)
    {
        delete column_names;
    }
    if (column_attributes != nullptr)
    {
        delete column_attributes;
    }
    if (rows != nullptr)
    {
        for (auto row : *rows)
        {
            delete row;
        }
        delete rows;
    }
}


QueryResult *SQLExec::execute(const SQLStatement *statement) 
{
    if (SQLExec::tables == nullptr)
    {
        SQLExec::tables = new Tables();
    }

    if (SQLExec::indices == nullptr)
    {
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
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) 
{
    column_name = col->name;
    switch (col->type)
    {
    case ColumnDefinition::INT:
        column_attribute.set_data_type(ColumnAttribute::INT);
        break;
    case ColumnDefinition::TEXT:
        column_attribute.set_data_type(ColumnAttribute::TEXT);
        break;
    default:
        throw SQLExecError("invalid ColumnAttribute type");
    }
}

QueryResult *SQLExec::create(const CreateStatement *statement) 
{
    if (statement->type == CreateStatement::kTable)
    {
        return create_table(statement);
    }
    else if (statement->type == CreateStatement::kIndex)
    {
        return create_index(statement);
    }
    else
    {
        return new QueryResult("creating a type that isn't supported right now");
    }
}

QueryResult *SQLExec::create_table(const CreateStatement *statement) 
{
    // Update schema
    Identifier table_name = statement->tableName;
    ValueDict row;
    row["table_name"] = table_name;
    Handle table_handle = SQLExec::tables->insert(&row);

    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;

    // Get all column names and attributes
    for(ColumnDefinition *column : *statement->columns)
    {
        column_definition(column, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    try 
    {
        // Update columns schema
        Handles column_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try 
        {
            for(uint i = 0; i < column_names.size(); i++)
            {
                row["column_name"] = column_names[i];
                if(column_attributes[i].get_data_type() == ColumnAttribute::INT)
                {
                    row["data_type"] = Value("INT");
                }
                else
                {
                    row["data_type"] = Value("TEXT");
                }
                column_handles.push_back(columns.insert(&row));
            }
            // Create table
            DbRelation &table = SQLExec::tables->get_table(table_name);
            if(statement->ifNotExists)
            {
                table.create_if_not_exists();
            }
            else
            {
                table.create();
            }
        }
        catch(...)
        {
            // Attempt to undo the insertions into columns
            try 
            {
                for(auto const &column_handle : column_handles)
                {
                    columns.del(column_handle);
                }
            }
            catch(...) 
            {
            }
            throw;
        }
    }
    catch(...)
    {
        // Attempt to undo the insertions into tables
        try 
        {
            SQLExec::tables->del(table_handle);
        }
        catch(...) 
        {
        }
        throw;
    }
    return new QueryResult("Created " + table_name);
}

QueryResult *SQLExec::create_index(const CreateStatement *statement)
{
    // Update schema
    Identifier table_name = statement->tableName;
    Identifier index_name = statement->indexName;
    Identifier index_type = statement->indexType;
    ValueDict row;
    row["table_name"] = Value(table_name);
    row["index_name"] = Value(index_name);
    row["index_type"] = Value(index_type);
    row["is_unique"] = Value(index_type == "BTREE");
    Handles indexHandles;

    //adding index columns then create index 
    try
    {
        int seq = 0;
        for(auto const &column : *statement->indexColumns) {
            row["seq_in_index"] = Value(++seq);
            row["column_name"] = Value(column);
            indexHandles.push_back(SQLExec::indices->insert(&row));
        }

        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.create();
    }
    catch (DbRelationError &e)
    {
        // if error happens, revert the insertion of index
        try
        {
            for(auto const &handle : indexHandles) {
                SQLExec::indices->del(handle);
            }
        }
        catch (DbRelationError &e) {}
        throw;
    }

    return new QueryResult("Created new index " + index_name);
}


// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) 
{
    if (statement->type == DropStatement::kTable)
    {
        return drop_table(statement);
    }
    else if (statement->type == DropStatement::kIndex)
    {
        return drop_index(statement);
    }
    else
    {
        return new QueryResult("creating a type that isn't supported right now");
    }
}

QueryResult *SQLExec::drop_table(const DropStatement *statement) 
{
    Identifier table_name = statement->name;

    // Check the table is not a schema table
    if(table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
    {
        return new QueryResult("Cannot drop a schema table!");
    }

    // get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // remove table
    table.drop();

    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the columns
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    
    Handles *handles = columns.select(&where);
    for (auto const &handle : *handles)
    {
        columns.del(handle);
    }
    delete handles;

    // finally, remove from table schema
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());

    return new QueryResult("dropped" + table_name);
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) 
{
    //follow the same idea of dropping table. 
    Identifier table_name = statement->name;
    Identifier indexName = statement->indexName;

    try {
        DbIndex &index = SQLExec::indices->get_index(table_name, indexName);
    index.drop();
    }
    catch(...) {
        return new QueryResult("Index not found");
    }

    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(indexName);
    Handles *handles = SQLExec::indices->select(&where);
    
    for (auto const &handle : *handles)
    {
        SQLExec::indices->del(handle);
    }
    delete handles;

    return new QueryResult("dropped index " + indexName + " from " + table_name); 
}



QueryResult *SQLExec::show(const ShowStatement *statement) 
{
    switch(statement->type)
    {
        case ShowStatement::kTables:
            return show_tables();
            break;
        case ShowStatement::kColumns:
            return show_columns(statement);
            break;
        case ShowStatement::kIndex:
            return show_index(statement);
            break;
        default:
            return new QueryResult("Not implemented");
    }
}

QueryResult *SQLExec::show_tables() 
{
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    
    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    // Get all entries from the tables
    ValueDicts *rows = new ValueDicts();
    Handles *handles = SQLExec::tables->select();
    int size = 0;

    for(auto const &handle : *handles)
    {
        // Get all entries from the column
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        // Check if table name is already in the schema
        Identifier table_name = row->at("table_name").s;
        if(table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
        {
            rows->push_back(row);
            size++;
        }
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(size) + " rows");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) 
{
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");
    ColumnAttributes *column_attributes = new ColumnAttributes;

    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    // Used to store the data
    ValueDicts *rows = new ValueDicts();

    // Used to locate the table
    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    
    Handles *handles = SQLExec::tables->get_table(Columns::TABLE_NAME).select(&where);
    int count = handles->size();

    for (auto const &handle : *handles)
    {
        ValueDict *row = SQLExec::tables->get_table(Columns::TABLE_NAME).project(handle, column_names);
        rows->push_back(row);
    }

    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(count) + " rows");
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) 
{
    //create columns for index
    ColumnNames *column_names = new ColumnNames();
    column_names->push_back("table_name");
    column_names->push_back("index_name");
    column_names->push_back("column_name");
    column_names->push_back("seq_in_index");
    column_names->push_back("index_type");
    column_names->push_back("is_unique");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);

    Handles *handles = SQLExec::indices->select(&where);
    int size = handles->size();

    ValueDicts *rows = new ValueDicts();

    for (auto const &handle : *handles)
    {
        ValueDict *row = SQLExec::indices->project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(size) + " rows");
}



// Test cases from Peacock, can also be found on Canvas under Milestone3
// Output matches Canvas Milestone3 test cases expected output
// For accurate testing, make sure no tables present prior to running test cases
// Test Function for SQLExec class
bool test_sqlexec_table()
{
    const int num_queries = 11;
    const string queries[num_queries] = {"show tables",
                                         "show columns from _tables",
                                         "show columns from _columns",
                                         "create table foo (id int, data text, x integer, y integer, z integer)",
                                         "create table foo (goober int)",
                                         "create table goo (x int, x text)",
                                         "show tables",
                                         "show columns from foo",
                                         "drop table foo",
                                         "show tables",
                                         "show columns from foo"};
    bool passed = true;
    const string results[num_queries] = {"SHOW TABLES table_name  successfully returned 0 rows",
                                         "SHOW COLUMNS FROM _tables table_name column_name data_type _tables table_name TEXT successfully returned 1 rows",
                                         "SHOW COLUMNS FROM _columns   table_name column_name data_type _columns   table_name TEXT_columns column_name  TEXT_columns data_type TEXT   successfully returned 3 rows",
                                         "CREATE TABLE foo (id INT, data TEXT, x INT, y INT, z INT)  created foo",
                                         "CREATE TABLE foo (goober INT)  Error: DbRelationError: foo already exists",
                                         "Error: DbRelationError: duplicate column goo.x",
                                         "SHOW TABLES  table_name foo successfully returned 1 rows",
                                         "SHOW COLUMNS FROM foo  table_name column_name data_type foo id INT foo data TEXT  foo x INT  foo y INT  foo z INT  successfully returned 5 rows",
                                         "DROP TABLE foo   dropped foo",
                                         "SHOW TABLES  table_name  successfully returned 0 rows",
                                         "SHOW COLUMNS FROM footable_name column_name data_type  successfully returned 0 rows"};

    for (int i = 0; i < num_queries; i++)
    {
        SQLParserResult *result = SQLParser::parseSQLString(queries[i]);
        if (result->isValid())
        {
            // if result is valid, pass result to our own execute function
            for (long unsigned int j = 0; j < result->size(); j++)
            {
                const SQLStatement *statement = result->getStatement(j);
                try
                {
                    string str1 = test_logic((const SQLStatement *)statement);
                    string str2 = str1;
                    str2.erase(remove_if(str2.begin(), str2.end(), [](char c)
                               { return !(isalnum(c)); }),
                               str2.end());
                    string str3 = results[i];
                    str3.erase(remove_if(str3.begin(), str3.end(), [](char c)
                               { return !(isalnum(c)); }),
                               str3.end());
                    // ADDITION: make test cases case-insensitive
                    for(long unsigned int k = 0; k < str2.length(); k++)
                    {
                        str2[k] = tolower(str2[k]);
                    }
                    for(long unsigned int k = 0; k < str3.length(); k++)
                    {
                        str3[k] = tolower(str3[k]);
                    }
                    if (str2 == str3)
                    {
                        cout << "query_result  " << str2 << endl;
                        cout << queries[i] << endl;
                        cout << str1 << endl;
                    }
                    else
                    {
                        cout << "Unexpected query  " << queries[i] << endl;
                        cout << "query_result  " << str2 << endl;
                        cout << "results[i]  " << str3 << endl;
                        passed = false;
                    }
                }
                catch (SQLExecError &e)
                {
                    cout << "Error: " << e.what() << endl;
                }
            }
        }
        else
        {
            passed = false;
            cout << "Invalid SQL" << endl;
        }
        delete result;
    }
    return passed;
}

// Test cases from Peacock, can also be found on Canvas under Milestone4
// Output matches Canvas Milestone4 test cases expected output
// For accurate testing, make sure no tables present prior to running test cases
// Test Function for SQLExec class
bool test_sqlexec_index()
{
    const int num_queries = 18;
    const string queries[num_queries] = {"create table goober (x integer, y integer, z integer)",
                                         "show tables",
                                         "show columns from goober",
                                         "create index fx on goober (x,y)",
                                         "show index from goober",
                                         "drop index fx from goober",
                                         "show index from goober",
                                         "create index fx on goober (x)",
                                         "show index from goober",
                                         "create index fx on goober (y,z)",
                                         "show index from goober",
                                         "create index fyz on goober (y,z)",
                                         "show index from goober",
                                         "drop index fx from goober",
                                         "show index from goober",
                                         "drop index fyz from goober",
                                         "show index from goober",
                                         "drop table goober"};
    bool passed = true;
    const string results[num_queries] = {"CREATE TABLE goober x INT yINT zINT created goober",
                                         "SHOW TABLES table_name goober successfully returned 1 rows",
                                         "SHOW COLUMNS FROM goober table_name column_name data_type goober x INT goober y INT goober z INT successfully returned 3 rows",
                                         "CREATE INDEX fx ON goober USING BTREE x y created index fx",
                                         "SHOW INDEX FROM goober table_name index_name column_name seq_in_index index_type is_unique goober fx x 1 BTREE true goober fx y 2 BTREE true successfully returned 2 rows",
                                         "DROP INDEX fx FROM goober dropped index fx",
                                         "SHOW INDEX FROM goober table_name index_name column_name seq_in_index index_type is_unique successfully returned 0 rows",
                                         "CREATE INDEX fx ON goober USING BTREE x created index fx",
                                         "SHOW INDEX FROM goober table_name index_name column_name seq_in_index index_type is_unique goober fx x 1 BTREE true successfully returned 1 rows",
                                         "CREATE INDEX fx ON goober USING BTREE (y, z) Error: DbRelationError: duplicate index goober fx",
                                         "SHOW INDEX FROM goober table_name index_name column_name seq_in_index index_type is_unique goober fx x 1 BTREE true successfully returned 1 rows",
                                         "CREATE INDEX fyz ON goober USING BTREE y z created index fyz",
                                         "SHOW INDEX FROM goober table_name index_name column_name seq_in_index index_type is_unique goober fx x 1 BTREE true goober fyz y 1 BTREE true goober fyz z 2 BTREE true successfully returned 3 rows",
                                         "DROP INDEX fx FROM goober dropped index fx",
                                         "SHOW INDEX FROM goober table_name index_name column_name seq_in_index index_type is_unique goober fyz y 1 BTREE true goober fyz z 2 BTREE true successfully returned 2 rows",
                                         "DROP INDEX fyz FROM goober dropped index fyz",
                                         "SHOW INDEX FROM goober table_name index_name column_name seq_in_index index_type is_unique successfully returned 0 rows",
                                         "DROP TABLE goober dropped goober"};

    for (int i = 0; i < num_queries; i++)
    {
        SQLParserResult *result = SQLParser::parseSQLString(queries[i]);
        if (result->isValid())
        {
            // if result is valid, pass result to our own execute function
            for (long unsigned int j = 0; j < result->size(); j++)
            {
                const SQLStatement *statement = result->getStatement(j);
                try
                {
                    string str1 = test_logic((const SQLStatement *)statement);
                    string str2 = str1;
                    str2.erase(remove_if(str2.begin(), str2.end(), [](char c)
                               { return !(isalnum(c)); }),
                               str2.end());
                    string str3 = results[i];
                    str3.erase(remove_if(str3.begin(), str3.end(), [](char c)
                               { return !(isalnum(c)); }),
                               str3.end());
                    // ADDITION: make test cases case-insensitive
                    for(long unsigned int k = 0; k < str2.length(); k++)
                    {
                        str2[k] = tolower(str2[k]);
                    }
                    for(long unsigned int k = 0; k < str3.length(); k++)
                    {
                        str3[k] = tolower(str3[k]);
                    }
                    if (str2 == str3)
                    {
                        cout << queries[i] << endl;
                        cout << str1 << endl;
                    }
                    else
                    {
                        cout << "Unexpected query  " << queries[i] << endl;
                        cout << "query_result  " << str2 << endl;
                        cout << "results[i]  " << str3 << endl;
                        passed = false;
                    }
                }
                catch (SQLExecError &e)
                {
                    cout << "Error: " << e.what() << endl;
                }
            }
        }
        else
        {
            passed = false;
            cout << "Invalid SQL" << endl;
        }
        delete result;
    }
    return passed;
}

// Test cases from Peacock, can also be found on Canvas under Milestone3 and Milestone4
string test_logic(const SQLStatement *statement)
{
    QueryResult *query_result = SQLExec::execute(statement);
    std::stringstream buffer;
    buffer << ParseTreeToString::statement(statement) << std::endl;
    buffer << *query_result << std::endl;
    string str1 = buffer.str();

    delete query_result;
    query_result = nullptr;
    return buffer.str();
}
