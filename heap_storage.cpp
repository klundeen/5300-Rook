/**
 * @file heap_storage.cpp - Implementation of storage_engine.
 * SlottedPage: DbBlock
 * HeapFile: DbFile
 * HeapTable: DbRelation
 *
 * @author  Kevin Lundeen,
            Zhicong Zeng,
            Lolakumari Jayachandran, Vindhya Nair

 * @see "Seattle University, CPSC5300, Spring 2022"
**/
#include "heap_storage.h"
#include <cstring>
#include <iostream>

using namespace std;

typedef u_int16_t u16;

/**
 * @class SlottedPage - heap file implementation of DbBlock.
 *
 *      Manage a database block that contains several records.
        Modeled after slotted-page from Database Systems Concepts, 6ed, Figure 10-9.

        Record id are handed out sequentially starting with 1 as records are added with add().
        Each record has a header which is a fixed offset from the beginning of the block:
            Bytes 0x00 - Ox01: number of records
            Bytes 0x02 - 0x03: offset to end of free space
            Bytes 0x04 - 0x05: size of record 1
            Bytes 0x06 - 0x07: offset to record 1
            etc.
**/

// SlottedPage constructor:
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new)
{
    if (is_new)
    {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    }
    else
    {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id..
RecordID SlottedPage::add(const Dbt *data)
{
    if (!has_room(data->get_size() + 4))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = this->num_records + 1;
    u16 size = (u16)data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

// Get a record from the block. Return None if it has been deleted.
Dbt *SlottedPage::get(RecordID record_id)
{
    u16 size, loc;
    get_header(size, loc, record_id);
    if (loc == 0)
    {
        return nullptr; // this is just a tombstone, record has been deleted
    }
    return new Dbt(address(loc), loc + size);
}

// Replace the record with the given data. Raises ValueError if it won't fit.
void SlottedPage::put(RecordID record_id, const Dbt &data)
{
    u16 size, loc;
    get_header(size, loc, record_id);
    u16 new_size = (u16)data.get_size();

    if (new_size > size)
    {
        u16 extra = new_size - size;
        if (!this->has_room(extra))
        {
            throw DbBlockNoRoomError("Not enough room in block");
        }
        slide(loc + new_size, loc + size);
        // Copy data from [data] to [loc - extra, loc + new_size]
        // Copy length is (extra + new_size)
        memcpy(this->address(loc - extra), data.get_data(), extra + new_size);
    }
    else
    {
        // Copy data from [data] to [loc, loc + new_size]
        // Copy length is (new_size)
        memcpy(this->address(loc), data.get_data(), new_size);
        this->slide(loc + new_size, loc + size);
    }

    get_header(size, loc, record_id);
    put_header(record_id, new_size, loc);
}

// Mark the given id as deleted by changing its size to zero and its location to 0.
// Compact the rest of the data in the block. But keep the record ids the same for everyone.
void SlottedPage::del(RecordID record_id)
{
    u16 size, loc;
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0);
    this->slide(loc, loc + size);
}

// Sequence of all non-deleted record ids.
RecordIDs *SlottedPage::ids(void)
{
    RecordIDs *record_ids = new RecordIDs();
    u16 size, loc;
    for (RecordID record_id = 1; record_id < this->num_records; record_id++)
    {
        get_header(size, loc, record_id);
        if (loc != 0)
            record_ids->push_back(record_id);
    }
    return record_ids;
}

// Get the size and offset for given id. For id of zero, it is the block header.
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id)
{
    size = get_n(4 * id);
    loc = get_n(4 * id + 2);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc)
{
    if (id == 0)
    { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4 * id, size);
    put_n(4 * id + 2, loc);
}

// Calculate if we have room to store a record with given size.
// The size should include the 4 bytes
// for the header, too, if this is an add.
bool SlottedPage::has_room(u_int16_t size)
{
    u16 available = this->end_free - (this->num_records + 1) * 4;
    return (size <= available);
}

/**If start < end, then remove data from offset start up to but not including offset end by sliding data
            that is to the left of start to the right. If start > end, then make room for extra data from end to start
            by sliding data that is to the left of start to the left.
            Also fix up any record headers whose data has slid. Assumes there is enough room if it is a left
            shift (end < start).**/
void SlottedPage::slide(u_int16_t start, u_int16_t end)
{
    u16 shift = end - start;
    if (shift == 0)
    {
        return;
    }

    // slide data
    // Copy data from [end_free + 1 , end_free + 1 + shift] to [end_free + 1 + shift, end_free + end]
    // Copy length is (end - start)
    memcpy(this->address(this->end_free + 1 + shift), this->address(this->end_free + 1), shift);

    // fix up headers
    u16 size, loc;
    RecordIDs *record_ids = ids();
    for (auto const &record_id : *record_ids)
    {
        get_header(size, loc, record_id);
        if (loc <= start)
        {
            loc += shift;
            put_header(record_id, size, loc);
        }
    }
    this->end_free += shift;
    put_header();
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset)
{
    return *(u16 *)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n)
{
    *(u16 *)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void *SlottedPage::address(u16 offset)
{
    return (void *)((char *)this->block.get_data() + offset);
}

/**
 * @class HeapFile - heap file implementation of DbFile
 *
 * Heap file organization. Built on top of Berkeley DB RecNo file. There is one of our
        database blocks for each Berkeley DB record in the RecNo file. In this way we are using Berkeley DB
        for buffer management and file management.
        Uses SlottedPage for storing records within blocks.
**/

// HeapFile constructor has been declared in heap_storage.h

//Create physical file.
void HeapFile::create(void) {
    this->db_open(DB_CREATE|DB_EXCL);
	SlottedPage *block = get_new(); //first block of the file
	delete block;
}

//Delete the physical file
void HeapFile::drop(void) {
    close();
    db.remove(this->dbfilename.c_str(), nullptr, 0);
    //virtual int remove(const char *, const char *, u_int32_t);
}

//Open physical file.
void HeapFile::open(void) {
    db_open();
    this->db.set_re_len(DbBlock::BLOCK_SZ);//whats in the file overrides __init__ parameter
}

//Close the physical file.
void HeapFile::close(void) {
    db.close(0);
	closed = true;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage *HeapFile::get_new(void)
{
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage *page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

//Get a block from the database file.
SlottedPage* HeapFile::get(BlockID block_id) {
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0);
    //virtual int get(DbTxn *txnid, Dbt *key, Dbt *data, u_int32_t flags);
    return new SlottedPage(data, block_id);
}

//Write a block back to the database file.
//put blockid into block.block...
void HeapFile::put(DbBlock* block) {
    BlockID block_id(block->get_block_id());
	Dbt blockid(&block_id, sizeof(block_id));
	this->db.put(nullptr, &blockid, block->get_block(), 0);
    //virtual int put(DbTxn *, Dbt *, Dbt *, u_int32_t);
}

//Sequence of all block ids
BlockIDs* HeapFile::block_ids() {
    BlockIDs* id = new BlockIDs();
	for (BlockID i = 1; i <= this->last; i++)
		id->push_back(i);
	return id;
}

/**This Function is not implemented yet.
 * python solution doesn't contain this function
 u_int32_t HeapFile::get_last_block_id() {

}
**/

//Wrapper for Berkeley DB open, which does both open and creation.
void HeapFile::db_open(uint flags) {
    if (!this->closed) {
		return;
	}
	//this->db.set_re_len(this->block_size);//record length - will be ignored if file already exists
    this->db.set_re_len(DbBlock::BLOCK_SZ);//record length - will be ignored if file already exists

	this->dbfilename = "./" + this->name + ".db"; //The DbEnv path implement later
	this->db.open(nullptr, (this->dbfilename).c_str(), nullptr, DB_RECNO, flags, 0);//we always use record number files
    //Db::open(DbTxn*, const char*, const char*, DBTYPE, u_int32_t, int)????

    DB_BTREE_STAT *stat;
	this->db.stat(nullptr,  &stat , flags);
    //virtual int stat(DbTxn *, void *sp, u_int32_t flags);

	this->last = stat->bt_ndata;
    //u_int32_t bt_ndata;		/* Number of data items. */

	this->closed = false;
}

/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */

/**This Function is not implemented yet.
void HeapTable::create() {

}
**/

/**This Function is not implemented yet.
void HeapTable::create_if_not_exists() {

}
**/

/**This Function is not implemented yet.
void HeapTable::drop() {

}
**/

/**This Function is not implemented yet.
void HeapTable::open() {

}
**/

/**This Function is not implemented yet.
void HeapTable::close() {

}
**/

/**This Function is not implemented yet.
Handle HeapTable::insert(const ValueDict* row){

}
**/

/**This Function is not implemented yet.
void HeapTable::update(const Handle handle, const ValueDict* new_values) {

}
**/

/**This Function is not implemented yet.
void HeapTable::del(const Handle handle) {

}
**/

/**This Function is not implemented yet.
Handles* HeapTable::select() {

}
**/

// Select the specific handles from where
// Return a list of handles(rows)
Handles *HeapTable::select(const ValueDict *where)
{
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();
    for (auto const &block_id : *block_ids)
    {
        SlottedPage *block = file.get(block_id);
        RecordIDs *record_ids = block->ids();
        for (auto const &record_id : *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

/**This Function is not implemented yet.
ValueDict* HeapTable::project(Handle handle) {

}
**/

/**This Function is not implemented yet.
ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names){

}
**/

/**This Function is not implemented yet.
ValueDict* HeapTable::validate(const ValueDict* row) {

}
**/

/**This Function is not implemented yet.
Handle HeapTable::append(const ValueDict* row) {

}
**/

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt *HeapTable::marshal(const ValueDict *row)
{
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name : this->column_names)
    {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT)
        {
            *(int32_t *)(bytes + offset) = value.n;
            offset += sizeof(int32_t);
        }
        else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT)
        {
            uint size = value.s.length();
            *(u16 *)(bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        }
        else
        {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

/**This Function is not implemented yet.
ValueDict* HeapTable::unmarshal(Dbt* data) {

}
**/

// test function -- returns true if all tests pass
bool test_heap_storage()
{
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop(); // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles *handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
        return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
        return false;
    table.drop();

    return true;
}