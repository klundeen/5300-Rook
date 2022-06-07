/**
 * @file btree.cpp - implementation of BTreeIndex, etc.
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2022"
 */
#include "btree.h"

BTreeIndex::BTreeIndex(DbRelation &relation, Identifier name, ColumnNames key_columns, bool unique) : DbIndex(relation,
                                                                                                              name,
                                                                                                              key_columns,
                                                                                                              unique),
                                                                                                      closed(true),
                                                                                                      stat(nullptr),
                                                                                                      root(nullptr),
                                                                                                      file(relation.get_table_name() +
                                                                                                           "-" + name),
                                                                                                      key_profile() {
    if (!unique)
        throw DbRelationError("BTree index must have unique key");
    build_key_profile();
}

BTreeIndex::~BTreeIndex() {
    delete stat;
    delete root;
}

// Create the index.
void BTreeIndex::create() {
    file.create();
    stat = new BTreeStat(file, STAT, STAT + 1, key_profile);
    root = new BTreeLeaf(file, stat->get_root_id(), key_profile, true);
    closed = false;
    Handles *table_rows = relation.select();
    for (auto const &row: *table_rows)
        insert(row);
    delete table_rows;
}

// Drop the index.
void BTreeIndex::drop() {
    file.drop();
}

// Open existing index. Enables: lookup, range, insert, delete, update.
void BTreeIndex::open() {
    if (closed) {
        file.open();
        stat = new BTreeStat(file, STAT, key_profile);
        if (stat->get_height() == 1)
            root = new BTreeLeaf(file, stat->get_root_id(), key_profile, false);
        else
            root = new BTreeInterior(file, stat->get_root_id(), key_profile, false);
        closed = true;
    }
}

// Closes the index. Disables: lookup, range, insert, delete, update.
void BTreeIndex::close() {
    if (!closed) {
        file.close();
        delete stat;
        stat = nullptr;
        delete root;
        root = nullptr;
        closed = true;
    }
}

// Find all the rows whose columns are equal to key. Assumes key is a dictionary whose keys are the column
// names in the index. Returns a list of row handles.
Handles *BTreeIndex::lookup(ValueDict *key_dict) const {
    return _lookup(this->root, stat->get_height(), this->tkey(key_dict));
}

Handles *BTreeIndex::_lookup(BTreeNode *node, uint height, const KeyValue *key) const {
    Handles* handles = new Handles;
    
    if (dynamic_cast<BTreeLeaf*>(node)) {
        try 
        { 
            handles->push_back(((BTreeLeaf*)node)->find_eq(key)); 
        }
        catch (...) 
        {
            std::cout << "Unknown Key" << std::endl;
        }
        return handles;
    }
    
    return _lookup(dynamic_cast<const BTreeInterior*>(node)->find(key, height), height - 1, key);
}

Handles *BTreeIndex::range(ValueDict *min_key, ValueDict *max_key) const {
    throw DbRelationError("Don't know how to do a range query on Btree index yet");
    // FIXME
}

// Insert a row with the given handle. Row must exist in relation already.
void BTreeIndex::insert(Handle handle) {
    open();
    ValueDict *key = relation.project(handle);
    KeyValue *tkey = this->tkey(key);
    Insertion insertion = _insert(root, stat->get_height(), tkey, handle);
    if (!BTreeNode::insertion_is_none(insertion)) {
        auto *new_root = new BTreeInterior(file, 0, key_profile, true);
        new_root->set_first(root->get_id());
        new_root->insert(&insertion.second, insertion.first);
        new_root->save();
        stat->set_root_id(new_root->get_id());
        stat->set_height(stat->get_height() + 1);
        stat->save();
        delete root;
        root = new_root;
        std::cout << "new root: " << *new_root << std::endl;
    }
    delete key;
    delete tkey;
}

// Recursive insert. If a split happens at this level, return the (new node, boundary) of the split.
Insertion BTreeIndex::_insert(BTreeNode *node, uint height, const KeyValue *key, Handle handle) {
    if (height == 1) {
        auto *leaf = dynamic_cast<BTreeLeaf *>(node);
        return leaf->insert(key, handle);
    } else {
        auto *interior = dynamic_cast<BTreeInterior *>(node);
        Insertion insertion = _insert(interior->find(key, height), height - 1, key, handle);
        if (!BTreeNode::insertion_is_none(insertion))
            insertion = interior->insert(&insertion.second, insertion.first);
        return insertion;
    }
}

void BTreeIndex::del(Handle handle) {
    throw DbRelationError("Don't know how to delete from a BTree index yet");
    // FIXME
}

KeyValue *BTreeIndex::tkey(const ValueDict *key) const {
    KeyValue *key_value = new KeyValue();
    for (auto const &column_name: key_columns)
        key_value->push_back(key->find(column_name)->second);
    return key_value;
}

// Figure out the data types of each key component and encode them in key_profile, a list of int/str classes.
void BTreeIndex::build_key_profile() {
    std::map<const Identifier, ColumnAttribute::DataType> types_by_colname;
    const ColumnAttributes column_attributes = relation.get_column_attributes();
    uint col_num = 0;
    for (auto const &column_name: relation.get_column_names()) {
        ColumnAttribute ca = column_attributes[col_num++];
        types_by_colname[column_name] = ca.get_data_type();
    }
    for (auto const &column_name: key_columns)
        key_profile.push_back(types_by_colname[column_name]);
}

bool test_btree() {
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    column_attributes.push_back(ColumnAttribute(ColumnAttribute::INT));
    column_attributes.push_back(ColumnAttribute(ColumnAttribute::INT));
    HeapTable table("__test_btree", column_names, column_attributes);
    table.create();
    ValueDict row1, row2;
    row1["a"] = Value(12);
    row1["b"] = Value(99);
    row2["a"] = Value(88);
    row2["b"] = Value(101);
    table.insert(&row1);
    table.insert(&row2);
    for (int i = 0; i < 100 * 1000; i++) {
        ValueDict row;
        row["a"] = Value(i + 100);
        row["b"] = Value(-i);
        table.insert(&row);
    }
    column_names.clear();
    column_names.push_back("a");
    BTreeIndex index(table, "fooindex", column_names, true);
    index.create();

    ValueDict lookup;
    lookup["a"] = 12;
    Handles *handles = index.lookup(&lookup);
    ValueDict *result = table.project(handles->back());
    if (*result != row1) {
        std::cout << "first lookup failed" << std::endl;
        return false;
    }
    delete handles;
    delete result;
    lookup["a"] = 88;
    handles = index.lookup(&lookup);
    result = table.project(handles->back());
    if (*result != row2) {
        std::cout << "second lookup failed" << std::endl;
        return false;
    }
    delete handles;
    delete result;
    lookup["a"] = 6;
    handles = index.lookup(&lookup);
    if (handles->size() != 0) {
        std::cout << "third lookup failed" << std::endl;
        return false;
    }
    delete handles;

    for (uint j = 0; j < 10; j++)
        for (int i = 0; i < 1000; i++) {
            lookup["a"] = i + 100;
            handles = index.lookup(&lookup);
            result = table.project(handles->back());
            row1["a"] = i + 100;
            row1["b"] = -i;
            if (*result != row1) {
                std::cout << "lookup failed " << i << std::endl;
                return false;
            }
            delete handles;
            delete result;
        }

    index.drop();
    table.drop();
    return true;
}

