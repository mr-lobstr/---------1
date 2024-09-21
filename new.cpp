#include <iostream>
#include <fstream>
#include <filesystem>
#include <sstream>
#include <string>
#include "nlohmann/json.hpp"
using namespace std;
using json = nlohmann::json;

struct Table
{
    string name;
    size_t size;
    string* columns;
};

struct Schema
{
    string name;
    size_t size;
    size_t limit;
    Table* tables;
};

Schema get_schema (string const& fName)
{
    ifstream jFile (fName);
    auto resp = json::parse (jFile);
    
    auto const& tables = resp["structure"];
    auto* pTables = new Table[tables.size()];
    size_t j = 0;

    for (auto it = tables.begin(); it != tables.end(); ++it)
    {
        auto const& name = it.key();
        auto const& columns = it.value();

        pTables[j].name = name;
        pTables[j].size = columns.size();
        pTables[j].columns = new string[columns.size()];

        for (size_t i = 0; i < columns.size(); ++i)
        {
            pTables[j].columns[i] = columns[i];
        }
        ++j;
    }

    return {
        resp["name"]
      , tables.size()
      , resp["tuples_limit"]
      , pTables
    };
}

void create_DB (Schema const& schema)
{
    using namespace filesystem;
    
    auto dataBaseDirPath = schema.name;
    create_directory(dataBaseDirPath);

    for (size_t i = 0; i < schema.size; ++i)
    {
        auto table = schema.tables[i];

        auto tableDirPath = dataBaseDirPath + "/" + table.name;
        create_directory(tableDirPath);

        ofstream firstPage (tableDirPath + "/1.csv");

        for (size_t j = 0; j < table.size; ++j)
        {
            firstPage << table.columns[j] << ";";
        }

        firstPage.close();
    }
}

void free_DB (Schema const& schema)
{
    for (size_t i = 0; i < schema.size; ++i)
    {
        auto table = schema.tables[i];
        delete[] table.columns;
    }

    delete[] schema.tables;
}

int main() 
{
    auto schema = get_schema ("tex.json");

    cout << schema.name << " "
         << schema.size << " "
         << schema.limit << " "
         << endl;

    for (size_t i = 0; i < schema.size; ++i)
    {
        cout << schema.tables[i].name << endl;
        for (size_t j = 0; j < schema.tables[i].size; ++j)
        {
            cout << "\t" << schema.tables[i].columns[j] << endl;
        }
    }

    create_DB(schema);
    free_DB(schema);
}