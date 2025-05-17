#include "catalog.h"
#include "query.h"
#include <cstring>

const Status QU_Delete(const string & relation, 
    const string & attrName, 
    const Operator op,
    const Datatype type, 
    const char *attrValue)
{
    Status status;
    HeapFileScan scan(relation, status);
    if (status != OK) return status;

    // If no attribute name is provided, delete all records
    if (attrName.empty() || attrValue == nullptr) {
        status = scan.startScan(0, 0, STRING, nullptr, EQ); // Will scan through all elements
        if (status != OK) return status;

        RID rid;
        while (scan.scanNext(rid) == OK) {  // Iterate through all records
            status = scan.deleteRecord();
            if (status != OK) {
                scan.endScan();
                return status;
            }
        }
        scan.endScan();
        return OK;
    }

    AttrDesc attrDesc;
    status = attrCat->getInfo(relation, attrName, attrDesc);    // Get target attribute info
    if (status != OK) return status;

    // Setup scan value (integer, float, or string)
    char *filterValue = new char[attrDesc.attrLen];
    switch (attrDesc.attrType) {
        case INTEGER: {
            int val = atoi(attrValue);
            memcpy(filterValue, &val, sizeof(int));
            break;
        }
        case FLOAT: {
            float val = atof(attrValue);
            memcpy(filterValue, &val, sizeof(float));
            break;
        }
        case STRING: {
            strncpy(filterValue, attrValue, attrDesc.attrLen);
            filterValue[attrDesc.attrLen - 1] = '\0'; 
            break;
        }
        default:
            delete[] filterValue;
            return ATTRTYPEMISMATCH;
    }

    // Start scan
    status = scan.startScan(
        attrDesc.attrOffset,
        attrDesc.attrLen,
        static_cast<Datatype>(attrDesc.attrType),
        filterValue,
        op
    );
    if (status != OK) {
        delete[] filterValue;
        return status;
    }

    // Iterate through each element matched by the scan (aka has to be deleted) and delete
    RID rid;
    while (scan.scanNext(rid) == OK) {
        status = scan.deleteRecord();
        if (status != OK) break;
    }

    scan.endScan();
    delete[] filterValue;
    return status;
}


//old version doesn't work 

// {
//     Status status;

//     HeapFileScan scan(relation, status);
//     if (status != OK) return status;

//     if (attrName.empty() || attrValue == nullptr) {
//         status = scan.startScan(0, 0, STRING, NULL, EQ); // unconditional scan
//         if (status != OK) return status;

//         RID rid;
//         while (scan.scanNext(rid) == OK) {
//             status = scan.deleteRecord();
//             if (status != OK) return status;
//         }

//         scan.endScan();
//         return OK;
//     }

//     // Get attribute info for filtering
//     AttrDesc attrDesc;
//     status = attrCat->getInfo(relation, attrName.c_str(), attrDesc);
//     if (status != OK) return status;

//     // 4. Convert attrValue to binary format
//     char *filterValue = new char[attrDesc.attrLen];
//     switch (type) {
//         case INTEGER: {
//             int val = atoi(attrValue);
//             memcpy(filterValue, &val, sizeof(int));
//             break;
//         }
//         case FLOAT: {
//             float val = atof(attrValue);
//             memcpy(filterValue, &val, sizeof(float));
//             break;
//         }
//         case STRING: {
//             strncpy(filterValue, attrValue, attrDesc.attrLen);
//             break;
//         }
//         default:
//             delete[] filterValue;
//             return ATTRTYPEMISMATCH;
//     }

//     // start the filtered scan
//     status = scan.startScan(attrDesc.attrOffset,
//                             attrDesc.attrLen,
//                             type,
//                             filterValue,
//                             op);
//     if (status != OK) {
//         delete[] filterValue;
//         return status;
//     }

//     RID rid;
//     while (scan.scanNext(rid) == OK) {
//         status = scan.deleteRecord();
//         if (status != OK) {
//             delete[] filterValue;
//             return status;
//         }
//     }

//     scan.endScan();
//     delete[] filterValue;
//     return OK;
// }
