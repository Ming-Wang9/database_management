#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

 const Status QU_Insert(const string & relation, 
    const int attrCnt, 
    const attrInfo attrList[])
{
    Status status;

    int schemaCnt;
    AttrDesc* schema = nullptr;
    status = attrCat->getRelInfo(relation, schemaCnt, schema);  // Get target relation info
    if (status != OK) return status;

    int totalLen = 0; 
    for (int i = 0; i < schemaCnt; i++)
        totalLen += schema[i].attrLen;

    char* tuple = new char[totalLen];
    memset(tuple, 0, totalLen);

    // Iterate through the schema to find spot where to insert
    for (int i = 0; i < schemaCnt; i++) {
        bool matched = false;
        for (int j = 0; j < attrCnt; j++) {
            if (strcmp(schema[i].attrName, attrList[j].attrName) == 0) {
                matched = true;

                // Match found, write attribute value based on write type 
                switch (schema[i].attrType) {
                    case INTEGER: {
                        int val = atoi((const char*)attrList[j].attrValue);
                        memcpy(tuple + schema[i].attrOffset, &val, sizeof(int));
                        break;
                    }
                    case FLOAT: {
                        float val = atof((const char*)attrList[j].attrValue);
                        memcpy(tuple + schema[i].attrOffset, &val, sizeof(float));
                        break;
                    }
                    case STRING: {
                        strncpy(tuple + schema[i].attrOffset,
                               (const char*)attrList[j].attrValue,
                               schema[i].attrLen);
                        tuple[schema[i].attrOffset + schema[i].attrLen - 1] = '\0'; 
                        break;
                    }
                    default:
                        delete[] tuple;
                        delete[] schema;
                        return ATTRTYPEMISMATCH;
                }
                break;
            }
        }
        if (!matched) { // Error case
            delete[] tuple;
            delete[] schema;
            return ATTRNOTFOUND; // Correct error code
        }
    }

    InsertFileScan heap(relation, status);
    if (status != OK) {
        delete[] tuple;
        delete[] schema;
        return status;
    }

    // Reinsert record
    Record rec;
    rec.data = tuple;
    rec.length = totalLen;
    RID rid;
    status = heap.insertRecord(rec, rid);

    // Clean up and done
    //free(tuple);
    delete[] tuple;
    delete[] schema;
    return status;
}
