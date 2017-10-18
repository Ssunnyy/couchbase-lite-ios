//
//  CBLDatabase.m
//  CouchbaseLite
//
//  Created by Pasin Suriyentrakorn on 12/15/16.
//  Copyright © 2016 Couchbase. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

#import "CBLDatabase.h"
#import "c4BlobStore.h"
#import "c4Observer.h"
#import "CBLCoreBridge.h"
#import "CBLDatabase+Internal.h"
#import "CBLMutableDocument.h"
#import "CBLDocument+Internal.h"
#import "CBLDocumentChange.h"
#import "CBLDocumentChangeListener.h"
#import "CBLDocumentFragment.h"
#import "CBLEncryptionKey+Internal.h"
#import "CBLIndex+Internal.h"
#import "CBLQuery+Internal.h"
#import "CBLMisc.h"
#import "CBLPredicateQuery+Internal.h"
#import "CBLSharedKeys.hh"
#import "CBLStringBytes.h"
#import "CBLStatus.h"


#define kDBExtension @"cblite2"


@implementation CBLDatabaseConfiguration

@synthesize directory=_directory, conflictResolver = _conflictResolver, encryptionKey=_encryptionKey;
@synthesize fileProtection=_fileProtection;


- (instancetype) init {
    self = [super init];
    if (self) {
#if TARGET_OS_IPHONE
        _fileProtection = NSDataWritingFileProtectionCompleteUntilFirstUserAuthentication;
#endif
    }
    return self;
}


- (instancetype) copyWithZone:(NSZone *)zone {
    CBLDatabaseConfiguration* o = [[self.class alloc] init];
    o.directory = _directory;
    o.conflictResolver = _conflictResolver;
    o.encryptionKey = _encryptionKey;
    o.fileProtection = _fileProtection;
    return o;
}


- (NSString*) directory {
    if (!_directory)
        _directory = defaultDirectory();
    return _directory;
}


static NSString* defaultDirectory() {
    NSSearchPathDirectory dirID = NSApplicationSupportDirectory;
#if TARGET_OS_TV
    dirID = NSCachesDirectory; // Apple TV only allows apps to store data in the Caches directory
#endif
    NSArray* paths = NSSearchPathForDirectoriesInDomains(dirID, NSUserDomainMask, YES);
    NSString* path = paths[0];
#if !TARGET_OS_IPHONE
    NSString* bundleID = [[NSBundle mainBundle] bundleIdentifier];
    NSCAssert(bundleID, @"No bundle ID");
    path = [path stringByAppendingPathComponent: bundleID];
#endif
    return [path stringByAppendingPathComponent: @"CouchbaseLite"];
}


@end


@implementation CBLDatabase {
    NSString* _name;
    CBLDatabaseConfiguration* _config;
    CBLPredicateQuery* _allDocsQuery;
    
    C4DatabaseObserver* _dbObs;
    NSMutableSet* _dbChangeListeners;
    
    NSMutableDictionary* _docObs;
    NSMutableDictionary* _docChangeListeners;
}


@synthesize name=_name;
@synthesize c4db=_c4db, sharedKeys=_sharedKeys;
@synthesize replications=_replications, activeReplications=_activeReplications;


static const C4DatabaseConfig kDBConfig = {
    .flags = (kC4DB_Create | kC4DB_AutoCompact | kC4DB_Bundled | kC4DB_SharedKeys),
    .storageEngine = kC4SQLiteStorageEngine,
    .versioning = kC4RevisionTrees,
};


static void dbObserverCallback(C4DatabaseObserver* obs, void* context) {
    CBLDatabase *db = (__bridge CBLDatabase *)context;
    dispatch_async(dispatch_get_main_queue(), ^{        //TODO: Support other queues
        [db postDatabaseChanged];
    });
}


static void docObserverCallback(C4DocumentObserver* obs, C4Slice docID, C4SequenceNumber seq,
                                void *context)
{
    CBLDatabase *db = (__bridge CBLDatabase *)context;
    dispatch_async(dispatch_get_main_queue(), ^{        //TODO: Support other queues
        [db postDocumentChanged: slice2string(docID)];
    });
}


+ (void) initialize {
    if (self == [CBLDatabase class]) {
        CBLLog_Init();
    }
}


- (instancetype) initWithName: (NSString*)name
                        error: (NSError**)outError {
    return [self initWithName: name
                       config: [CBLDatabaseConfiguration new]
                        error: outError];
}


- (instancetype) initWithName: (NSString*)name
                       config: (nullable CBLDatabaseConfiguration*)config
                        error: (NSError**)outError {
    self = [super init];
    if (self) {
        _name = name;
        _config = config != nil? [config copy] : [CBLDatabaseConfiguration new];
        if (![self open: outError])
            return nil;
        _replications = [NSMapTable strongToWeakObjectsMapTable];
        _activeReplications = [NSMutableSet new];
    }
    return self;
}


- (instancetype) copyWithZone: (NSZone*)zone {
    return [[[self class] alloc] initWithName: _name config: _config error: nil];
}


- (void) dealloc {
    [self freeC4Observer];
    [self freeC4DB];
}


- (NSString*) description {
    return [NSString stringWithFormat: @"%@[%@]", self.class, _name];
}


- (NSString*) path {
    return _c4db != nullptr ? sliceResult2FilesystemPath(c4db_getPath(_c4db)) : nil;
}


- (uint64_t) count {
    return _c4db != nullptr ? c4db_getDocumentCount(_c4db) : 0;
}


- (CBLDatabaseConfiguration*) config {
    return [_config copy];
}


#pragma mark - GET EXISTING DOCUMENT


- (CBLDocument*) documentWithID: (NSString*)documentID {
    return [self documentWithID: documentID mustExist: YES error: nil];
}


#pragma mark - CHECK DOCUMENT EXISTS


- (BOOL) contains: (NSString*)docID {
    id doc = [self documentWithID: docID mustExist: YES error: nil];
    return doc != nil;
}


#pragma mark - SUBSCRIPTION


- (CBLDocumentFragment*) objectForKeyedSubscript: (NSString*)documentID {
    return [[CBLDocumentFragment alloc] initWithDocument: [self documentWithID: documentID]];
}


#pragma mark - SAVE


- (CBLDocument*) saveDocument: (CBLMutableDocument*)document error: (NSError**)error {
    if ([self prepareDocument: document error: error])
        return [self saveDocument: document
                    usingResolver: document.effectiveConflictResolver
                         deletion: NO
                            error: error];
    else
        return nil;
}


- (BOOL) deleteDocument: (CBLDocument*)document error: (NSError**)error {
    if ([self prepareDocument: document error: error])
        return [self saveDocument: document
                    usingResolver: document.effectiveConflictResolver
                         deletion: YES
                            error: error] != nil;
    else
        return NO;
}


- (BOOL) purgeDocument: (CBLDocument*)document error: (NSError**)error {
    if ([self prepareDocument: document error: error]) {
        if (!document.exists) {
            return createError(kCBLStatusNotFound, error);
        }
        
        C4Transaction transaction(self.c4db);
        if (!transaction.begin())
            return convertError(transaction.error(),  error);
        
        C4Error err;
        if (c4doc_purgeRevision(document.c4Doc.rawDoc, C4Slice(), &err) >= 0) {
            if (c4doc_save(document.c4Doc.rawDoc, 0, &err)) {
                // Save succeeded; now commit:
                if (!transaction.commit())
                    return convertError(transaction.error(), error);
                
                return YES;
            }
        }
        return convertError(err, error);
    } else
        return NO;
}


#pragma mark - BATCH OPERATION


- (BOOL) inBatch: (NSError**)outError do: (void (^)())block {
    [self mustBeOpen];
    
    C4Transaction transaction(_c4db);
    if (outError)
        *outError = nil;
    
    if (!transaction.begin())
        return convertError(transaction.error(), outError);
    
    block();
    
    if (!transaction.commit())
        return convertError(transaction.error(), outError);
    
    [self postDatabaseChanged];
    return YES;
}


#pragma mark - DATABASE MAINTENANCE


- (BOOL) close: (NSError**)outError {
    if (_c4db == nullptr)
        return YES;
    
    CBLLog(Database, @"Closing %@ at path %@", self, self.path);
    
    _allDocsQuery = nil;
    
    C4Error err;
    if (!c4db_close(_c4db, &err))
        return convertError(err, outError);
    
    [self freeC4Observer];
    [self freeC4DB];
    
    return YES;
}


- (BOOL) deleteDatabase: (NSError**)outError {
    [self mustBeOpen];
    
    C4Error err;
    if (!c4db_delete(_c4db, &err))
        return convertError(err, outError);
    
    [self freeC4Observer];
    [self freeC4DB];
    
    return YES;
}


- (BOOL) compact: (NSError**)outError {
    [self mustBeOpen];
    
    C4Error err;
    if (!c4db_compact(_c4db, &err))
        return convertError(err, outError);
    return YES;
}


- (BOOL) setEncryptionKey: (nullable CBLEncryptionKey*)key error: (NSError**)outError {
    [self mustBeOpen];
    
    C4Error err;
    C4EncryptionKey encKey = c4EncryptionKey(key);
    if (!c4db_rekey(_c4db, &encKey, &err))
        return convertError(err, outError);
    
    return YES;
}


+ (BOOL) deleteDatabase: (NSString*)name
            inDirectory: (nullable NSString*)directory
                  error: (NSError**)outError
{
    NSString* path = databasePath(name, directory ?: defaultDirectory());
    CBLStringBytes bPath(path);
    C4Error err;
    return c4db_deleteAtPath(bPath, &kDBConfig, &err) || err.code==0 || convertError(err, outError);
}


+ (BOOL) databaseExists: (NSString*)name
            inDirectory: (nullable NSString*)directory {
    NSString* path = databasePath(name, directory ?: defaultDirectory());
    return [[NSFileManager defaultManager] fileExistsAtPath: path];
}


+ (BOOL) copyFromPath: (NSString*)path
           toDatabase: (NSString*)name
               config: (nullable CBLDatabaseConfiguration*)config
                error: (NSError**)outError
{
    NSString* toPathStr = databasePath(name, config.directory ?: defaultDirectory());
    CBLStringBytes toPath(toPathStr);
    CBLStringBytes fromPath(path);
    
    C4Error err;
    C4DatabaseConfig c4Config = c4DatabaseConfig(config ?: [CBLDatabaseConfiguration new]);
    if (c4db_copy(fromPath, toPath, &c4Config, &err) || err.code==0 || convertError(err, outError)) {
        BOOL success = setupDatabaseDirectory(toPathStr, config.fileProtection, outError);
        if (!success) {
            NSError* removeError;
            if (![[NSFileManager defaultManager] removeItemAtPath: toPathStr error: &removeError])
                CBLWarn(Database, @"Error when deleting the copied database dir: %@", removeError);
        }
        return success;
    } else
        return NO;
}


#pragma mark - Logging


+ (void) setLogLevel: (CBLLogLevel)level domain: (CBLLogDomain)domain {
    C4LogLevel c4level = level != kCBLLogLevelNone ? (C4LogLevel)level : kC4LogNone;
    switch (domain) {
        case kCBLLogDomainAll:
            CBLSetLogLevel(Database, c4level);
            CBLSetLogLevel(DB, c4level);
            CBLSetLogLevel(Query, c4level);
            CBLSetLogLevel(SQL, c4level);
            CBLSetLogLevel(Sync, c4level);
            CBLSetLogLevel(BLIP, c4level);
            CBLSetLogLevel(Actor, c4level);
            CBLSetLogLevel(WebSocket, c4level);
            break;
        case kCBLLogDomainDatabase:
            CBLSetLogLevel(Database, c4level);
            CBLSetLogLevel(DB, c4level);
            break;
        case kCBLLogDomainQuery:
            CBLSetLogLevel(Query, c4level);
            CBLSetLogLevel(SQL, c4level);
            break;
        case kCBLLogDomainReplicator:
            CBLSetLogLevel(Sync, c4level);
            break;
        case kCBLLogDomainNetwork:
            CBLSetLogLevel(BLIP, c4level);
            CBLSetLogLevel(Actor, c4level);
            CBLSetLogLevel(WebSocket, c4level);
        default:
            break;
    }
}


#pragma mark - DOCUMENT CHANGES


- (id<NSObject>) addChangeListener: (void (^)(CBLDatabaseChange*))block {
    [self mustBeOpen];
    
    return [self addDatabaseChangeListener: block];
}


- (id<NSObject>) addChangeListenerForDocumentID: (NSString*)documentID
                                     usingBlock: (void (^)(CBLDocumentChange*))block
{
    [self mustBeOpen];
    
    return [self addDocumentChangeListener: block documentID: documentID];
}


- (void) removeChangeListener: (id<NSObject>)listener {
    [self mustBeOpen];
    
    if ([listener isKindOfClass: [CBLDocumentChangeListener class]])
        [self removeDocumentChangeListener:listener];
    else
        [self removeDatabaseChangeListener:listener];
}


#pragma mark - Index:


- (NSArray<NSString*>*) indexes {
    [self mustBeOpen];
    
    C4SliceResult data = c4db_getIndexes(_c4db, nullptr);
    FLValue value = FLValue_FromTrustedData((FLSlice)data);
    return FLValue_GetNSObject(value, &_sharedKeys);
}


- (BOOL) createIndex: (CBLIndex*)index withName: (NSString*)name error: (NSError**)outError {
    [self mustBeOpen];
    
    NSData* json = [NSJSONSerialization dataWithJSONObject: index.indexItems
                                                   options: 0
                                                     error: outError];
    if (!json)
        return NO;
    
    CBLStringBytes bName(name);
    C4IndexType type = index.indexType;
    C4IndexOptions options = index.indexOptions;
    
    C4Error c4err;
    return c4db_createIndex(_c4db, bName, {json.bytes, json.length}, type, &options, &c4err) ||
        convertError(c4err, outError);
}


- (BOOL)deleteIndexForName:(NSString *)name error:(NSError **)outError {
    [self mustBeOpen];
    
    CBLStringBytes bName(name);
    C4Error c4err;
    return c4db_deleteIndex(_c4db, bName, &c4err) || convertError(c4err, outError);
}


#pragma mark - QUERIES:


- (NSEnumerator<CBLMutableDocument*>*) allDocuments {
    [self mustBeOpen];
    
    if (!_allDocsQuery) {
        _allDocsQuery = [[CBLPredicateQuery alloc] initWithDatabase: self];
        _allDocsQuery.orderBy = @[@"_id"];
    }
    auto e = [_allDocsQuery allDocuments: nullptr];
    Assert(e, @"allDocuments failed?!");
    return e;
}


- (CBLPredicateQuery*) createQueryWhere: (nullable id)where {
    [self mustBeOpen];
    
    auto query = [[CBLPredicateQuery alloc] initWithDatabase: self];
    query.where = where;
    return query;
}


#pragma mark - INTERNAL


- (C4BlobStore*) getBlobStore: (NSError**)outError {
    if (![self mustBeOpen: outError])
        return nil;
    C4Error err;
    C4BlobStore *blobStore = c4db_getBlobStore(_c4db, &err);
    if (!blobStore)
        convertError(err, outError);
    return blobStore;
}


#pragma mark - PRIVATE


- (BOOL) open: (NSError**)outError {
    if (_c4db)
        return YES;
    
    NSString* dir = _config.directory;
    Assert(dir != nil);
    if (!setupDatabaseDirectory(dir, _config.fileProtection, outError))
        return NO;
    
    NSString* path = databasePath(_name, dir);
    CBLStringBytes bPath(path);
    
    C4DatabaseConfig c4config = c4DatabaseConfig(_config);
    CBLLog(Database, @"Opening %@ at path %@", self, path);
    C4Error err;
    _c4db = c4db_open(bPath, &c4config, &err);
    if (!_c4db)
        return convertError(err, outError);
    
    _sharedKeys = cbl::SharedKeys(_c4db);
    
    return YES;
}


static NSString* databasePath(NSString* name, NSString* dir) {
    name = [[name stringByReplacingOccurrencesOfString: @"/" withString: @":"]
            stringByAppendingPathExtension: kDBExtension];
    NSString* path = [dir stringByAppendingPathComponent: name];
    return path.stringByStandardizingPath;
}


static BOOL setupDatabaseDirectory(NSString* dir,
                                   NSDataWritingOptions fileProtection,
                                   NSError** outError)
{
    NSDictionary* attributes = nil;
#if TARGET_OS_IPHONE
    // Set the iOS file protection mode of the manager's top-level directory.
    // This mode will be inherited by all files created in that directory.
    NSString* protection;
    switch (fileProtection & NSDataWritingFileProtectionMask) {
        case NSDataWritingFileProtectionNone:
            protection = NSFileProtectionNone;
            break;
        case NSDataWritingFileProtectionComplete:
            protection = NSFileProtectionComplete;
            break;
        case NSDataWritingFileProtectionCompleteUntilFirstUserAuthentication:
            protection = NSFileProtectionCompleteUntilFirstUserAuthentication;
            break;
        default:
            protection = NSFileProtectionCompleteUnlessOpen;
            break;
    }
    attributes = @{NSFileProtectionKey: protection};
#endif
    
    NSError* error;
    if (![[NSFileManager defaultManager] createDirectoryAtPath: dir
                                   withIntermediateDirectories: YES
                                                    attributes: attributes
                                                         error: &error]) {
        if (!CBLIsFileExistsError(error)) {
            if (outError) *outError = error;
            return NO;
        }
    }
    
    if (attributes) {
        // TODO: Optimization - Check the existing file protection level.
        if (![[NSFileManager defaultManager] setAttributes: attributes
                                              ofItemAtPath: dir
                                                     error: outError])
            return NO;
    }
    
    return YES;
}


static C4DatabaseConfig c4DatabaseConfig (CBLDatabaseConfiguration* config) {
    C4DatabaseConfig c4config = kDBConfig;
    if (config.encryptionKey != nil)
        c4config.encryptionKey = c4EncryptionKey(config.encryptionKey);
    return c4config;
}


static C4EncryptionKey c4EncryptionKey(CBLEncryptionKey* key) {
    C4EncryptionKey cKey;
    if (key) {
        cKey.algorithm = kC4EncryptionAES256;
        Assert(key.key.length == sizeof(cKey.bytes), @"Invalid key size");
        memcpy(cKey.bytes, key.key.bytes, sizeof(cKey.bytes));
    } else
        cKey.algorithm = kC4EncryptionNone;
    return cKey;
}


- (void) mustBeOpen {
    if (_c4db == nullptr) {
        [NSException raise: NSInternalInconsistencyException
                    format: @"Database is not open."];
    }
}


- (BOOL) mustBeOpen: (NSError**)outError {
    return _c4db != nullptr || convertError({LiteCoreDomain, kC4ErrorNotOpen}, outError);
}


- (nullable CBLDocument*) documentWithID: (NSString*)documentID
                               mustExist: (bool)mustExist
                                   error: (NSError**)outError
{
    [self mustBeOpen];
    
    return [[CBLDocument alloc] initWithDatabase: self
                                      documentID: documentID
                                       mustExist: mustExist
                                           error: outError];
}


- (BOOL) prepareDocument: (CBLDocument*)document error: (NSError**)error {
    [self mustBeOpen];
    
    if (!document.database) {
        document.database = self;
    } else if (document.database != self) {
        return createError(kCBLStatusForbidden,
                           @"The document is from the different database.", error);
    }
    return YES;
}


- (id<NSObject>) addDatabaseChangeListener: (void (^)(CBLDatabaseChange*))block {
    if (!_dbChangeListeners) {
        _dbChangeListeners = [NSMutableSet set];
        _dbObs = c4dbobs_create(_c4db, dbObserverCallback, (__bridge void *)self);
    }
    
    CBLChangeListener* listener = [[CBLChangeListener alloc] initWithBlock: block];
    [_dbChangeListeners addObject: listener];
    return listener;
}


- (void) removeDatabaseChangeListener: (id<NSObject>)listener {
    [_dbChangeListeners removeObject:listener];
    
    if (_dbChangeListeners.count == 0) {
        c4dbobs_free(_dbObs);
        _dbObs = nil;
        _dbChangeListeners = nil;
    }
}


- (void) postDatabaseChanged {
    if (!_dbObs || !_c4db || c4db_isInTransaction(_c4db))
        return;
    
    const uint32_t kMaxChanges = 100u;
    C4DatabaseChange changes[kMaxChanges];
    bool external = false;
    uint32_t nChanges = 0u;
    NSMutableArray* docIDs = [NSMutableArray new];
    do {
        // Read changes in batches of kMaxChanges:
        bool newExternal;
        nChanges = c4dbobs_getChanges(_dbObs, changes, kMaxChanges, &newExternal);
        if (nChanges == 0 || external != newExternal || docIDs.count > 1000) {
            if(docIDs.count > 0) {
                CBLDatabaseChange* change = [[CBLDatabaseChange alloc] initWithDocumentIDs: docIDs
                                                                                isExternal: external];
                for (CBLChangeListener* listener in _dbChangeListeners) {
                    void (^block)(CBLDatabaseChange*) = listener.block;
                    block(change);
                }
                docIDs = [NSMutableArray new];
            }
        }
        
        external = newExternal;
        for(uint32_t i = 0; i < nChanges; i++) {
            NSString *docID =slice2string(changes[i].docID);
            [docIDs addObject: docID];
        }
    } while(nChanges > 0);
}


- (id<NSObject>) addDocumentChangeListener: (void (^)(CBLDocumentChange*))block
                                documentID: (NSString*)documentID
{
    if (!_docChangeListeners)
        _docChangeListeners = [NSMutableDictionary dictionary];
    
    NSMutableSet* listeners = _docChangeListeners[documentID];
    if (!listeners) {
        listeners = [NSMutableSet set];
        [_docChangeListeners setObject: listeners forKey: documentID];
        
        CBLStringBytes bDocID(documentID);
        C4DocumentObserver* o =
        c4docobs_create(_c4db, bDocID, docObserverCallback, (__bridge void *)self);
        
        if (!_docObs)
            _docObs = [NSMutableDictionary dictionary];
        [_docObs setObject: [NSValue valueWithPointer: o] forKey: documentID];
    }
    
    id listener = [[CBLDocumentChangeListener alloc] initWithDocumentID: documentID
                                                              withBlock: block];
    [listeners addObject: listener];
    return listener;
}


- (void) removeDocumentChangeListener: (CBLDocumentChangeListener*)listener {
    NSString* documentID = listener.documentID;
    NSMutableSet* listeners = _docChangeListeners[documentID];
    if (listeners) {
        [listeners removeObject: listener];
        if (listeners.count == 0) {
            NSValue* obsValue = [_docObs objectForKey: documentID];
            if (obsValue) {
                C4DocumentObserver* obs = (C4DocumentObserver*)obsValue.pointerValue;
                c4docobs_free(obs);
            }
            [_docObs removeObjectForKey: documentID];
            [_docChangeListeners removeObjectForKey:documentID];
        }
    }
}


- (void) postDocumentChanged: (NSString*)documentID {
    if (!_docObs[documentID] || !_c4db ||  c4db_isInTransaction(_c4db))
        return;
    
    CBLDocumentChange* change = [[CBLDocumentChange alloc] initWithDocumentID: documentID];
    NSSet* listeners = [_docChangeListeners objectForKey: documentID];
    for (CBLDocumentChangeListener* listener in listeners) {
        void (^block)(CBLDocumentChange*) = listener.block;
        block(change);
    }
}


- (void) freeC4Observer {
    c4dbobs_free(_dbObs);
    _dbObs = nullptr;
    _dbChangeListeners = nil;
    
    for (NSValue* obsValue in _docObs.allValues) {
        C4DocumentObserver* obs = (C4DocumentObserver*)obsValue.pointerValue;
        c4docobs_free(obs);
    }
    
    _docObs = nil;
    _docChangeListeners = nil;
}


- (void) freeC4DB {
    c4db_free(_c4db);
    _c4db = nil;
}


#pragma mark - SAVE DOCUMENT


// Lower-level save method. On conflict, returns YES but sets *outDoc to NULL.
- (BOOL) saveDocument: (CBLDocument*)doc
                 into: (C4Document**)outDoc
             asDelete: (BOOL)deletion
          usingParent: (C4Document*)parent
                error: (NSError **)outError
{
    C4RevisionFlags revFlags = 0;
    if (deletion)
        revFlags = kRevDeleted;
    NSData* body = nil;
    C4Slice bodySlice = {};
    if (!deletion && !doc.isEmpty) {
        // Encode properties to Fleece data:
        body = [doc encode: outError];
        if (!body) {
            *outDoc = nullptr;
            return NO;
        }
        bodySlice = data2slice(body);
        auto root = FLValue_FromTrustedData(bodySlice);
        if (c4doc_dictContainsBlobs((FLDict)root, self.sharedKeys))
            revFlags |= kRevHasAttachments;
    }
    
    // Save to database:
    C4Error err;
    C4Document* c4Doc = parent;
    if (!c4Doc)
        c4Doc = doc.c4Doc.rawDoc;
    
    if (c4Doc) {
        *outDoc = c4doc_update(c4Doc, bodySlice, revFlags, &err);
    } else {
        CBLStringBytes docID(doc.id);
        *outDoc = c4doc_create(self.c4db, docID, data2slice(body), revFlags, &err);
    }
    
    if (!*outDoc && !(err.domain == LiteCoreDomain && err.code == kC4ErrorConflict)) {
        // conflict is not an error, at this level
        return convertError(err, outError);
    }
    return YES;
}


// "Pulls" from the database, merging the latest revision into the in-memory properties,
//  without saving. */
- (CBLDocument*) resolveConflictInDocument: (CBLDocument*)doc
                             usingResolver: (id<CBLConflictResolver>)resolver
                                  deletion: (bool)deletion
                                    parent: (C4Document**)outParent
                                     error: (NSError**)outError
{
    if (outParent)
        *outParent = NULL;
    
    if (!resolver) {
        convertError({LiteCoreDomain, kC4ErrorConflict}, outError);
        return nil;
    }
    
    // Read the current revision from the database:
    CBLDocument* current = [[CBLDocument alloc] initWithDatabase: self
                                                      documentID: doc.id
                                                       mustExist: YES
                                                           error: outError];
    if (!current)
        return nil;
    
    // Resolve conflict:
    CBLDocument* resolved;
    if (deletion) {
        // Deletion always loses a conflict:
        resolved = current;
    } else {
        // Call the conflict resolver:
        CBLDocument* base = nil;
        if (doc.c4Doc) {
            base = [[CBLDocument alloc] initWithDatabase: self
                                              documentID: doc.id
                                                   c4Doc: doc.c4Doc];
        }
        
        CBLConflict* conflict = [[CBLConflict alloc] initWithMine: doc theirs: current base: base];
        resolved = [resolver resolve: conflict];
        if (!resolved) {
            convertError({LiteCoreDomain, kC4ErrorConflict}, outError);
            return nil;
        }
        resolved.database = self;
    }
    
    if (outParent)
        *outParent = current.c4Doc.rawDoc;
    
    return resolved;
}


- (CBLDocument*) saveDocument: (CBLDocument*)doc
                usingResolver: (id<CBLConflictResolver>)resolver
                     deletion: (bool)deletion
                        error: (NSError**)outError
{
    if (deletion && !doc.exists) {
        createError(kCBLStatusNotFound, outError);
        return nil;
    }
    
    // Begin a db transaction:
    C4Transaction transaction(self.c4db);
    if (!transaction.begin()) {
        convertError(transaction.error(), outError);
        return nil;
    }
    // Attempt to save. (On conflict, this will succeed but newDoc will be null.)
    C4Document* newDoc;
    if (![self saveDocument: doc into: &newDoc asDelete: deletion usingParent: NULL error: outError]) {
        return nil;
    }
    
    if (!newDoc) {
        // There's been a conflict; first merge with the new saved revision:
        C4Document* parent = NULL;
        CBLDocument* resolved = [self resolveConflictInDocument: doc
                                                  usingResolver: resolver
                                                       deletion: deletion
                                                         parent: &parent
                                                          error: outError];
        if (!resolved)
            return nil;
        
        if (resolved.c4Doc.rawDoc == parent)
            return resolved;
        
        // Now save the merged properties:
        if (![self saveDocument: resolved into: &newDoc asDelete: deletion usingParent: parent error: outError])
            return nil;
        Assert(newDoc);     // In a transaction we can't have a second conflict after merging!
    }
    
    // Save succeeded; now commit the transaction:
    BOOL success = transaction.commit();
    if (!success) {
        c4doc_free(newDoc);
        convertError(transaction.error(), outError);
        return nil;
    }
    
    return [[CBLDocument alloc] initWithDatabase: self documentID: doc.id c4Doc: [CBLC4Document document: newDoc]];
}


#pragma mark - RESOLVING REPLICATED CONFLICTS:


- (bool) resolveConflictInDocument: (NSString*)docID
                     usingResolver: (id<CBLConflictResolver>)resolver
                             error: (NSError**)outError {
    C4Transaction t(_c4db);
    t.begin();

    auto doc = [[CBLDocument alloc] initWithDatabase: self
                                          documentID: docID
                                           mustExist: YES
                                               error: outError];
    if (!doc)
        return false;

    // Read the conflicting remote revision:
    auto otherDoc = [[CBLDocument alloc] initWithDatabase: self
                                               documentID: docID
                                                mustExist: YES
                                                    error: outError];
    if (!otherDoc || ![otherDoc selectConflictingRevision])
        return false;

    // Read the common ancestor revision (if it's available):
    auto baseDoc = [[CBLDocument alloc] initWithDatabase: self
                                              documentID: docID
                                               mustExist: YES
                                                   error: outError];
    if (![baseDoc selectCommonAncestorOfDoc: doc andDoc: otherDoc] || !baseDoc.data)
        baseDoc = nil;

    // Call the conflict resolver:
    CBLDocument* resolved;
    if (otherDoc.isDeleted) {
        resolved = doc;
    } else if (doc.isDeleted) {
        resolved = otherDoc;
    } else {
        if (!resolver)
            resolver = doc.effectiveConflictResolver;
        auto conflict = [[CBLConflict alloc] initWithMine: doc theirs: otherDoc base: baseDoc];
        CBLLog(Database, @"Resolving doc '%@' with %@ (mine=%@, theirs=%@, base=%@",
               docID, resolver.class, doc.revID, otherDoc.revID, baseDoc.revID);
        resolved = [resolver resolve: conflict];
        if (!resolved)
            return convertError({LiteCoreDomain, kC4ErrorConflict}, outError);
    }

    // Figure out what revision to delete and what if anything to add:
    CBLStringBytes winningRevID, losingRevID;
    NSData* mergedBody = nil;
    if (resolved == otherDoc) {
        winningRevID = otherDoc.revID;
        losingRevID = doc.revID;
    } else {
        winningRevID = doc.revID;
        losingRevID = otherDoc.revID;
        if (resolved != doc) {
            resolved.database = self;
            mergedBody = [resolved encode: outError];
            if (!mergedBody)
                return false;
        }
    }

    // Tell LiteCore to do the resolution:
    C4Document *rawDoc = doc.c4Doc.rawDoc;
    C4Error c4err;
    if (!c4doc_resolveConflict(rawDoc,
                               winningRevID,
                               losingRevID,
                               data2slice(mergedBody),
                               &c4err)
            || !c4doc_save(rawDoc, 0, &c4err)) {
        return convertError(c4err, outError);
    }
    CBLLog(Database, @"Conflict resolved as doc '%@' rev %.*s",
           docID, (int)rawDoc->revID.size, rawDoc->revID.buf);

    return t.commit() || convertError(t.error(), outError);
}


@end

