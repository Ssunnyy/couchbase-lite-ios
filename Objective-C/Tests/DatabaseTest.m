//
//  DatabaseTest.m
//  CouchbaseLite
//
//  Created by Pasin Suriyentrakorn on 1/10/17.
//  Copyright © 2017 Couchbase. All rights reserved.
//

#import "CBLTestCase.h"
#import "CBLDatabase+Internal.h"


@interface DatabaseTest : CBLTestCase
@end

@interface DummyResolver : NSObject <CBLConflictResolver>
@end

@implementation DummyResolver

- (CBLDocument*) resolve: (CBLConflict*)conflict {
    NSAssert(NO, @"Resolver should not have been called!");
    return nil;
}

@end


@implementation DatabaseTest


// helper method to delete database
- (void) deleteDatabase: (CBLDatabase*)db {
    NSError* error;
    NSString* path = db.path;
    Assert([[NSFileManager defaultManager] fileExistsAtPath: path]);
    Assert([db deleteDatabase: &error]);
    AssertNil(error);
    AssertFalse([[NSFileManager defaultManager] fileExistsAtPath: path]);
}


// helper method to close database
- (void) closeDatabase: (CBLDatabase*)db{
    NSError* error;
    Assert([db close:&error]);
    AssertNil(error);
}


// helper method to save document
- (CBLMutableDocument*) generateDocument: (NSString*)docID {
    CBLMutableDocument* doc = [self createDocument: docID];
    [doc setObject:@1 forKey:@"key"];
    
    doc = [[self saveDocument: doc] edit];
    AssertEqual(1, (long)self.db.count);
    AssertEqual(1L, (long)doc.sequence);
    return doc;
}


// helper method to store Blob
- (void) storeBlob: (CBLDatabase*)db doc: (CBLMutableDocument*)doc content: (NSData*)content {
    CBLBlob* blob = [[CBLBlob alloc] initWithContentType: @"text/plain" data: content];
    [doc setObject: blob forKey: @"data"];
    [self saveDocument: doc];
}


// helper methods to verify getDoc
- (void) verifyGetDocument: (NSString*)docID {
    [self verifyGetDocument: docID value: 1];
}


- (void) verifyGetDocument: (NSString*)docID value: (int)value {
    [self verifyGetDocument: self.db docID: docID value: value];
}


- (void) verifyGetDocument: (CBLDatabase*)db docID: (NSString*)docID {
    [self verifyGetDocument: self.db docID: docID value: 1];
}


- (void) verifyGetDocument: (CBLDatabase*)db docID: (NSString*)docID value: (int)value {
    CBLMutableDocument* doc = [[db documentWithID: docID] edit];
    AssertNotNil(doc);
    AssertEqualObjects(docID, doc.id);
    AssertFalse(doc.isDeleted);
    AssertEqualObjects(@(value), [doc objectForKey: @"key"]);
}


// helper method to save n number of docs
- (NSArray*) createDocs: (int)n {
    NSMutableArray* docs = [NSMutableArray arrayWithCapacity: n];
    for(int i = 0; i < n; i++){
        CBLMutableDocument* doc = [self createDocument: [NSString stringWithFormat: @"doc_%03d", i]];
        [doc setObject: @(i) forKey:@"key"];
        CBLDocument* newDoc = [self saveDocument: doc];
        [docs addObject: [newDoc edit]];
    }
    AssertEqual(n, (long)self.db.count);
    return docs;
}


- (void)validateDocs: (int)n {
    for (int i = 0; i < n; i++) {
        [self verifyGetDocument: [NSString stringWithFormat: @"doc_%03d", i] value: i];
    }
}


// helper method to purge doc and verify doc.
- (void) purgeDocAndVerify: (CBLMutableDocument*)doc {
    NSError* error;
    Assert([self.db purgeDocument: doc error: &error]);
    AssertNil(error);
}


#pragma mark - DatabaseConfiguration


- (void) testCreateConfiguration {
    // Default:
    CBLDatabaseConfiguration* config1 = [[CBLDatabaseConfiguration alloc] init];
#if !TARGET_OS_IPHONE
    // MacOS needs directory as there is no bundle in mac unit test:
    config1.directory = @"/tmp";
#endif
    AssertNotNil(config1.directory);
    Assert(config1.directory.length > 0);
    AssertNil(config1.conflictResolver);
    AssertNil(config1.encryptionKey);
    AssertNil(config1.encryptionKey);
#if TARGET_OS_IPHONE
    AssertEqual(config1.fileProtection, NSDataWritingFileProtectionCompleteUntilFirstUserAuthentication);
#endif
    
    // Default + Copy:
    CBLDatabaseConfiguration* config1a = [config1 copy];
    AssertNotNil(config1a.directory);
    Assert(config1a.directory.length > 0);
    AssertNil(config1a.conflictResolver);
    AssertNil(config1a.encryptionKey);
#if TARGET_OS_IPHONE
    AssertEqual(config1a.fileProtection, NSDataWritingFileProtectionCompleteUntilFirstUserAuthentication);
#endif
    
    // Custom:
    CBLEncryptionKey* key = [[CBLEncryptionKey alloc] initWithPassword: @"key"];
    DummyResolver *resolver = [DummyResolver new];
    CBLDatabaseConfiguration* config2 = [[CBLDatabaseConfiguration alloc] init];
    config2.directory = @"/tmp/mydb";
    config2.conflictResolver = resolver;
    config2.encryptionKey = key;
#if TARGET_OS_IPHONE
    config2.fileProtection = NSDataWritingFileProtectionComplete;
#endif
    
    AssertEqualObjects(config2.directory, @"/tmp/mydb");
    AssertEqual(config2.conflictResolver, resolver);
    AssertEqualObjects(config2.encryptionKey, key);
#if TARGET_OS_IPHONE
    AssertEqual(config2.fileProtection, NSDataWritingFileProtectionComplete);
#endif
    
    // Custom + Copy:
    CBLDatabaseConfiguration* config2a = [config2 copy];
    AssertEqualObjects(config2a.directory, @"/tmp/mydb");
    AssertEqual(config2a.conflictResolver, resolver);
    AssertEqualObjects(config2a.encryptionKey, key);
#if TARGET_OS_IPHONE
    AssertEqual(config2a.fileProtection, NSDataWritingFileProtectionComplete);
#endif
}


- (void) testGetSetConfiguration {
    CBLDatabaseConfiguration* config = [[CBLDatabaseConfiguration alloc] init];
#if !TARGET_OS_IPHONE
    // MacOS needs directory as there is no bundle in mac unit test:
    config.directory = _db.config.directory;
#endif
    
    NSError* error;
    CBLDatabase* db = [[CBLDatabase alloc] initWithName: @"db"
                                                 config: config
                                                  error: &error];
    AssertNotNil(db.config);
    Assert(db.config != config);
    AssertEqualObjects(db.config.directory, config.directory);
    AssertEqualObjects(db.config.conflictResolver, config.conflictResolver);
    AssertEqual(db.config.encryptionKey, config.encryptionKey);
    AssertEqual(db.config.fileProtection, config.fileProtection);
    
}


- (void) testConfigurationIsCopiedWhenGetSet {
    CBLDatabaseConfiguration* config = [[CBLDatabaseConfiguration alloc] init];
#if !TARGET_OS_IPHONE
    // MacOS needs directory as there is no bundle in mac unit test:
    config.directory = _db.config.directory;
#endif
    
    NSError* error;
    CBLDatabase* db = [[CBLDatabase alloc] initWithName: @"db"
                                                 config: config
                                                  error: &error];
    config.conflictResolver = [DummyResolver new];
    AssertNotNil(db.config);
    Assert(db.config != config);
    Assert(db.config.conflictResolver != config.conflictResolver);
}


#pragma mark - Create Database


- (void) testCreate {
    // create db with default
    NSError* error;
    CBLDatabase* db = [self openDBNamed: @"db" error: &error];
    AssertNil(error);
    AssertNotNil(db);
    AssertEqual(0, (long)db.count);
    
    // delete database
    [self deleteDatabase: db];
}


#if TARGET_OS_IPHONE
- (void) testCreateWithDefaultConfiguration {
    // create db with default configuration
    NSError* error;
    CBLDatabase* db = [[CBLDatabase alloc] initWithName: @"db"
                                                 config: [CBLDatabaseConfiguration new]
                                                  error: &error];
    AssertNil(error);
    AssertNotNil(db, @"Couldn't open db: %@", error);
    AssertEqualObjects(db.name, @"db");
    Assert([db.path.lastPathComponent hasSuffix: @".cblite2"]);
    AssertEqual(0, (long)db.count);
    
    // delete database
    [self deleteDatabase: db];
}
#endif


- (void) testCreateWithSpecialCharacterDBNames {
    // create db with default configuration
    NSError* error;
    CBLDatabase* db = [self openDBNamed: @"`~@#$%^&*()_+{}|\\][=-/.,<>?\":;'" error: &error];
    AssertNil(error);
    AssertNotNil(db, @"Couldn't open db: %@", db.name);
    AssertEqualObjects(db.name, @"`~@#$%^&*()_+{}|\\][=-/.,<>?\":;'");
    Assert([db.path.lastPathComponent hasSuffix: @".cblite2"]);
    AssertEqual(0, (long)db.count);
    
    // delete database
    [self deleteDatabase: db];
}


- (void) testCreateWithEmptyDBNames {
    // create db with default configuration
    [self expectError: @"LiteCore" code: 30 in: ^BOOL(NSError** error) {
        return [self openDBNamed: @"" error: error] != nil;
    }];
}


- (void) testCreateWithCustomDirectory {
    NSString* dir = [NSTemporaryDirectory() stringByAppendingPathComponent: @"CouchbaseLite"];
    [CBLDatabase deleteDatabase: @"db" inDirectory: dir error: nil];
    AssertFalse([CBLDatabase databaseExists: @"db" inDirectory: dir]);
    
    // create db with custom directory
    NSError* error;
    CBLDatabaseConfiguration* config = [[CBLDatabaseConfiguration alloc] init];
    config.directory = dir;
    CBLDatabase* db = [[CBLDatabase alloc] initWithName: @"db" config: config error: &error];
    AssertNil(error);
    AssertNotNil(db, @"Couldn't open db: %@", error);
    AssertEqualObjects(db.name, @"db");
    Assert([db.path.lastPathComponent hasSuffix: @".cblite2"]);
    Assert([db.path containsString: dir]);
    Assert([CBLDatabase databaseExists: @"db" inDirectory: dir]);
    AssertEqual(0, (long)db.count);

    // delete database
    [self deleteDatabase: db];
}


#pragma mark - Get Document


- (void) testGetNonExistingDocWithID {
    AssertNil([self.db documentWithID:@"non-exist"]);
}


- (void) testGetExistingDocWithID {
    // store doc
    NSString* docID = @"doc1";
    [self generateDocument: docID];
    
    // validate document by getDocument.
    [self verifyGetDocument: docID];
}


- (void) testGetExistingDocWithIDFromDifferentDBInstance {
    // store doc
    NSString* docID = @"doc1";
    [self generateDocument: docID];
    
    // open db with same db name and default option
    NSError* error;
    CBLDatabase* otherDB = [self openDBNamed: [self.db name] error: &error];
    AssertNil(error);
    AssertNotNil(otherDB);
    Assert(otherDB != self.db);
    
    // get doc from other DB.
    AssertEqual(1, (long)otherDB.count);
    Assert([otherDB contains:docID]);
    
    [self verifyGetDocument: otherDB docID: docID];
    
    // close otherDB
    [self closeDatabase: otherDB];
}


- (void) testGetExistingDocWithIDInBatch {
    // save 10 docs
    [self createDocs: 10];
    
    // validate
    NSError* error;
    BOOL success = [self.db inBatch: &error do: ^{
        [self validateDocs: 10];
    }];
    Assert(success);
    AssertNil(error);
}


- (void) testGetDocFromClosedDB {
    // store doc
    [self generateDocument: @"doc1"];
    
    // close db
    [self closeDatabase: self.db];
    
    // Get doc
    [self expectException: @"NSInternalInconsistencyException" in: ^{
        [self.db documentWithID: @"doc1"];
    }];
}


- (void) testGetDocFromDeletedDB {
    // store doc
    [self generateDocument: @"doc1"];
    
    // delete db
    [self deleteDatabase: self.db];
    
    [self expectException: @"NSInternalInconsistencyException" in: ^{
        [self.db documentWithID: @"doc1"];
    }];
}


#pragma mark - Save Document


- (void) testSaveNewDocWithID {
    NSString* docID = @"doc1";
    
    [self generateDocument: docID];
    
    AssertEqual(1, (long)self.db.count);
    Assert([self.db contains: docID]);
    
    [self verifyGetDocument: docID];
}


- (void) testSaveNewDocWithSpecialCharactersDocID {
    NSString* docID = @"`~@#$%^&*()_+{}|\\][=-/.,<>?\":;'";
    
    [self generateDocument: docID];
    
    AssertEqual(1, (long)self.db.count);
    Assert([self.db contains: docID]);
    
    [self verifyGetDocument: docID];
}


- (void) testSaveDoc {
    // store doc
    NSString* docID = @"doc1";
    CBLMutableDocument* doc = [self generateDocument: docID];
    
    // update doc
    [doc setObject:@2 forKey:@"key"];
    [self saveDocument: doc];
    
    AssertEqual(1, (long)self.db.count);
    Assert([self.db contains: docID]);
    
    // verify
    [self verifyGetDocument: docID value: 2];
}


- (void) testSaveDocInDifferentDBInstance {
    // store doc
    NSString* docID = @"doc1";
    CBLMutableDocument* doc = [self generateDocument: docID];
    
    // create db with default
    NSError* error;
    CBLDatabase* otherDB = [self openDBNamed: [self.db name] error: &error];
    AssertNil(error);
    AssertNotNil(otherDB);
    Assert(otherDB != self.db);
    AssertEqual(1, (long)otherDB.count);
    
    // update doc & store it into different instance
    [doc setObject: @2 forKey: @"key"];
    [self expectError: @"CouchbaseLite" code: 403 in: ^BOOL(NSError** error2) {
        return [otherDB saveDocument: doc error: error2];
    }]; // forbidden
    
    // close otherDB
    [self closeDatabase: otherDB];
}


- (void) testSaveDocInDifferentDB {
    // store doc
    NSString* docID = @"doc1";
    CBLMutableDocument* doc = [self generateDocument: docID];
    
    // create db with default
    NSError* error;
    CBLDatabase* otherDB = [self openDBNamed: @"otherDB" error: &error];
    AssertNil(error);
    AssertNotNil(otherDB);
    Assert(otherDB != self.db);
    AssertEqual(0, (long)otherDB.count);
    
    // update doc & store it into different db
    [doc setObject: @2 forKey: @"key"];
    [self expectError: @"CouchbaseLite" code: 403 in: ^BOOL(NSError** error2) {
        return [otherDB saveDocument: doc error: error2];
    }]; // forbidden
    
    // delete otherDB
    [self deleteDatabase: otherDB];
}


- (void) testSaveSameDocTwice {
    // store doc
    NSString* docID = @"doc1";
    CBLMutableDocument* doc = [self generateDocument: docID];
    
    // second store
    [self saveDocument: doc];
    
    AssertEqualObjects(docID, doc.id);
    AssertEqual(1, (long)self.db.count);
}


- (void) testSaveInBatch {
    NSError* error;
    BOOL success = [self.db inBatch: &error do: ^{
        // save 10 docs
        [self createDocs: 10];
    }];
    Assert(success);
    AssertEqual(10, (long)self.db.count);
    
    [self validateDocs: 10];
}


- (void) testSaveDocToClosedDB {
    [self closeDatabase: self.db];
    
    CBLMutableDocument* doc = [self createDocument: @"doc1"];
    [doc setObject:@1 forKey:@"key"];
    
    [self expectException: @"NSInternalInconsistencyException" in: ^{
        [self.db saveDocument: doc error: nil];
    }];
}


- (void) testSaveDocToDeletedDB {
    // delete db
    [self deleteDatabase: self.db];
    
    CBLMutableDocument* doc = [self createDocument: @"doc1"];
    [doc setObject: @1 forKey: @"key"];
    
    [self expectException: @"NSInternalInconsistencyException" in: ^{
        [self.db saveDocument: doc error: nil];
    }];
}


#pragma mark - Delete Document


- (void) testDeletePreSaveDoc {
    CBLMutableDocument* doc = [self createDocument: @"doc1"];
    [doc setObject: @1 forKey: @"key"];
    
    [self expectError: @"CouchbaseLite" code: 404 in: ^BOOL(NSError** error) {
        return [self.db deleteDocument: doc error: error];
    }];
    AssertEqual(0, (long)self.db.count);
}


- (void) testDeleteDoc {
    // store doc
    NSString* docID = @"doc1";
    CBLMutableDocument* doc = [self generateDocument: docID];
    
    NSError* error;
    Assert([self.db deleteDocument: doc error: &error]);
    AssertNil(error);
    AssertEqual(0, (long)self.db.count);
    
    CBLDocument* result = [self.db documentWithID: docID];
    AssertEqualObjects(docID, result.id);
    Assert(result.isDeleted);
    AssertEqual(2, (int)result.sequence);
    AssertNil([result objectForKey: @"key"]);
}


- (void) testDeleteDocInDifferentDBInstance {
    // store doc
    NSString* docID = @"doc1";
    CBLMutableDocument* doc = [self generateDocument: docID];
    
    // create db with same name
    NSError* error;
    CBLDatabase* otherDB = [self openDBNamed: [self.db name] error: &error];
    AssertNil(error);
    AssertNotNil(otherDB);
    Assert(otherDB != self.db);
    Assert([otherDB contains:docID]);
    AssertEqual(1, (long)otherDB.count);
    
    [self expectError: @"CouchbaseLite" code: 403 in: ^BOOL(NSError** error2) {
        return [otherDB deleteDocument: doc error: error2];
    }]; // forbidden

    AssertEqual(1, (long)otherDB.count);
    AssertEqual(1, (long)self.db.count);
    AssertFalse(doc.isDeleted);
    
    // close otherDB
    [self closeDatabase: otherDB];
}


- (void) testDeleteDocInDifferentDB {
    // store doc
    NSString* docID = @"doc1";
    CBLMutableDocument* doc = [self generateDocument: docID];
    
    // create db with different name
    NSError* error;
    CBLDatabase* otherDB = [self openDBNamed: @"otherDB" error: &error];
    AssertNil(error);
    AssertNotNil(otherDB);
    Assert(otherDB != self.db);
    AssertFalse([otherDB contains: docID]);
    AssertEqual(0, (long)otherDB.count);
    
    [self expectError: @"CouchbaseLite" code: 403 in: ^BOOL(NSError** error2) {
        return [otherDB deleteDocument: doc error: error2];
    }]; // forbidden
    
    AssertEqual(0, (long)otherDB.count);
    AssertEqual(1, (long)self.db.count);
    
    AssertFalse(doc.isDeleted);
    
    // delete otherDB
    [self deleteDatabase: otherDB];
}


- (void) testDeleteSameDocTwice {
    // store doc
    NSString* docID = @"doc1";
    CBLMutableDocument* doc = [self generateDocument:docID];
    
    // first time deletion
    NSError* error;
    Assert([self.db deleteDocument: doc error: &error]);
    AssertNil(error);
    AssertEqual(0, (long)self.db.count);
    
    CBLDocument* result = [self.db documentWithID: docID];
    AssertNil([result objectForKey: @"key"]);
    AssertEqual(2, (int)result.sequence);
    Assert(result.isDeleted);
    
    // second time deletion
    Assert([self.db deleteDocument: result error: &error]);
    AssertNil(error);
    AssertEqual(0, (long)self.db.count);
    
    result = [self.db documentWithID: docID];
    AssertNil([result objectForKey: @"key"]);
    AssertEqual(3, (int)result.sequence);
    Assert(result.isDeleted);
}


- (void) testDeleteDocInBatch {
    // save 10 docs
    [self createDocs: 10];
    
    NSError* error;
    BOOL success = [self.db inBatch: &error do: ^{
        for(int i = 0; i < 10; i++){
            NSError* err;
            NSString* docID = [[NSString alloc] initWithFormat: @"doc_%03d", i];
            CBLDocument* doc = [self.db documentWithID: docID];
            Assert([self.db deleteDocument: doc error: &err]);
            AssertNil(err);
            
            doc = [self.db documentWithID: docID];
            AssertNil([doc objectForKey: @"key"]);
            Assert(doc.isDeleted);
            AssertEqual(9 - i, (long)self.db.count);
        }
    }];
    Assert(success);
    AssertNil(error);
    AssertEqual(0, (long)self.db.count);
}


- (void) testDeleteDocOnClosedDB {
    // store doc
    CBLMutableDocument* doc = [self generateDocument: @"doc1"];
    
    // close db
    [self closeDatabase: self.db];
    
    // delete doc from db.
    [self expectException: @"NSInternalInconsistencyException" in: ^{
        [self.db deleteDocument: doc error: nil];
    }];
}


- (void) testDeleteDocOnDeletedDB {
    // store doc
    CBLMutableDocument* doc = [self generateDocument:@"doc1"];
    
    // delete db
    [self deleteDatabase: self.db];
    
    // delete doc from db.
    [self expectException: @"NSInternalInconsistencyException" in: ^{
        [self.db deleteDocument: doc error: nil];
    }];
}


#pragma mark - Purge Document


- (void) testPurgePreSaveDoc {
    CBLMutableDocument* doc = [self createDocument: @"doc1"];
    
    [self expectError: @"CouchbaseLite" code: 404 in: ^BOOL(NSError** error) {
        return [self.db purgeDocument: doc error: error];
    }];
    AssertEqual(0, (long)self.db.count);
}


- (void) failing_testPurgeDoc {
    // Store doc:
    CBLMutableDocument* doc = [self generateDocument: @"doc1"];
    
    // Purge Doc:
    [self purgeDocAndVerify: doc];
    AssertEqual(0, (long)self.db.count);
    
    // Save to check sequence number -> 2
    [self saveDocument: doc];
    AssertEqual(2L, (long)doc.sequence);
}


- (void) testPurgeDocInDifferentDBInstance {
    // store doc
    NSString* docID = @"doc1";
    CBLMutableDocument* doc = [self generateDocument: docID];
    
    // create db instance with same name
    NSError* error;
    CBLDatabase* otherDB = [self openDBNamed: [self.db name] error: &error];
    AssertNil(error);
    AssertNotNil(otherDB);
    Assert(otherDB != self.db);
    Assert([otherDB contains:docID]);
    AssertEqual(1, (long)otherDB.count);
    
    // purge document against other db instance
    [self expectError: @"CouchbaseLite" code: 403 in: ^BOOL(NSError** error2) {
        return [otherDB purgeDocument: doc error: error2];
    }]; // forbidden
    AssertEqual(1, (long)otherDB.count);
    AssertEqual(1, (long)self.db.count);
    AssertFalse(doc.isDeleted);
    
    // close otherDB
    [self closeDatabase: otherDB];
}


- (void) testPurgeDocInDifferentDB {
    // store doc
    NSString* docID = @"doc1";
    CBLMutableDocument* doc = [self generateDocument: docID];
    
    // create db with different name
    NSError* error;
    CBLDatabase* otherDB =  [self openDBNamed: @"otherDB" error: &error];
    AssertNil(error);
    AssertNotNil(otherDB);
    Assert(otherDB != self.db);
    AssertFalse([otherDB contains: docID]);
    AssertEqual(0, (long)otherDB.count);
    
    // purge document against other db
    [self expectError: @"CouchbaseLite" code: 403 in: ^BOOL(NSError** error2) {
        return [otherDB purgeDocument: doc error: error2];
    }]; // forbidden
    
    AssertEqual(0, (long)otherDB.count);
    AssertEqual(1, (long)self.db.count);
    AssertFalse(doc.isDeleted);
    
    [self deleteDatabase: otherDB];
}


- (void) testPurgeSameDocTwice {
    // store doc
    NSString* docID = @"doc1";
    CBLMutableDocument* doc = [self generateDocument: docID];
    
    // get document for second purge
    CBLMutableDocument* doc1 = [[self.db documentWithID: docID] edit];
    AssertNotNil(doc1);
    
    // Purge Doc first time
    [self purgeDocAndVerify: doc];
    AssertEqual(0, (long)self.db.count);
    
    // Purge Doc second time
    [self purgeDocAndVerify: doc1];
    AssertEqual(0, (long)self.db.count);
}


- (void) testPurgeDocInBatch {
    // save 10 docs
    [self createDocs: 10];

    NSError* error;
    BOOL success = [self.db inBatch: &error do: ^{
        for(int i = 0; i < 10; i++){
            //NSError* err;
            NSString* docID = [[NSString alloc] initWithFormat: @"doc_%03d", i];
            CBLMutableDocument* doc = [[self.db documentWithID: docID] edit];
            [self purgeDocAndVerify: doc];
            AssertEqual(9 - i, (long)self.db.count);
        }
    }];
    Assert(success);
    AssertNil(error);
    AssertEqual(0, (long)self.db.count);
}


- (void) testPurgeDocOnClosedDB {
    // store doc
    CBLMutableDocument* doc = [self generateDocument: @"doc1"];
    
    // close db
    [self closeDatabase:self.db];
    
    // purge doc
    [self expectException: @"NSInternalInconsistencyException" in: ^{
        [self.db purgeDocument: doc error: nil];
    }];
}


- (void) testPurgeDocOnDeletedDB {
    // store doc
    CBLMutableDocument* doc = [self generateDocument: @"doc1"];
   
    // delete db
    [self deleteDatabase: self.db];
    
    // purge doc
    [self expectException: @"NSInternalInconsistencyException" in: ^{
        [self.db purgeDocument: doc error: nil];
    }];
}


#pragma mark - Close Database


- (void) testClose {
    // close db
    [self closeDatabase: self.db];
}


- (void) testCloseTwice {
    // close db twice
    [self closeDatabase: self.db];
    [self closeDatabase: self.db];
}


- (void) testCloseThenAccessDoc {
    // store doc
    NSString* docID = @"doc1";
    CBLMutableDocument* doc = [self generateDocument: docID];
    
    // close db
    [self closeDatabase: self.db];
    
    // content should be accessible & modifiable without error
    AssertEqualObjects(docID, doc.id);
    AssertEqualObjects(@(1), [doc objectForKey: @"key"]);
    [doc setObject:@(2) forKey: @"key"];
    [doc setObject: @"value" forKey: @"key1"];
}


- (void)testCloseThenAccessBlob {
    // store doc with blob
    CBLMutableDocument* doc = [self generateDocument: @"doc1"];
    [self storeBlob: self.db doc: doc content: [@"12345" dataUsingEncoding: NSUTF8StringEncoding]];
    CBLDocument* result = [self.db documentWithID: doc.id];
    
    // clsoe db
    [self closeDatabase: self.db];
    
    // content should be accessible & modifiable without error
    Assert([[doc objectForKey: @"data"] isKindOfClass: [CBLBlob class]]);
    CBLBlob* blob = [doc objectForKey: @"data"];
    AssertEqual(blob.length, 5ull);
    AssertNotNil(blob.content);
    
    Assert([[result objectForKey: @"data"] isKindOfClass: [CBLBlob class]]);
    blob = [result objectForKey: @"data"];
    AssertEqual(blob.length, 5ull);
    AssertNil(blob.content);
}


- (void) testCloseThenGetDatabaseName {
    // clsoe db
    [self closeDatabase: self.db];
    AssertEqualObjects(@"testdb", self.db.name);
}


- (void) testCloseThenGetDatabasePath {
    // clsoe db
    [self closeDatabase:self.db];
    
    
    
    AssertNil(self.db.path);
}


- (void) testCloseThenCallInBatch {
    NSError* error;
    BOOL success = [self.db inBatch: &error do: ^{
        [self expectError: @"LiteCore" code: 26 in: ^BOOL(NSError** error2) {
            return [self.db close: error2];
        }];
        // 26 -> kC4ErrorTransactionNotClosed
    }];
    Assert(success);
    AssertNil(error);
}


- (void) falingTestCloseThenDeleteDatabase {
    [self closeDatabase: self.db];
    [self deleteDatabase: self.db];
}


#pragma mark - Delete Database


- (void) testDelete {
    // delete db
    [self deleteDatabase: self.db];
}


- (void) testDeleteTwice {
    NSError* error;
    Assert([self.db deleteDatabase: &error]);
    [self expectException: @"NSInternalInconsistencyException" in: ^{
        [self.db deleteDatabase: nil];
    }];
}


- (void) testDeleteThenAccessDoc {
    // store doc
    NSString* docID = @"doc1";
    CBLMutableDocument* doc = [self generateDocument: docID];
    
    // delete db
    [self deleteDatabase: self.db];
    
    // content should be accessible & modifiable without error
    AssertEqualObjects(docID, doc.id);
    AssertEqualObjects(@(1), [doc objectForKey: @"key"]);
    [doc setObject: @(2) forKey: @"key"];
    [doc setObject: @"value" forKey: @"key1"];
}


- (void) testDeleteThenAccessBlob {
    // store doc with blob
    CBLMutableDocument* doc = [self generateDocument: @"doc1"];
    [self storeBlob: self.db doc: doc content: [@"12345" dataUsingEncoding: NSUTF8StringEncoding]];
    CBLDocument* result = [self.db documentWithID: doc.id];
    
    // delete db
    [self deleteDatabase: self.db];
    
    // content should be accessible & modifiable without error
    Assert([[doc objectForKey: @"data"] isKindOfClass: [CBLBlob class]]);
    CBLBlob* blob = [doc objectForKey: @"data"];
    AssertEqual(blob.length, 5ull);
    AssertNotNil(blob.content);
    
    Assert([[result objectForKey: @"data"] isKindOfClass: [CBLBlob class]]);
    blob = [result objectForKey: @"data"];
    AssertEqual(blob.length, 5ull);
    AssertNil(blob.content);
}


- (void) testDeleteThenGetDatabaseName {
    // delete db
    [self deleteDatabase: self.db];
    AssertEqualObjects(@"testdb", self.db.name);
}


- (void) testDeleteThenGetDatabasePath{
    // delete db
    [self deleteDatabase: self.db];
    
    
    AssertNil(self.db.path);
}


- (void) testDeleteThenCallInBatch {
    NSError* error;
    BOOL sucess = [self.db inBatch: &error do:^{
        [self expectError: @"LiteCore" code: 26 in: ^BOOL(NSError** error2) {
            return [self.db deleteDatabase: error2];
        }];
        // 26 -> kC4ErrorTransactionNotClosed: Function cannot be called while in a transaction
    }];
    Assert(sucess);
    AssertNil(error);
}


- (void) testDeleteDBOpendByOtherInstance {
    // open db with same db name and default option
    NSError* error;
    CBLDatabase* otherDB = [self openDBNamed: [self.db name] error: &error];
    AssertNil(error);
    AssertNotNil(otherDB);
    
    // delete db
    [self expectError: @"LiteCore" code: 24 in: ^BOOL(NSError** error2) {
        return [self.db deleteDatabase: error2];
    }];
    // 24 -> kC4ErrorBusy: Database is busy/locked
}


#pragma mark - Delate Database (static)


#if TARGET_OS_IPHONE
- (void) testDeleteWithDefaultDirDB {
    // open db with default dir
    NSError* error;
    CBLDatabase* db = [[CBLDatabase alloc] initWithName: @"db" error: &error];
    AssertNil(error);
    AssertNotNil(db);
    
    // Get path
    NSString* path = db.path;
    AssertNotNil(path);
    
    // close db before delete
    [self closeDatabase: db];
    
    // delete db with nil directory
    Assert([CBLDatabase deleteDatabase: @"db" inDirectory: nil error: &error]);
    AssertNil(error);
    AssertFalse([[NSFileManager defaultManager] fileExistsAtPath: path]);
}
#endif


#if TARGET_OS_IPHONE
- (void) testDeleteOpeningDBWithDefaultDir {
    // open db with default dir
    NSError* error;
    CBLDatabase* db = [[CBLDatabase alloc] initWithName: @"db" error: &error];
    AssertNil(error);
    AssertNotNil(db);
    
    // delete db with nil directory
    // 24 -> kC4ErrorBusy: Database is busy/locked
    [self expectError: @"LiteCore" code: 24 in: ^BOOL(NSError** error2) {
        return [CBLDatabase deleteDatabase: @"db" inDirectory: nil error: error2];
    }];
}
#endif


- (void) testDeleteByStaticMethod {
    // create db with custom directory
    NSError* error;
    NSString* dir = [NSTemporaryDirectory() stringByAppendingPathComponent: @"CouchbaseLite"];
    CBLDatabaseConfiguration* config = [[CBLDatabaseConfiguration alloc] init];
    config.directory = dir;
    CBLDatabase* db = [[CBLDatabase alloc] initWithName: @"db" config: config error: &error];
    AssertNotNil(db);
    AssertNil(error);
    
    NSString* path = db.path;
    
    // close db before delete
    [self closeDatabase: db];
    
    Assert([CBLDatabase deleteDatabase: @"db" inDirectory: dir error:&error]);
    AssertNil(error);
    AssertFalse([[NSFileManager defaultManager] fileExistsAtPath: path]);
}


- (void) testDeleteOpeningDBByStaticMethod {
    // create db with custom directory
    NSError* error;
    NSString* dir = [NSTemporaryDirectory() stringByAppendingPathComponent: @"CouchbaseLite"];
    CBLDatabaseConfiguration* config = [[CBLDatabaseConfiguration alloc] init];
    config.directory = dir;
    CBLDatabase* db = [[CBLDatabase alloc] initWithName: @"db" config: config error: &error];
    AssertNotNil(db);
    AssertNil(error);
    
    [self expectError: @"LiteCore" code: 24 in: ^BOOL(NSError** error2) {
        return [CBLDatabase deleteDatabase: @"db" inDirectory: dir error: error2];
    }];
    // 24 -> kC4ErrorBusy: Database is busy/locked
}


#if TARGET_OS_IPHONE
- (void) testDeleteNonExistingDBWithDefaultDir {
    // Expectation: No operation
    NSError* error;
    Assert([CBLDatabase deleteDatabase: @"notexistdb" inDirectory: nil error: &error]);
    AssertNil(error);
}
#endif


- (void) testDeleteNonExistingDB {
    // Expectation: No operation
    NSError* error;
    NSString* dir = [NSTemporaryDirectory() stringByAppendingPathComponent: @"CouchbaseLite"];
    Assert([CBLDatabase deleteDatabase: @"notexistdb" inDirectory: dir error: &error]);
    AssertNil(error);
}


#pragma mark - Database Existing


#if TARGET_OS_IPHONE
- (void) testDatabaseExistsWithDefaultDir {
    AssertFalse([CBLDatabase databaseExists: @"db" inDirectory: nil]);
    
    // open db with default dir
    NSError* error;
    CBLDatabase* db = [[CBLDatabase alloc] initWithName: @"db" error: &error];
    Assert([CBLDatabase databaseExists: @"db" inDirectory: nil]);
    
    // delete db
    [self deleteDatabase: db];
    
    AssertFalse([CBLDatabase databaseExists: @"db" inDirectory: nil]);
}
#endif


- (void) testDatabaseExistsWithDir {
    NSError* error;
    NSString* dir = [NSTemporaryDirectory() stringByAppendingPathComponent: @"CouchbaseLite"];
    
    AssertFalse([CBLDatabase databaseExists:@"db" inDirectory:dir]);
    
    // create db with custom directory
    CBLDatabaseConfiguration* config = [[CBLDatabaseConfiguration alloc] init];
    config.directory = dir;
    CBLDatabase* db = [[CBLDatabase alloc] initWithName: @"db" config: config error: &error];
    AssertNotNil(db);
    AssertNil(error);
    NSString* path = db.path;
    
    Assert([CBLDatabase databaseExists: @"db" inDirectory: dir]);
    
    // close db
    [self closeDatabase: db];
    
    Assert([CBLDatabase databaseExists: @"db" inDirectory: dir]);
    
    // delete db
    Assert([CBLDatabase deleteDatabase: @"db" inDirectory: dir error: &error]);
    AssertNil(error);
    AssertFalse([[NSFileManager defaultManager] fileExistsAtPath: path]);
    
    AssertFalse([CBLDatabase databaseExists: @"db" inDirectory: dir]);
}


#if TARGET_OS_IPHONE
- (void) testDatabaseExistsAgainstNonExistDBWithDefaultDir {
    AssertFalse([CBLDatabase databaseExists: @"nonexist" inDirectory: nil]);
}
#endif


- (void) testDatabaseExistsAgainstNonExistDB {
    NSString* dir = [NSTemporaryDirectory() stringByAppendingPathComponent: @"CouchbaseLite"];
    AssertFalse([CBLDatabase databaseExists: @"nonexist" inDirectory: dir]);
}


- (void) testCompact {
    NSArray* docs = [self createDocs: 20];
    
    // Update each doc 25 times:
    NSError* error;
    [_db inBatch: &error do: ^{
        for (CBLMutableDocument* doc in docs) {
            for (NSUInteger i = 0; i < 25; i++) {
                [doc setObject: @(i) forKey: @"number"];
                [self saveDocument: doc];
            }
        }
    }];
    
    // Add each doc with a blob object:
    for (CBLMutableDocument* doc in docs) {
        NSData* content = [doc.id dataUsingEncoding: NSUTF8StringEncoding];
        CBLBlob* blob = [[CBLBlob alloc] initWithContentType:@"text/plain" data: content];
        [doc setObject: blob forKey: @"blob"];
        [self saveDocument: doc];
    }
    
    AssertEqual(_db.count, 20u);
    
    NSString* attsDir = [_db.path stringByAppendingPathComponent:@"Attachments"];
    NSArray* atts = [[NSFileManager defaultManager] contentsOfDirectoryAtPath: attsDir error: nil];
    AssertEqual(atts.count, 20u);
    
    // Compact:
    Assert([_db compact: &error], @"Error when compacting the database");
    
    // Delete all docs:
    for (CBLMutableDocument* doc in docs) {
        Assert([_db deleteDocument:  [_db documentWithID: doc.id] error: &error], @"Error when deleting doc: %@", error);
    }
    AssertEqual(_db.count, 0u);
    
    // Compact:
    Assert([_db compact: &error], @"Error when compacting the database: %@", error);
    
    atts = [[NSFileManager defaultManager] contentsOfDirectoryAtPath: attsDir error: nil];
    AssertEqual(atts.count, 0u);
}


- (void) testCopy {
    for (NSUInteger i = 0; i < 10; i++) {
        NSString* docID = [NSString stringWithFormat: @"doc%lu", (unsigned long)i];
        CBLMutableDocument* doc = [self createDocument: docID];
        [doc setObject: docID forKey: @"name"];
        
        NSData* data = [docID dataUsingEncoding: NSUTF8StringEncoding];
        CBLBlob* blob = [[CBLBlob alloc] initWithContentType: @"text/plain" data: data];
        [doc setObject: blob forKey: @"data"];
        
        [self saveDocument: doc];
    }
    
    NSString* dbName = @"nudb";
    CBLDatabaseConfiguration* config = _db.config;
    NSString* dir = config.directory;
    
    // Make sure no an existing database at the new location:
    Assert([CBLDatabase deleteDatabase: dbName inDirectory: dir error: nil]);
    
    // Copy:
    NSError* error;
    Assert([CBLDatabase copyFromPath: _db.path toDatabase: dbName config: config error: &error],
           @"Error when copying the database: %@", error);
    
    // Verify:
    Assert([CBLDatabase databaseExists: dbName inDirectory: dir]);
    CBLDatabase* nudb = [[CBLDatabase alloc] initWithName: dbName config: config  error: &error];
    Assert(nudb, @"Cannot open the new database: %@", error);
    AssertEqual(nudb.count, 10u);
    
    CBLQueryExpression* DOCID = [CBLQueryExpression meta].id;
    CBLQuerySelectResult* S_DOCID = [CBLQuerySelectResult expression: DOCID];
    CBLQuery* query = [CBLQuery select: @[S_DOCID]
                                  from: [CBLQueryDataSource database: nudb]];
    CBLQueryResultSet* rs = [query run: &error];
    
    for (CBLQueryResult* r in rs) {
        NSString* docID = [r stringAtIndex: 0];
        Assert(docID);
        
        CBLMutableDocument* doc = [[nudb documentWithID: docID] edit];
        Assert(doc);
        AssertEqualObjects([doc stringForKey:@"name"], docID);
        
        CBLBlob* blob = [doc blobForKey: @"data"];
        Assert(blob);
        
        NSString* data = [[NSString alloc] initWithData: blob.content encoding: NSUTF8StringEncoding];
        AssertEqualObjects(data, docID);
    }
    
    // Clean up:
    Assert([nudb close: nil]);
    Assert([CBLDatabase deleteDatabase: dbName inDirectory: dir error: nil]);
}


- (void) testCreateIndex {
    // Precheck:
    Assert(self.db.indexes);
    AssertEqual(self.db.indexes.count, 0u);
    
    // Create value index:
    CBLQueryExpression* fName = [CBLQueryExpression property: @"firstName"];
    CBLQueryExpression* lName = [CBLQueryExpression property: @"lastName"];
    
    CBLValueIndexItem* fNameItem = [CBLValueIndexItem expression: fName];
    CBLValueIndexItem* lNameItem = [CBLValueIndexItem expression: lName];
    
    NSError* error;
    
    CBLIndex* index1 = [CBLIndex valueIndexOn: @[fNameItem, lNameItem]];
    Assert([self.db createIndex: index1 withName: @"index1" error: &error],
           @"Error when creating value index: %@", error);
    
    // Create FTS index:
    CBLQueryExpression* detail  = [CBLQueryExpression property: @"detail"];
    CBLFTSIndexItem* detailItem = [CBLFTSIndexItem expression: detail];
    CBLIndex* index2 = [CBLIndex ftsIndexOn: detailItem options: nil];
    Assert([self.db createIndex: index2 withName: @"index2" error: &error],
           @"Error when creating FTS index without options: %@", error);
    
    CBLQueryExpression* detail2 = [CBLQueryExpression property: @"es-detail"];
    CBLFTSIndexItem* detailItem2 = [CBLFTSIndexItem expression: detail2];
    CBLFTSIndexOptions* options = [[CBLFTSIndexOptions alloc] init];
    options.locale = @"es";
    options.ignoreAccents = YES;
    CBLIndex* index3 = [CBLIndex ftsIndexOn: detailItem2 options: options];
    Assert([self.db createIndex: index3 withName: @"index3" error: &error],
           @"Error when creating FTS index with options: %@", error);
    
    NSArray* names = self.db.indexes;
    AssertEqual(names.count, 3u);
    AssertEqualObjects(names, (@[@"index1", @"index2", @"index3"]));
}


- (void) testCreateSameIndexTwice {
    // Create index with first name:
    NSError* error;
    CBLValueIndexItem* item = [CBLValueIndexItem expression:
                               [CBLQueryExpression property: @"firstName"]];
    CBLIndex* index = [CBLIndex valueIndexOn: @[item]];
    Assert([self.db createIndex: index withName: @"myindex" error: &error],
           @"Error when creating value index: %@", error);
    
    // Call create index again:
    Assert([self.db createIndex: index withName: @"myindex" error: &error],
           @"Error when creating value index: %@", error);
    
    NSArray* names = self.db.indexes;
    AssertEqual(names.count, 1u);
    AssertEqualObjects(names, (@[@"myindex"]));
}


- (void) testCreateSameNameIndexes {
    NSError* error;
    
    CBLQueryExpression* fName = [CBLQueryExpression property: @"firstName"];
    CBLQueryExpression* lName = [CBLQueryExpression property: @"lastName"];
    CBLQueryExpression* detail  = [CBLQueryExpression property: @"detail"];
    
    // Create value index with first name:
    CBLValueIndexItem* fNameItem = [CBLValueIndexItem expression: fName];
    CBLIndex* fNameIndex = [CBLIndex valueIndexOn: @[fNameItem]];
    Assert([self.db createIndex: fNameIndex withName: @"myindex" error: &error],
           @"Error when creating value index: %@", error);

    // Create value index with last name:
    CBLValueIndexItem* lNameItem = [CBLValueIndexItem expression: lName];
    CBLIndex* lNameIndex = [CBLIndex valueIndexOn: @[lNameItem]];
    Assert([self.db createIndex: lNameIndex withName: @"myindex" error: &error],
           @"Error when creating value index: %@", error);
    
    // Check:
    NSArray* names = self.db.indexes;
    AssertEqual(names.count, 1u);
    AssertEqualObjects(names, (@[@"myindex"]));
    
    // Create FTS index:
    CBLFTSIndexItem* detailItem = [CBLFTSIndexItem expression: detail];
    CBLIndex* detailIndex = [CBLIndex ftsIndexOn: detailItem options: nil];
    Assert([self.db createIndex: detailIndex withName: @"myindex" error: &error],
           @"Error when creating FTS index without options: %@", error);
    
    // Check:
    names = self.db.indexes;
    AssertEqual(names.count, 1u);
    AssertEqualObjects(names, (@[@"myindex"]));
}


- (void) testDeleteIndex {
    // Precheck:
    AssertEqual(self.db.indexes.count, 0u);
    
    // Create value index:
    CBLQueryExpression* fName = [CBLQueryExpression property: @"firstName"];
    CBLQueryExpression* lName = [CBLQueryExpression property: @"lastName"];
    
    CBLValueIndexItem* fNameItem = [CBLValueIndexItem expression: fName];
    CBLValueIndexItem* lNameItem = [CBLValueIndexItem expression: lName];
    
    NSError* error;
    
    CBLIndex* index1 = [CBLIndex valueIndexOn: @[fNameItem, lNameItem]];
    Assert([self.db createIndex: index1 withName: @"index1" error: &error],
           @"Error when creating value index: %@", error);
    
    // Create FTS index:
    CBLQueryExpression* detail  = [CBLQueryExpression property: @"detail"];
    CBLFTSIndexItem* detailItem = [CBLFTSIndexItem expression: detail];
    CBLIndex* index2 = [CBLIndex ftsIndexOn: detailItem options: nil];
    Assert([self.db createIndex: index2 withName: @"index2" error: &error],
           @"Error when creating FTS index without options: %@", error);
    
    CBLQueryExpression* detail2 = [CBLQueryExpression property: @"es-detail"];
    CBLFTSIndexItem* detail2Item = [CBLFTSIndexItem expression: detail2];
    CBLFTSIndexOptions* options = [[CBLFTSIndexOptions alloc] init];
    options.locale = @"es";
    options.ignoreAccents = YES;
    CBLIndex* index3 = [CBLIndex ftsIndexOn: detail2Item options: options];
    Assert([self.db createIndex: index3 withName: @"index3" error: &error],
           @"Error when creating FTS index with options: %@", error);
    
    NSArray* names = self.db.indexes;
    AssertEqual(names.count, 3u);
    AssertEqualObjects(names, (@[@"index1", @"index2", @"index3"]));
    
    // Delete indexes:
    Assert([self.db deleteIndexForName: @"index1" error: &error]);
    names = self.db.indexes;
    AssertEqual(names.count, 2u);
    AssertEqualObjects(names, (@[@"index2", @"index3"]));
    
    Assert([self.db deleteIndexForName: @"index2" error: &error]);
    names = self.db.indexes;
    AssertEqual(names.count, 1u);
    AssertEqualObjects(names, (@[@"index3"]));
    
    Assert([self.db deleteIndexForName: @"index3" error: &error]);
    names = self.db.indexes;
    Assert(names);
    AssertEqual(names.count, 0u);
    
    // Delete non existing index:
    Assert([self.db deleteIndexForName: @"dummy" error: &error]);
    
    // Delete deleted indexes:
    Assert([self.db deleteIndexForName: @"index1" error: &error]);
    Assert([self.db deleteIndexForName: @"index2" error: &error]);
    Assert([self.db deleteIndexForName: @"index3" error: &error]);
}

@end
