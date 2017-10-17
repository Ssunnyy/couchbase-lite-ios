//
//  CRUDTest.m
//  CouchbaseLite
//
//  Created by Pasin Suriyentrakorn on 10/17/17.
//  Copyright © 2017 Couchbase. All rights reserved.
//

#import "CBLTestCase.h"

@interface CRUDTest : CBLTestCase

@end

@implementation CRUDTest

/**
 Immutable Objects:
   CBLDocument          <- CBLReadOnlyDocument
   CBLDictionary        <- CBLReadOnlyDictionary
   CBLArray             <- CBLReadOnlyArray
 
 Mutable Objects:
   CBLMutableDocument   <- CBLDocument
   CBLMutableDictionary <- CBLDictionary
   CBLMutableArray      <- CBLArray
 
 CBLDatabase:
 - (BOOL) saveDocument:   (CBLMutableDocument*)document error: (NSError**)error;
    * May return a new CBLDocument object
 - (BOOL) deleteDocument: (CBLDocument*)document        error: (NSError**)error;
    * May accept docID instead of document object
 - (BOOL) purgeDocument:  (CBLDocument*)document        error: (NSError**)error;
    * May accept docID instead of document object
 
 CBLDocument:
 - (CBLMutableDocument*) edit;
 
 Note:
 - CBLDocument and CBLMutableDocument turn to be revision snapshot.
   When they are saved or deleted, the save or delete operation doesn't mutate
   its internal status.
 - All Immutable objects are thread safe; All Mutable objects are nto thread safe.
 */

- (void) testBasicOperations {
    // Create:
    CBLMutableDocument* mDoc = [[CBLMutableDocument alloc] init];
    [mDoc setObject: @"Scott" forKey: @"name"];
    CBLDocument* savedDoc = [self.db saveDocument: mDoc error: nil];
    AssertNotNil(savedDoc);
    
    // Read:
    CBLDocument* doc = [self.db documentWithID: mDoc.id];
    
    // Update:
    mDoc = [doc edit];
    [mDoc setObject: @"David" forKey: @"name"];
    savedDoc = [self.db saveDocument: mDoc error: nil];
    AssertNotNil(savedDoc);
    
    // Delete:
    doc = [self.db documentWithID: mDoc.id];
    [self.db deleteDocument: doc error: nil];
}

@end
