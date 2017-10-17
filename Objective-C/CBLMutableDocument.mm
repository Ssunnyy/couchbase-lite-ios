//
//  CBLMutableDocument.m
//  CouchbaseLite
//
//  Created by Pasin Suriyentrakorn on 12/29/16.
//  Copyright © 2016 Couchbase. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

#import "CBLMutableDocument.h"
#import "CBLMutableArray.h"
#import "CBLC4Document.h"
#import "CBLConflictResolver.h"
#import "CBLData.h"
#import "CBLCoreBridge.h"
#import "CBLDocument+Internal.h"
#import "CBLDatabase+Internal.h"
#import "CBLJSON.h"
#import "CBLMisc.h"
#import "CBLSharedKeys.hh"
#import "CBLStringBytes.h"
#import "CBLStatus.h"


@implementation CBLMutableDocument {
    CBLMutableDictionary* _dict;
}

#pragma mark - Initializer

+ (instancetype) document {
    return [[self alloc] initWithID: nil];
}


+ (instancetype) documentWithID: (nullable NSString*)documentID {
    return [[self alloc] initWithID: documentID];
}


- (instancetype) initWithDatabase: (CBLDatabase*)database
                       documentID: (NSString*)documentID
                            c4Doc: (nullable CBLC4Document*)c4Doc
{
    self = [super initWithDatabase: database
                        documentID: documentID
                             c4Doc: c4Doc];
    if (self) {
        _dict = [[CBLMutableDictionary alloc] initWithFleeceData: self.data];
    }
    return self;
}


- (instancetype) init {
    return [self initWithID: nil];
}


- (instancetype) initWithID: (nullable NSString*)documentID {
    return [self initWithDatabase: nil
                       documentID: (documentID ?: CBLCreateUUID())
                            c4Doc: nil];
}


- (instancetype) initWithDictionary: (NSDictionary<NSString*,id>*)dictionary {
    self = [self initWithID: nil];
    if (self) {
        [self setDictionary: dictionary];
    }
    return self;
}


- (instancetype) initWithID: (nullable NSString*)documentID
                 dictionary: (NSDictionary<NSString*,id>*)dictionary
{
    self = [self initWithID: documentID];
    if (self) {
        [self setDictionary: dictionary];
    }
    return self;
}


/* internal */ - (instancetype) initWithDocument: (CBLDocument*)doc {
    return [self initWithDatabase: doc.database
                       documentID: doc.id
                            c4Doc: doc.c4Doc];
    
}


- (CBLMutableDocument*) edit {
    return self;
}


#pragma mark - CBLDictionary


- (NSUInteger) count {
    return _dict.count;
}


- (NSArray*) keys {
    return _dict.keys;
}


- (nullable CBLMutableArray*) arrayForKey: (NSString*)key {
    return [_dict arrayForKey: key];
}


- (nullable CBLBlob*) blobForKey: (NSString*)key {
    return [_dict blobForKey: key];
}


- (BOOL) booleanForKey: (NSString*)key {
    return [_dict booleanForKey: key];
}


- (nullable NSDate*) dateForKey: (NSString*)key {
    return [_dict dateForKey: key];
}


- (nullable CBLMutableDictionary*) dictionaryForKey: (NSString*)key {
    return [_dict dictionaryForKey: key];
}

- (double) doubleForKey: (NSString*)key {
    return [_dict doubleForKey: key];
}


- (float) floatForKey: (NSString*)key {
    return [_dict floatForKey: key];
}


- (NSInteger) integerForKey: (NSString*)key {
    return [_dict integerForKey: key];
}


- (long long) longLongForKey: (NSString*)key {
    return [_dict longLongForKey: key];
}


- (nullable NSNumber*) numberForKey: (NSString*)key {
    return [_dict numberForKey: key];
}


- (nullable id) objectForKey: (NSString*)key {
    return [_dict objectForKey: key];
}


- (nullable NSString*) stringForKey: (NSString*)key {
    return [_dict stringForKey: key];
}


- (BOOL) containsObjectForKey: (NSString*)key {
    return [_dict containsObjectForKey: key];
}


- (NSDictionary<NSString*,id>*) toDictionary {
    return [_dict toDictionary];
}


#pragma mark - CBLMutableDictionary


- (void) setArray: (nullable CBLMutableArray *)value forKey: (NSString *)key {
    [_dict setArray: value forKey: key];
}


- (void) setBoolean: (BOOL)value forKey: (NSString *)key {
    [_dict setBoolean: value forKey: key];
}


- (void) setBlob: (nullable CBLBlob*)value forKey: (NSString *)key {
    [_dict setBlob: value forKey: key];
}


- (void) setDate: (nullable NSDate *)value forKey: (NSString *)key {
    [_dict setDate: value forKey: key];
}


- (void) setDictionary: (nullable CBLMutableDictionary *)value forKey: (NSString *)key {
    [_dict setDictionary: value forKey: key];
}


- (void) setDouble: (double)value forKey: (NSString *)key {
    [_dict setDouble: value forKey: key];
}


- (void) setFloat: (float)value forKey: (NSString *)key {
    [_dict setFloat: value forKey: key];
}


- (void) setInteger: (NSInteger)value forKey: (NSString *)key {
    [_dict setInteger: value forKey: key];
}


- (void) setLongLong: (long long)value forKey: (NSString *)key {
    [_dict setLongLong: value forKey: key];
}


- (void) setNumber: (nullable NSNumber*)value forKey: (NSString *)key {
    [self setNumber: value forKey: key];
}


- (void) setObject: (nullable id)value forKey: (NSString*)key {
    [_dict setObject: value forKey: key];
}


- (void) setString: (nullable NSString *)value forKey: (NSString *)key {
    [_dict setString: value forKey: key];
}


- (void) removeObjectForKey: (NSString *)key {
    [_dict removeObjectForKey: key];
}


- (void) setDictionary: (NSDictionary<NSString *,id> *)dictionary {
    [_dict setDictionary: dictionary];
}


#pragma mark - NSFastEnumeration


- (NSUInteger)countByEnumeratingWithState: (NSFastEnumerationState *)state
                                  objects: (id __unsafe_unretained [])buffer
                                    count: (NSUInteger)len
{
    return [_dict countByEnumeratingWithState: state objects: buffer count: len];
}


#pragma mark - Subscript


- (CBLMutableFragment*) objectForKeyedSubscript: (NSString*)key {
    return [_dict objectForKeyedSubscript: key];
}


#pragma mark - Internal


- (NSUInteger) generation {
    return super.generation + !!self.changed;
}


- (BOOL) isEmpty {
    return _dict.isEmpty;
}

#pragma mark - Private


// Reflects only direct changes to the document. Changes on sub dictionaries or arrays will
// not be propagated here.
- (BOOL) changed {
    return _dict.changed;
}

#pragma mark - Fleece Encodable


- (NSData*) encode: (NSError**)outError {
    auto encoder = c4db_createFleeceEncoder(self.c4db);
    if (![_dict cbl_fleeceEncode: encoder database: self.database error: outError])
        return nil;
    FLError flErr;
    FLSliceResult body = FLEncoder_Finish(encoder, &flErr);
    FLEncoder_Free(encoder);
    if (!body.buf)
        convertError(flErr, outError);
    return sliceResult2data(body);
}


@end
