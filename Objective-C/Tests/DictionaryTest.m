//
//  DictionaryTest.m
//  CouchbaseLite
//
//  Created by Pasin Suriyentrakorn on 2/13/17.
//  Copyright © 2017 Couchbase. All rights reserved.
//

#import "CBLTestCase.h"

@interface DictionaryTest : CBLTestCase

@end

@implementation DictionaryTest


- (void) testCreateDictionary {
    CBLMutableDictionary* address = [[CBLMutableDictionary alloc] init];
    AssertEqual(address.count, 0u);
    AssertEqualObjects([address toDictionary], @{});
    
    CBLMutableDocument* doc = [self createDocument: @"doc1"];
    [doc setObject: address forKey: @"address"];
    AssertEqual([doc dictionaryForKey: @"address"], address);
    
    CBLDocument* nuDoc = [self saveDocument: doc];
    AssertEqualObjects([[nuDoc dictionaryForKey: @"address"] toDictionary], @{});
}


- (void) testCreateDictionaryWithNSDictionary {
    NSDictionary* dict = @{@"street": @"1 Main street",
                           @"city": @"Mountain View",
                           @"state": @"CA"};
    CBLMutableDictionary* address = [[CBLMutableDictionary alloc] initWithDictionary: dict];
    AssertEqualObjects([address objectForKey: @"street"], @"1 Main street");
    AssertEqualObjects([address objectForKey: @"city"], @"Mountain View");
    AssertEqualObjects([address objectForKey: @"state"], @"CA");
    AssertEqualObjects([address toDictionary], dict);
    
    CBLMutableDocument* doc = [self createDocument: @"doc1"];
    [doc setObject: address forKey: @"address"];
    AssertEqual([doc dictionaryForKey: @"address"], address);
    
    CBLDocument* nuDoc = [self saveDocument: doc];
    AssertEqualObjects([[nuDoc dictionaryForKey: @"address"] toDictionary], dict);
}


- (void) testGetValueFromNewEmptyDictionary {
    CBLMutableDictionary* mDict = [[CBLMutableDictionary alloc] init];
    
    AssertEqual([mDict integerForKey: @"key"], 0);
    AssertEqual([mDict floatForKey: @"key"], 0.0f);
    AssertEqual([mDict doubleForKey: @"key"], 0.0);
    AssertEqual([mDict booleanForKey: @"key"], NO);
    AssertNil([mDict blobForKey: @"key"]);
    AssertNil([mDict dateForKey: @"key"]);
    AssertNil([mDict numberForKey: @"key"]);
    AssertNil([mDict objectForKey: @"key"]);
    AssertNil([mDict stringForKey: @"key"]);
    AssertNil([mDict dictionaryForKey: @"key"]);
    AssertNil([mDict arrayForKey: @"key"]);
    AssertEqualObjects([mDict toDictionary], @{});
    
    CBLMutableDocument* doc = [[CBLMutableDocument alloc] initWithID: @"doc1"];
    [doc setObject: mDict forKey: @"dict"];
    
    CBLDocument* nuDoc = [self saveDocument: doc];
    
    CBLDictionary* dict = [nuDoc dictionaryForKey: @"dict"];
    AssertEqual([dict integerForKey: @"key"], 0);
    AssertEqual([dict floatForKey: @"key"], 0.0f);
    AssertEqual([dict doubleForKey: @"key"], 0.0);
    AssertEqual([dict booleanForKey: @"key"], NO);
    AssertNil([dict blobForKey: @"key"]);
    AssertNil([dict dateForKey: @"key"]);
    AssertNil([dict numberForKey: @"key"]);
    AssertNil([dict objectForKey: @"key"]);
    AssertNil([dict stringForKey: @"key"]);
    AssertNil([dict dictionaryForKey: @"key"]);
    AssertNil([dict arrayForKey: @"key"]);
    AssertEqualObjects([dict toDictionary], @{});
}


- (void) testSetNestedDictionaries {
    CBLMutableDocument* doc = [self createDocument: @"doc1"];
    
    CBLMutableDictionary *level1 = [[CBLMutableDictionary alloc] init];
    [level1 setObject: @"n1" forKey: @"name"];
    [doc setObject: level1 forKey: @"level1"];
    
    CBLMutableDictionary *level2 = [[CBLMutableDictionary alloc] init];
    [level2 setObject: @"n2" forKey: @"name"];
    [level1 setObject: level2 forKey: @"level2"];
    
    CBLMutableDictionary *level3 = [[CBLMutableDictionary alloc] init];
    [level3 setObject: @"n3" forKey: @"name"];
    [level2 setObject: level3 forKey: @"level3"];
    
    AssertEqualObjects([doc dictionaryForKey: @"level1"], level1);
    AssertEqualObjects([level1 dictionaryForKey: @"level2"], level2);
    AssertEqualObjects([level2 dictionaryForKey: @"level3"], level3);
    NSDictionary* dict = @{@"level1": @{@"name": @"n1",
                                        @"level2": @{@"name": @"n2",
                                                     @"level3": @{@"name": @"n3"}}}};
    AssertEqualObjects([doc toDictionary], dict);
    
    CBLDocument* nuDoc = [self saveDocument: doc];
    
    Assert([nuDoc dictionaryForKey: @"level1"] != level1);
    AssertEqualObjects([nuDoc toDictionary], dict);
}


- (void) testDictionaryArray {
    CBLMutableDocument* doc = [self createDocument: @"doc1"];
    NSArray* data = @[@{@"name": @"1"}, @{@"name": @"2"}, @{@"name": @"3"}, @{@"name": @"4"}];
    [doc setDictionary: @{@"dicts": data}];
    
    CBLMutableArray* dicts = [doc arrayForKey: @"dicts"];
    AssertEqual(dicts.count, 4u);
    
    CBLMutableDictionary* d1 = [dicts dictionaryAtIndex: 0];
    CBLMutableDictionary* d2 = [dicts dictionaryAtIndex: 1];
    CBLMutableDictionary* d3 = [dicts dictionaryAtIndex: 2];
    CBLMutableDictionary* d4 = [dicts dictionaryAtIndex: 3];
    
    AssertEqualObjects([d1 stringForKey: @"name"], @"1");
    AssertEqualObjects([d2 stringForKey: @"name"], @"2");
    AssertEqualObjects([d3 stringForKey: @"name"], @"3");
    AssertEqualObjects([d4 stringForKey: @"name"], @"4");
    
    CBLDocument* nuDoc = [self saveDocument: doc];
    
    CBLArray* nuDicts = [nuDoc arrayForKey: @"dicts"];
    AssertEqual(nuDicts.count, 4u);
    
    CBLDictionary* nuD1 = [nuDicts dictionaryAtIndex: 0];
    CBLDictionary* nuD2 = [nuDicts dictionaryAtIndex: 1];
    CBLDictionary* nuD3 = [nuDicts dictionaryAtIndex: 2];
    CBLDictionary* nuD4 = [nuDicts dictionaryAtIndex: 3];
    
    AssertEqualObjects([nuD1 stringForKey: @"name"], @"1");
    AssertEqualObjects([nuD2 stringForKey: @"name"], @"2");
    AssertEqualObjects([nuD3 stringForKey: @"name"], @"3");
    AssertEqualObjects([nuD4 stringForKey: @"name"], @"4");
}


- (void) testReplaceDictionary {
    CBLMutableDocument* doc = [self createDocument: @"doc1"];
    CBLMutableDictionary *profile1 = [[CBLMutableDictionary alloc] init];
    [profile1 setObject: @"Scott Tiger" forKey: @"name"];
    [doc setObject: profile1 forKey: @"profile"];
    AssertEqualObjects([doc dictionaryForKey: @"profile"], profile1);
    
    CBLMutableDictionary *profile2 = [[CBLMutableDictionary alloc] init];
    [profile2 setObject: @"Daniel Tiger" forKey: @"name"];
    [doc setObject: profile2 forKey: @"profile"];
    AssertEqualObjects([doc dictionaryForKey: @"profile"], profile2);
    
    // Profile1 should be now detached:
    [profile1 setObject: @(20) forKey: @"age"];
    AssertEqualObjects([profile1 objectForKey: @"name"], @"Scott Tiger");
    AssertEqualObjects([profile1 objectForKey: @"age"], @(20));
    
    // Check profile2:
    AssertEqualObjects([profile2 objectForKey: @"name"], @"Daniel Tiger");
    AssertNil([profile2 objectForKey: @"age"]);
    
    // Save:
    CBLDocument* nuDoc = [self saveDocument: doc];
    
    Assert([nuDoc dictionaryForKey: @"profile"] != profile2);
    CBLDictionary* resultProfile = [nuDoc dictionaryForKey: @"profile"];
    AssertEqualObjects([resultProfile objectForKey: @"name"], @"Daniel Tiger");
}


- (void) testReplaceDictionaryDifferentType {
    CBLMutableDocument* doc = [self createDocument: @"doc1"];
    CBLMutableDictionary *profile1 = [[CBLMutableDictionary alloc] init];
    [profile1 setObject: @"Scott Tiger" forKey: @"name"];
    [doc setObject: profile1 forKey: @"profile"];
    AssertEqualObjects([doc dictionaryForKey: @"profile"], profile1);
    
    // Set string value to profile:
    [doc setObject: @"Daniel Tiger" forKey: @"profile"];
    AssertEqualObjects([doc objectForKey: @"profile"], @"Daniel Tiger");
    
    // Profile1 should be now detached:
    [profile1 setObject: @(20) forKey: @"age"];
    AssertEqualObjects([profile1 objectForKey: @"name"], @"Scott Tiger");
    AssertEqualObjects([profile1 objectForKey: @"age"], @(20));

    // Check whether the profile value has no change:
    AssertEqualObjects([doc objectForKey: @"profile"], @"Daniel Tiger");
    
    // Save:
    CBLDocument* nuDoc = [self saveDocument: doc];
    
    AssertEqualObjects([nuDoc objectForKey: @"profile"], @"Daniel Tiger");
}


- (void) testRemoveDictionary {
    CBLMutableDocument* doc = [self createDocument: @"doc1"];
    CBLMutableDictionary *profile1 = [[CBLMutableDictionary alloc] init];
    [profile1 setObject: @"Scott Tiger" forKey: @"name"];
    [doc setObject: profile1 forKey: @"profile"];
    AssertEqualObjects([doc dictionaryForKey: @"profile"], profile1);
    Assert([doc containsObjectForKey: @"profile"]);
    
    // Remove profile
    [doc removeObjectForKey: @"profile"];
    AssertNil([doc objectForKey: @"profile"]);
    AssertFalse([doc containsObjectForKey: @"profile"]);
    
    // Profile1 should be now detached:
    [profile1 setObject: @(20) forKey: @"age"];
    AssertEqualObjects([profile1 objectForKey: @"name"], @"Scott Tiger");
    AssertEqualObjects([profile1 objectForKey: @"age"], @(20));
    
    // Check whether the profile value has no change:
    AssertNil([doc objectForKey: @"profile"]);
    
    // Save:
    CBLDocument* nuDoc = [self saveDocument: doc];
    
    AssertNil([nuDoc objectForKey: @"profile"]);
    AssertFalse([nuDoc containsObjectForKey: @"profile"]);
}


- (void) testEnumeratingKeys {
    CBLMutableDictionary *dict = [[CBLMutableDictionary alloc] init];
    for (NSInteger i = 0; i < 20; i++) {
        [dict setObject: @(i) forKey: [NSString stringWithFormat:@"key%ld", (long)i]];
    }
    NSDictionary* content = [dict toDictionary];
    
    __block NSMutableDictionary* result = [NSMutableDictionary dictionary];
    __block NSUInteger count = 0;
    for (NSString* key in dict) {
        result[key] = [dict objectForKey: key];
        count++;
    }
    AssertEqualObjects(result, content);
    AssertEqual(count, content.count);
    
    // Update:
    
    [dict setObject: nil forKey: @"key2"];
    [dict setObject: @(20) forKey: @"key20"];
    [dict setObject: @(21) forKey: @"key21"];
    content = [dict toDictionary];
    
    result = [NSMutableDictionary dictionary];
    count = 0;
    for (NSString* key in dict) {
        result[key] = [dict objectForKey: key];
        count++;
    }
    AssertEqualObjects(result, content);
    AssertEqual(count, content.count);
    
    CBLMutableDocument* doc = [self createDocument: @"doc1"];
    [doc setObject: dict forKey: @"dict"];
    
    [self saveDocument: doc eval:^(CBLDocument *d) {
        result = [NSMutableDictionary dictionary];
        count = 0;
        CBLDictionary* dictObj = [d dictionaryForKey: @"dict"];
        for (NSString* key in dictObj) {
            result[key] = [dictObj objectForKey: key];
            count++;
        }
        AssertEqualObjects(result, content);
        AssertEqual(count, content.count);
    }];
}


@end
