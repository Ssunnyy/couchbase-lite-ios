//
//  CBLFragment.m
//  CouchbaseLite
//
//  Created by Pasin Suriyentrakorn on 4/21/17.
//  Copyright © 2017 Couchbase. All rights reserved.
//

#import "CBLFragment.h"
#import "CBLBlob.h"
#import "CBLJSON.h"
#import "CBLArray.h"
#import "CBLDictionary.h"
#import "CBLData.h"

@implementation CBLFragment {
    id _value;
}

- /* internal */ (instancetype) initWithValue: (id)value {
    self = [super init];
    if (self) {
        _value = value;
    }
    return self;
}


#pragma mark - GET


- (NSInteger) integerValue {
    return [$castIf(NSNumber, _value) integerValue];
}


- (float) floatValue {
    return [$castIf(NSNumber, _value) floatValue];
}


- (double) doubleValue {
    return [$castIf(NSNumber, _value) doubleValue];
}


- (BOOL) booleanValue {
    return [CBLData booleanValueForObject: _value];
}


- (NSObject*) object {
    return _value;
}


- (NSString*) string {
    return $castIf(NSString, _value);
}


- (NSNumber*) number {
    return $castIf(NSNumber, _value);
}


- (NSDate*) date {
    return [CBLJSON dateWithJSONObject: self.string];
}


- (CBLBlob*) blob {
    return $castIf(CBLBlob, _value);
}


- (CBLArray*) array {
    return $castIf(CBLArray, _value);
}


- (CBLDictionary*) dictionary {
    return $castIf(CBLDictionary, _value);
}


- (NSObject*) value {
    return _value;
}


#pragma mark - EXISTENCE


- (BOOL) exists {
    return _value != nil;
}


#pragma mark SUBSCRIPTING


- (CBLFragment*) objectForKeyedSubscript: (NSString*)key {
    if ([_value conformsToProtocol: @protocol(CBLDictionary)])
        return [_value objectForKeyedSubscript: key];
    return [[CBLFragment alloc] initWithValue: nil];
}


- (CBLFragment*) objectAtIndexedSubscript: (NSUInteger)index {
    if ([_value conformsToProtocol: @protocol(CBLArray)])
        return [_value objectAtIndexedSubscript: index];
    return [[CBLFragment alloc] initWithValue: nil];
}


@end
