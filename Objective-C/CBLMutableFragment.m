//
//  CBLMutableFragment.m
//  CouchbaseLite
//
//  Created by Pasin Suriyentrakorn on 4/21/17.
//  Copyright © 2017 Couchbase. All rights reserved.
//

#import "CBLMutableFragment.h"
#import "CBLData.h"
#import "CBLDocument+Internal.h"
#import "CBLJSON.h"

@implementation CBLMutableFragment {
    id _value;
    id _parent;
    id _parentKey;
    NSUInteger _index;
}


- /* internal */ (instancetype) initWithValue: (id)value parent: (id)parent parentKey: (id)parentKey {
    self = [super initWithValue: value];
    if (self) {
        _value = value;
        _parent = parent;
        _parentKey = parentKey;
    }
    return self;
}


#pragma mark - SUBSCRIPTING


- (CBLMutableFragment*) objectForKeyedSubscript: (NSString*)key {
    if ([_value conformsToProtocol: @protocol(CBLMutableDictionary)])
        return [_value objectForKeyedSubscript: key];
    return [[CBLMutableFragment alloc] initWithValue: nil parent: nil parentKey: nil];
}


- (CBLMutableFragment*) objectAtIndexedSubscript: (NSUInteger)index {
    if ([_value conformsToProtocol: @protocol(CBLMutableArray)])
        return [_value objectAtIndexedSubscript: index];
    return [[CBLMutableFragment alloc] initWithValue: nil parent: nil parentKey: nil];
}


#pragma mark - SET


- (void) setValue: (NSObject*)value {
    if ([_parent conformsToProtocol: @protocol(CBLMutableDictionary)]) {
        NSString* key = (NSString*)_parentKey;
        [_parent setObject: value forKey: key];
        _value = [_parent objectForKey: key];
    } else if ([_parent conformsToProtocol: @protocol(CBLMutableArray)]) {
        NSInteger index = [_parentKey integerValue];
        if (index >= 0 && (NSUInteger)index < ((CBLMutableArray*)_parent).count) {
            [_parent setObject: value atIndex: index];
            _value = [_parent objectAtIndex: index];
        }
    }
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


- (CBLMutableArray*) array {
    return $castIf(CBLMutableArray, _value);
}


- (CBLMutableDictionary*) dictionary {
    return $castIf(CBLMutableDictionary, _value);
}


- (NSObject*) value {
    return _value;
}


#pragma mark - EXISTENCE


- (BOOL) exists {
    return _value != nil;
}


@end
