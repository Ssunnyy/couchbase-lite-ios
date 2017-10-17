//
//  CBLMutableArrayFragment.h
//  CouchbaseLite
//
//  Created by Pasin Suriyentrakorn on 4/21/17.
//  Copyright © 2017 Couchbase. All rights reserved.
//

#import "CBLArrayFragment.h"
@class CBLMutableFragment;

/** CBLMutableArrayFragment protocol provides subscript access to CBLMutableFragment objects by index. */
@protocol CBLMutableArrayFragment <CBLArrayFragment>

/** 
 Subscript access to a CBLMutableFragment object by index.
 
 @param index The index.
 @return The CBLMutableFragment object.
 */
- (CBLMutableFragment*) objectAtIndexedSubscript: (NSUInteger)index;

@end
