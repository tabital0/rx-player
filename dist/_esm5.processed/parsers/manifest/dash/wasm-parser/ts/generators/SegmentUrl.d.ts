/**
 * Copyright 2015 CANAL+ Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { ISegmentUrlIntermediateRepresentation } from "../../../node_parser_types";
import { IAttributeParser } from "../parsers_stack";
/**
 * Generate "attribute parser" for an encountered `SegmentURL` opening
 * tag.
 * @param {Object} segmentUrlAttrs
 * @param {WebAssembly.Memory} linearMemory
 * @returns {Function}
 */
export declare function generateSegmentUrlAttrParser(segmentUrlAttrs: ISegmentUrlIntermediateRepresentation, linearMemory: WebAssembly.Memory): IAttributeParser;