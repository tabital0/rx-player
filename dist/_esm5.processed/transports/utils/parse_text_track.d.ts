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
import { Adaptation, ISegment, Representation } from "../../manifest";
import { IChunkTimeInfo, ITextTrackSegmentData } from "../types";
/**
 * Return plain text text track from the given ISOBMFF.
 * @param {Uint8Array} chunkBytes
 * @returns {string}
 */
export declare function extractTextTrackFromISOBMFF(chunkBytes: Uint8Array): string;
/**
 * Returns the a string expliciting the format of a text track when that text
 * track is embedded into a ISOBMFF file.
 * @param {Object} representation
 * @returns {string}
 */
export declare function getISOBMFFTextTrackFormat(representation: Representation): "ttml" | "vtt";
/**
 * Returns the a string expliciting the format of a text track in plain text.
 * @param {Object} representation
 * @returns {string}
 */
export declare function getPlainTextTrackFormat(representation: Representation): "ttml" | "sami" | "vtt" | "srt";
/**
 * @param {Object} content
 * @param {ArrayBuffer|UInt8Array|null} chunkData
 * @param {Object|null} chunkInfos
 * @param {boolean} isChunked
 * @returns {Object|null}
 */
export declare function getISOBMFFEmbeddedTextTrackData({ segment, adaptation, representation }: {
    segment: ISegment;
    adaptation: Adaptation;
    representation: Representation;
}, chunkBytes: Uint8Array, chunkInfos: IChunkTimeInfo | null, isChunked: boolean): ITextTrackSegmentData | null;
/**
 * @param {Object} content
 * @param {ArrayBuffer|UInt8Array|null} chunkData
 * @param {Object|null} chunkInfos
 * @param {boolean} isChunked
 * @returns {Object|null}
 */
export declare function getPlainTextTrackData({ segment, adaptation, representation }: {
    segment: ISegment;
    adaptation: Adaptation;
    representation: Representation;
}, textTrackData: string, isChunked: boolean): ITextTrackSegmentData | null;
