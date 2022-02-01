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
import { IRepresentationIndex, ISegment } from "../../../../../manifest";
import { IEMSG } from "../../../../containers/isobmff";
import { IIndexSegment } from "../../../utils/index_helpers";
import { IResolvedBaseUrl } from "../resolve_base_urls";
/**
 * Index property defined for a SegmentBase RepresentationIndex
 * This object contains every property needed to generate an ISegment for a
 * given media time.
 */
export interface IBaseIndex {
    /** Byte range for a possible index of segments in the server. */
    indexRange?: [number, number];
    /**
     * Temporal offset, in the current timescale (see timescale), to add to the
     * presentation time (time a segment has at decoding time) to obtain the
     * corresponding media time (original time of the media segment in the index
     * and on the media file).
     * For example, to look for a segment beginning at a second `T` on a
     * HTMLMediaElement, we actually will look for a segment in the index
     * beginning at:
     * ```
     * T * timescale + indexTimeOffset
     * ```
     */
    indexTimeOffset: number;
    /** Information on the initialization segment. */
    initialization?: {
        /** URLs to access the initialization segment. */
        mediaURLs: string[] | null;
        /** possible byte range to request it. */
        range?: [number, number];
    };
    /**
     * Base URL(s) to access any segment. Can contain tokens to replace to convert
     * it to real URLs.
     */
    mediaURLs: string[] | null;
    /** Number from which the first segments in this index starts with. */
    startNumber?: number;
    /** Every segments defined in this index. */
    timeline: IIndexSegment[];
    /**
     * Timescale to convert a time given here into seconds.
     * This is done by this simple operation:
     * ``timeInSeconds = timeInIndex * timescale``
     */
    timescale: number;
}
/**
 * `index` Argument for a SegmentBase RepresentationIndex.
 * Most of the properties here are already defined in IBaseIndex.
 */
export interface IBaseIndexIndexArgument {
    timeline?: IIndexSegment[];
    timescale?: number;
    media?: string;
    indexRange?: [number, number];
    initialization?: {
        media?: string;
        range?: [number, number];
    };
    startNumber?: number;
    /**
     * Offset present in the index to convert from the mediaTime (time declared in
     * the media segments and in this index) to the presentationTime (time wanted
     * when decoding the segment).  Basically by doing something along the line
     * of:
     * ```
     * presentationTimeInSeconds =
     *   mediaTimeInSeconds -
     *   presentationTimeOffsetInSeconds +
     *   periodStartInSeconds
     * ```
     * The time given here is in the current
     * timescale (see timescale)
     */
    presentationTimeOffset?: number;
}
/** Aditional context needed by a SegmentBase RepresentationIndex. */
export interface IBaseIndexContextArgument {
    /** Start of the period concerned by this RepresentationIndex, in seconds. */
    periodStart: number;
    /** End of the period concerned by this RepresentationIndex, in seconds. */
    periodEnd: number | undefined;
    /** Base URL for the Representation concerned. */
    representationBaseURLs: IResolvedBaseUrl[];
    /** ID of the Representation concerned. */
    representationId?: string;
    /** Bitrate of the Representation concerned. */
    representationBitrate?: number;
    isEMSGWhitelisted: (inbandEvent: IEMSG) => boolean;
}
export default class BaseRepresentationIndex implements IRepresentationIndex {
    /**
     * `true` if the list of segments is already known.
     * `false` if the initialization segment should be loaded (and the segments
     * added) first.
     * @see isInitialized method
     */
    private _isInitialized;
    /** Underlying structure to retrieve segment information. */
    private _index;
    /** Absolute end of the period, timescaled and converted to index time. */
    private _scaledPeriodEnd;
    private _isEMSGWhitelisted;
    /**
     * @param {Object} index
     * @param {Object} context
     */
    constructor(index: IBaseIndexIndexArgument, context: IBaseIndexContextArgument);
    /**
     * Construct init Segment.
     * @returns {Object}
     */
    getInitSegment(): ISegment;
    /**
     * Get the list of segments that are currently available from the `from`
     * position, in seconds, ending `dur` seconds after that position.
     *
     * Note that if not already done, you might need to "initialize" the
     * `BaseRepresentationIndex` first so that the list of available segments
     * is known.
     *
     * @see isInitialized for more information on `BaseRepresentationIndex`
     * initialization.
     * @param {Number} from
     * @param {Number} dur
     * @returns {Array.<Object>}
     */
    getSegments(from: number, dur: number): ISegment[];
    /**
     * Returns false as no Segment-Base based index should need to be refreshed.
     * @returns {Boolean}
     */
    shouldRefresh(): false;
    /**
     * Returns first position in index.
     * @returns {Number|null}
     */
    getFirstPosition(): number | null;
    /**
     * Returns last position in index.
     * @returns {Number|null}
     */
    getLastPosition(): number | null;
    /**
     * Segments in a segmentBase scheme should stay available.
     * @returns {Boolean|undefined}
     */
    isSegmentStillAvailable(): true;
    /**
     * We do not check for discontinuity in SegmentBase-based indexes.
     * @returns {null}
     */
    checkDiscontinuity(): null;
    /**
     * `BaseRepresentationIndex` should just already all be generated.
     * Return `true` as a default value here.
     * @returns {boolean}
     */
    areSegmentsChronologicallyGenerated(): true;
    /**
     * No segment in a `BaseRepresentationIndex` are known initially.
     * It is only defined generally in an "index segment" that will thus need to
     * be first loaded and parsed.
     * Until then, this `BaseRepresentationIndex` is considered as `uninitialized`
     * (@see isInitialized).
     *
     * Once that those information are available, the present
     * `BaseRepresentationIndex` can be "initialized" by adding that parsed
     * segment information through this method.
     * @param {Array.<Object>} indexSegments
     * @returns {Array.<Object>}
     */
    initializeIndex(indexSegments: IAddedIndexSegment[]): void;
    /**
     * Returns `false` as a `BaseRepresentationIndex` should not be dynamic and as
     * such segments should never fall out-of-sync.
     * @returns {Boolean}
     */
    canBeOutOfSyncError(): false;
    /**
     * Returns `true` as SegmentBase are not dynamic and as such no new segment
     * should become available in the future.
     * @returns {Boolean}
     */
    isFinished(): true;
    /**
     * No segment in a `BaseRepresentationIndex` are known initially.
     * It is only defined generally in an "index segment" that will thus need to
     * be first loaded and parsed.
     *
     * Once the index segment or equivalent has been parsed, the `initializeIndex`
     * method have to be called with the corresponding segment information so the
     * `BaseRepresentationIndex` can be considered as "initialized" (and so this
     * method can return `true`).
     * Until then this method will return `false` and segments linked to that
     * Representation may be missing.
     * @returns {Boolean}
     */
    isInitialized(): boolean;
    /**
     * Replace in-place this `BaseRepresentationIndex` information by the
     * information from another one.
     * @param {Object} newIndex
     */
    _replace(newIndex: BaseRepresentationIndex): void;
    _update(): void;
}
/**
 * Format of a segment received through the `initialize` method, allowing  to
 * add segments to a BaseRepresentationIndex.
 */
export interface IAddedIndexSegment {
    /** This segment start time, timescaled. */
    time: number;
    /** This segment difference between its end and start time, timescaled. */
    duration: number;
    /** Dividing `time` or `duration` with this value allows to obtain seconds. */
    timescale: number;
    /**
     * Start and ending bytes (included) for the segment in the whole ISOBMFF
     * buffer.
     */
    range: [number, number];
}