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
import config from "../../../config";
import applyPrioritizerToSegmentFetcher from "./prioritized_segment_fetcher";
import ObservablePrioritizer from "./prioritizer";
import createSegmentFetcher, { getSegmentFetcherOptions, } from "./segment_fetcher";
var MIN_CANCELABLE_PRIORITY = config.MIN_CANCELABLE_PRIORITY, MAX_HIGH_PRIORITY_LEVEL = config.MAX_HIGH_PRIORITY_LEVEL;
/**
 * Interact with the transport pipelines to download segments with the right
 * priority.
 *
 * @class SegmentFetcherCreator
 *
 * @example
 * ```js
 * const creator = new SegmentFetcherCreator(transport);
 *
 * // 2 - create a new fetcher with its backoff options
 * const fetcher = creator.createSegmentFetcher("audio", {
 *   maxRetryRegular: Infinity,
 *   maxRetryOffline: Infinity,
 * });
 *
 * // 3 - load a segment with a given priority
 * fetcher.createRequest(myContent, 1)
 *   // 4 - parse it
 *   .pipe(
 *     filter(evt => evt.type === "chunk"),
 *     mergeMap(response => response.parse());
 *   )
 *   // 5 - use it
 *   .subscribe((res) => console.log("audio chunk downloaded:", res));
 * ```
 */
var SegmentFetcherCreator = /** @class */ (function () {
    /**
     * @param {Object} transport
     */
    function SegmentFetcherCreator(transport, options) {
        this._transport = transport;
        this._prioritizer = new ObservablePrioritizer({
            prioritySteps: { high: MAX_HIGH_PRIORITY_LEVEL,
                low: MIN_CANCELABLE_PRIORITY },
        });
        this._backoffOptions = options;
    }
    /**
     * Create a segment fetcher, allowing to easily perform segment requests.
     * @param {string} bufferType - The type of buffer concerned (e.g. "audio",
     * "video", etc.)
     * @param {Subject} requests$ - Subject through which request-related events
     * (such as those needed by the ABRManager) will be sent.
     * @returns {Object}
     */
    SegmentFetcherCreator.prototype.createSegmentFetcher = function (bufferType, requests$) {
        var backoffOptions = getSegmentFetcherOptions(bufferType, this._backoffOptions);
        var pipelines = this._transport[bufferType];
        // Types are very complicated here as they are per-type of buffer.
        // This is the reason why `any` is used instead.
        var segmentFetcher = createSegmentFetcher(bufferType, pipelines, requests$, backoffOptions);
        return applyPrioritizerToSegmentFetcher(this._prioritizer, segmentFetcher);
    };
    return SegmentFetcherCreator;
}());
export default SegmentFetcherCreator;
