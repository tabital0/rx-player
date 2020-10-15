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
import { of as observableOf, } from "rxjs";
import request, { fetchIsSupported, } from "../../utils/request";
import warnOnce from "../../utils/warn_once";
import byteRange from "../utils/byte_range";
import isMP4EmbeddedTextTrack from "../utils/is_mp4_embedded_text_track";
import addSegmentIntegrityChecks from "./add_segment_integrity_checks_to_loader";
import initSegmentLoader from "./init_segment_loader";
import lowLatencySegmentLoader from "./low_latency_segment_loader";
/**
 * Perform requests for "text" segments
 * @param {boolean} lowLatencyMode
 * @returns {Function}
 */
export default function generateTextTrackLoader(_a) {
    var lowLatencyMode = _a.lowLatencyMode, checkMediaSegmentIntegrity = _a.checkMediaSegmentIntegrity;
    return checkMediaSegmentIntegrity !== true ? textTrackLoader :
        addSegmentIntegrityChecks(textTrackLoader);
    /**
     * @param {Object} args
     * @returns {Observable}
     */
    function textTrackLoader(args) {
        var range = args.segment.range;
        var url = args.url;
        if (url === null) {
            return observableOf({ type: "data-created",
                value: { responseData: null } });
        }
        if (args.segment.isInit) {
            return initSegmentLoader(url, args);
        }
        var isMP4Embedded = isMP4EmbeddedTextTrack(args.representation);
        if (lowLatencyMode && isMP4Embedded) {
            if (fetchIsSupported()) {
                return lowLatencySegmentLoader(url, args);
            }
            else {
                warnOnce("DASH: Your browser does not have the fetch API. You will have " +
                    "a higher chance of rebuffering when playing close to the live edge");
            }
        }
        // ArrayBuffer when in mp4 to parse isobmff manually, text otherwise
        var responseType = isMP4Embedded ? "arraybuffer" :
            "text";
        return request({ url: url,
            responseType: responseType, headers: Array.isArray(range) ?
                { Range: byteRange(range) } :
                null,
            sendProgressEvents: true });
    }
}