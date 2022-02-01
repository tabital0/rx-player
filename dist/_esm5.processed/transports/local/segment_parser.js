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
import { getMDHDTimescale, takePSSHOut, } from "../../parsers/containers/isobmff";
import { getTimeCodeScale } from "../../parsers/containers/matroska";
import takeFirstSet from "../../utils/take_first_set";
import getISOBMFFTimingInfos from "../utils/get_isobmff_timing_infos";
import inferSegmentContainer from "../utils/infer_segment_container";
export default function segmentParser(loadedSegment, content, initTimescale) {
    var period = content.period, adaptation = content.adaptation, representation = content.representation, segment = content.segment;
    var data = loadedSegment.data;
    var appendWindow = [period.start, period.end];
    if (data === null) {
        if (segment.isInit) {
            return { segmentType: "init",
                initializationData: null,
                protectionDataUpdate: false,
                initTimescale: undefined };
        }
        return { segmentType: "media",
            chunkData: null,
            chunkInfos: null,
            chunkOffset: 0,
            protectionDataUpdate: false, appendWindow: appendWindow };
    }
    var chunkData = new Uint8Array(data);
    var containerType = inferSegmentContainer(adaptation.type, representation);
    // TODO take a look to check if this is an ISOBMFF/webm?
    var seemsToBeMP4 = containerType === "mp4" || containerType === undefined;
    var protectionDataUpdate = false;
    if (seemsToBeMP4) {
        var psshInfo = takePSSHOut(chunkData);
        if (psshInfo.length > 0) {
            protectionDataUpdate = representation._addProtectionData("cenc", psshInfo);
        }
    }
    if (segment.isInit) {
        var timescale = containerType === "webm" ? getTimeCodeScale(chunkData, 0) :
            // assume ISOBMFF-compliance
            getMDHDTimescale(chunkData);
        return { segmentType: "init",
            initializationData: chunkData,
            initTimescale: timescale !== null && timescale !== void 0 ? timescale : undefined, protectionDataUpdate: protectionDataUpdate };
    }
    var chunkInfos = seemsToBeMP4 ? getISOBMFFTimingInfos(chunkData, false, segment, initTimescale) :
        null; // TODO extract time info from webm
    var chunkOffset = takeFirstSet(segment.timestampOffset, 0);
    return { segmentType: "media", chunkData: chunkData, chunkInfos: chunkInfos, chunkOffset: chunkOffset, protectionDataUpdate: false, appendWindow: appendWindow };
}