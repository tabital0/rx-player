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
import log from "../../../../log";
import arrayFind from "../../../../utils/array_find";
import arrayIncludes from "../../../../utils/array_includes";
import isNonEmptyString from "../../../../utils/is_non_empty_string";
import attachTrickModeTrack from "./attach_trickmode_track";
// eslint-disable-next-line max-len
import extractMinimumAvailabilityTimeOffset from "./extract_minimum_availability_time_offset";
import inferAdaptationType from "./infer_adaptation_type";
import parseRepresentations from "./parse_representations";
import resolveBaseURLs from "./resolve_base_urls";
/**
 * Detect if the accessibility given defines an adaptation for the visually
 * impaired.
 * Based on DVB Document A168 (DVB-DASH) and DASH-IF 4.3.
 * @param {Object} accessibility
 * @returns {Boolean}
 */
function isVisuallyImpaired(accessibility) {
    if (accessibility == null) {
        return false;
    }
    var isVisuallyImpairedAudioDvbDash = (accessibility.schemeIdUri === "urn:tva:metadata:cs:AudioPurposeCS:2007" &&
        accessibility.value === "1");
    var isVisuallyImpairedDashIf = (accessibility.schemeIdUri === "urn:mpeg:dash:role:2011" &&
        accessibility.value === "description");
    return isVisuallyImpairedAudioDvbDash || isVisuallyImpairedDashIf;
}
/**
 * Detect if the accessibility given defines an adaptation for the hard of
 * hearing.
 * Based on DVB Document A168 (DVB-DASH).
 * @param {Object} accessibility
 * @returns {Boolean}
 */
function isHardOfHearing(accessibility) {
    if (accessibility == null) {
        return false;
    }
    return (accessibility.schemeIdUri === "urn:tva:metadata:cs:AudioPurposeCS:2007" &&
        accessibility.value === "2");
}
/**
 * Detect if the accessibility given defines an AdaptationSet containing a sign
 * language interpretation.
 * Based on DASH-IF 4.3.
 * @param {Object} accessibility
 * @returns {Boolean}
 */
function hasSignLanguageInterpretation(accessibility) {
    if (accessibility == null) {
        return false;
    }
    return (accessibility.schemeIdUri === "urn:mpeg:dash:role:2011" &&
        accessibility.value === "sign");
}
/**
 * Contruct Adaptation ID from the information we have.
 * @param {Object} adaptation
 * @param {Array.<Object>} representations
 * @param {Array.<Object>} representations
 * @param {Object} infos
 * @returns {string}
 */
function getAdaptationID(adaptation, infos) {
    if (isNonEmptyString(adaptation.attributes.id)) {
        return adaptation.attributes.id;
    }
    var isClosedCaption = infos.isClosedCaption, isAudioDescription = infos.isAudioDescription, isSignInterpreted = infos.isSignInterpreted, isTrickModeTrack = infos.isTrickModeTrack, type = infos.type;
    var idString = type;
    if (isNonEmptyString(adaptation.attributes.language)) {
        idString += "-" + adaptation.attributes.language;
    }
    if (isClosedCaption === true) {
        idString += "-cc";
    }
    if (isAudioDescription === true) {
        idString += "-ad";
    }
    if (isSignInterpreted === true) {
        idString += "-si";
    }
    if (isTrickModeTrack) {
        idString += "-trickMode";
    }
    if (isNonEmptyString(adaptation.attributes.contentType)) {
        idString += "-" + adaptation.attributes.contentType;
    }
    if (isNonEmptyString(adaptation.attributes.codecs)) {
        idString += "-" + adaptation.attributes.codecs;
    }
    if (isNonEmptyString(adaptation.attributes.mimeType)) {
        idString += "-" + adaptation.attributes.mimeType;
    }
    if (isNonEmptyString(adaptation.attributes.frameRate)) {
        idString += "-" + adaptation.attributes.frameRate;
    }
    return idString;
}
/**
 * Returns a list of ID this adaptation can be seamlessly switched to
 * @param {Object} adaptation
 * @returns {Array.<string>}
 */
function getAdaptationSetSwitchingIDs(adaptation) {
    if (adaptation.children.supplementalProperties != null) {
        var supplementalProperties = adaptation.children.supplementalProperties;
        for (var _i = 0, supplementalProperties_1 = supplementalProperties; _i < supplementalProperties_1.length; _i++) {
            var supplementalProperty = supplementalProperties_1[_i];
            if (supplementalProperty.schemeIdUri ===
                "urn:mpeg:dash:adaptation-set-switching:2016" &&
                supplementalProperty.value != null) {
                return supplementalProperty.value.split(",")
                    .map(function (id) { return id.trim(); })
                    .filter(function (id) { return id; });
            }
        }
    }
    return [];
}
/**
 * Process AdaptationSets intermediate representations to return under its final
 * form.
 * Note that the AdaptationSets returned are sorted by priority (from the most
 * priority to the least one).
 * @param {Array.<Object>} adaptationsIR
 * @param {Object} periodInfos
 * @returns {Array.<Object>}
 */
export default function parseAdaptationSets(adaptationsIR, periodInfos) {
    var _a;
    var _b, _c, _d, _e, _f;
    var parsedAdaptations = {};
    var trickModeAdaptations = [];
    var adaptationSwitchingInfos = {};
    var parsedAdaptationsIDs = [];
    /**
     * Index of the last parsed AdaptationSet with a Role set as "main" in
     * `parsedAdaptations` for a given type.
     * Not defined for a type with no main Adaptation inside.
     * This is used to put main AdaptationSet first in the resulting array of
     * Adaptation while still preserving the MPD order among them.
     */
    var lastMainAdaptationIndex = {};
    // first sort AdaptationSets by absolute priority.
    adaptationsIR.sort(function (a, b) {
        var _a, _b;
        /* As of DASH-IF 4.3, `1` is the default value. */
        var priority1 = (_a = a.attributes.selectionPriority) !== null && _a !== void 0 ? _a : 1;
        var priority2 = (_b = b.attributes.selectionPriority) !== null && _b !== void 0 ? _b : 1;
        return priority2 - priority1;
    });
    for (var _i = 0, adaptationsIR_1 = adaptationsIR; _i < adaptationsIR_1.length; _i++) {
        var adaptation = adaptationsIR_1[_i];
        var adaptationChildren = adaptation.children;
        var essentialProperties = adaptationChildren.essentialProperties, roles = adaptationChildren.roles;
        var isMainAdaptation = Array.isArray(roles) &&
            roles.some(function (role) { return role.value === "main"; }) &&
            roles.some(function (role) { return role.schemeIdUri === "urn:mpeg:dash:role:2011"; });
        var representationsIR = adaptation.children.representations;
        var availabilityTimeOffset = extractMinimumAvailabilityTimeOffset(adaptation.children.baseURLs) +
            periodInfos.availabilityTimeOffset;
        var adaptationMimeType = adaptation.attributes.mimeType;
        var adaptationCodecs = adaptation.attributes.codecs;
        var type = inferAdaptationType(representationsIR, isNonEmptyString(adaptationMimeType) ?
            adaptationMimeType :
            null, isNonEmptyString(adaptationCodecs) ?
            adaptationCodecs :
            null, adaptationChildren.roles != null ?
            adaptationChildren.roles :
            null);
        if (type === undefined) {
            continue;
        }
        var originalID = adaptation.attributes.id;
        var newID = void 0;
        var adaptationSetSwitchingIDs = getAdaptationSetSwitchingIDs(adaptation);
        var parentSegmentTemplates = [];
        if (periodInfos.segmentTemplate !== undefined) {
            parentSegmentTemplates.push(periodInfos.segmentTemplate);
        }
        if (adaptation.children.segmentTemplate !== undefined) {
            parentSegmentTemplates.push(adaptation.children.segmentTemplate);
        }
        var adaptationInfos = {
            aggressiveMode: periodInfos.aggressiveMode,
            availabilityTimeOffset: availabilityTimeOffset,
            baseURLs: resolveBaseURLs(periodInfos.baseURLs, adaptationChildren.baseURLs),
            manifestBoundsCalculator: periodInfos.manifestBoundsCalculator,
            end: periodInfos.end,
            isDynamic: periodInfos.isDynamic,
            manifestProfiles: periodInfos.manifestProfiles,
            parentSegmentTemplates: parentSegmentTemplates,
            receivedTime: periodInfos.receivedTime,
            start: periodInfos.start,
            timeShiftBufferDepth: periodInfos.timeShiftBufferDepth,
            unsafelyBaseOnPreviousAdaptation: null,
        };
        var trickModeProperty = Array.isArray(essentialProperties) ?
            arrayFind(essentialProperties, function (scheme) {
                return scheme.schemeIdUri === "http://dashif.org/guidelines/trickmode";
            }) : undefined;
        var trickModeAttachedAdaptationIds = (_b = trickModeProperty === null || trickModeProperty === void 0 ? void 0 : trickModeProperty.value) === null || _b === void 0 ? void 0 : _b.split(" ");
        var isTrickModeTrack = trickModeAttachedAdaptationIds !== undefined;
        if (type === "video" &&
            isMainAdaptation &&
            parsedAdaptations.video !== undefined &&
            parsedAdaptations.video.length > 0 &&
            lastMainAdaptationIndex.video !== undefined &&
            !isTrickModeTrack) {
            // Add to the already existing main video adaptation
            // TODO remove that ugly custom logic?
            var videoMainAdaptation = parsedAdaptations.video[lastMainAdaptationIndex.video];
            adaptationInfos.unsafelyBaseOnPreviousAdaptation = (_d = (_c = periodInfos
                .unsafelyBaseOnPreviousPeriod) === null || _c === void 0 ? void 0 : _c.getAdaptation(videoMainAdaptation.id)) !== null && _d !== void 0 ? _d : null;
            var representations = parseRepresentations(representationsIR, adaptation, adaptationInfos);
            (_a = videoMainAdaptation.representations).push.apply(_a, representations);
            newID = videoMainAdaptation.id;
        }
        else {
            var accessibilities = adaptationChildren.accessibilities;
            var isDub = void 0;
            if (roles !== undefined &&
                roles.some(function (role) { return role.value === "dub"; })) {
                isDub = true;
            }
            var isClosedCaption = void 0;
            if (type !== "text") {
                isClosedCaption = false;
            }
            else if (accessibilities !== undefined) {
                isClosedCaption = accessibilities.some(isHardOfHearing);
            }
            var isAudioDescription = void 0;
            if (type !== "audio") {
                isAudioDescription = false;
            }
            else if (accessibilities !== undefined) {
                isAudioDescription = accessibilities.some(isVisuallyImpaired);
            }
            var isSignInterpreted = void 0;
            if (type !== "video") {
                isSignInterpreted = false;
            }
            else if (accessibilities !== undefined) {
                isSignInterpreted = accessibilities.some(hasSignLanguageInterpretation);
            }
            var adaptationID = getAdaptationID(adaptation, { isAudioDescription: isAudioDescription, isClosedCaption: isClosedCaption, isSignInterpreted: isSignInterpreted, isTrickModeTrack: isTrickModeTrack, type: type });
            // Avoid duplicate IDs
            while (arrayIncludes(parsedAdaptationsIDs, adaptationID)) {
                adaptationID += "-dup";
            }
            newID = adaptationID;
            parsedAdaptationsIDs.push(adaptationID);
            adaptationInfos.unsafelyBaseOnPreviousAdaptation = (_f = (_e = periodInfos
                .unsafelyBaseOnPreviousPeriod) === null || _e === void 0 ? void 0 : _e.getAdaptation(adaptationID)) !== null && _f !== void 0 ? _f : null;
            var representations = parseRepresentations(representationsIR, adaptation, adaptationInfos);
            var parsedAdaptationSet = { id: adaptationID, representations: representations, type: type, isTrickModeTrack: isTrickModeTrack };
            if (adaptation.attributes.language != null) {
                parsedAdaptationSet.language = adaptation.attributes.language;
            }
            if (isClosedCaption != null) {
                parsedAdaptationSet.closedCaption = isClosedCaption;
            }
            if (isAudioDescription != null) {
                parsedAdaptationSet.audioDescription = isAudioDescription;
            }
            if (isDub === true) {
                parsedAdaptationSet.isDub = true;
            }
            if (isSignInterpreted === true) {
                parsedAdaptationSet.isSignInterpreted = true;
            }
            var adaptationsOfTheSameType = parsedAdaptations[type];
            if (trickModeAttachedAdaptationIds !== undefined) {
                trickModeAdaptations.push({ adaptation: parsedAdaptationSet, trickModeAttachedAdaptationIds: trickModeAttachedAdaptationIds });
            }
            else if (adaptationsOfTheSameType === undefined) {
                parsedAdaptations[type] = [parsedAdaptationSet];
                if (isMainAdaptation) {
                    lastMainAdaptationIndex[type] = 0;
                }
            }
            else {
                var mergedInto = null;
                var _loop_1 = function (id) {
                    var _h;
                    var switchingInfos = adaptationSwitchingInfos[id];
                    if (switchingInfos != null &&
                        switchingInfos.newID !== newID &&
                        arrayIncludes(switchingInfos.adaptationSetSwitchingIDs, originalID)) {
                        var adaptationToMergeInto = arrayFind(adaptationsOfTheSameType, function (a) { return a.id === id; });
                        if (adaptationToMergeInto != null &&
                            adaptationToMergeInto.audioDescription ===
                                parsedAdaptationSet.audioDescription &&
                            adaptationToMergeInto.closedCaption ===
                                parsedAdaptationSet.closedCaption &&
                            adaptationToMergeInto.language === parsedAdaptationSet.language) {
                            log.info("DASH Parser: merging \"switchable\" AdaptationSets", originalID, id);
                            (_h = adaptationToMergeInto.representations)
                                .push.apply(_h, parsedAdaptationSet.representations);
                            mergedInto = adaptationToMergeInto;
                        }
                    }
                };
                // look if we have to merge this into another Adaptation
                for (var _g = 0, adaptationSetSwitchingIDs_1 = adaptationSetSwitchingIDs; _g < adaptationSetSwitchingIDs_1.length; _g++) {
                    var id = adaptationSetSwitchingIDs_1[_g];
                    _loop_1(id);
                }
                if (isMainAdaptation) {
                    var oldLastMainIdx = lastMainAdaptationIndex[type];
                    var newLastMainIdx = oldLastMainIdx === undefined ? 0 :
                        oldLastMainIdx + 1;
                    if (mergedInto === null) {
                        // put "main" Adaptation after all other Main Adaptations
                        adaptationsOfTheSameType.splice(newLastMainIdx, 0, parsedAdaptationSet);
                        lastMainAdaptationIndex[type] = newLastMainIdx;
                    }
                    else {
                        var indexOf = adaptationsOfTheSameType.indexOf(mergedInto);
                        if (indexOf < 0) { // Weird, not found
                            adaptationsOfTheSameType.splice(newLastMainIdx, 0, parsedAdaptationSet);
                            lastMainAdaptationIndex[type] = newLastMainIdx;
                        }
                        else if (oldLastMainIdx === undefined || indexOf > oldLastMainIdx) {
                            // Found but was not main
                            adaptationsOfTheSameType.splice(indexOf, 1);
                            adaptationsOfTheSameType.splice(newLastMainIdx, 0, mergedInto);
                            lastMainAdaptationIndex[type] = newLastMainIdx;
                        }
                    }
                }
                else if (mergedInto === null) {
                    adaptationsOfTheSameType.push(parsedAdaptationSet);
                }
            }
        }
        if (originalID != null && adaptationSwitchingInfos[originalID] == null) {
            adaptationSwitchingInfos[originalID] = { newID: newID, adaptationSetSwitchingIDs: adaptationSetSwitchingIDs };
        }
    }
    attachTrickModeTrack(parsedAdaptations, trickModeAdaptations);
    return parsedAdaptations;
}